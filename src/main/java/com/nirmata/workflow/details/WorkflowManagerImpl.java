/**
 * Copyright 2014 Nirmata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nirmata.workflow.details;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.admin.RunInfo;
import com.nirmata.workflow.admin.TaskDetails;
import com.nirmata.workflow.admin.TaskInfo;
import com.nirmata.workflow.admin.WorkflowAdmin;
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.details.internalmodels.RunnableTask;
import com.nirmata.workflow.details.internalmodels.StartedTask;
import com.nirmata.workflow.events.WorkflowListenerManager;
import com.nirmata.workflow.executor.TaskExecution;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.QueueFactory;
import com.nirmata.workflow.serialization.Serializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WorkflowManagerImpl implements WorkflowManager, WorkflowAdmin
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework curator;
    private final String instanceName;
    private final List<QueueConsumer> consumers;
    private final SchedulerSelector schedulerSelector;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final Serializer serializer;
    private final Executor taskRunnerService;

    private static final TaskType nullTaskType = new TaskType("", "", false);

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public WorkflowManagerImpl(CuratorFramework curator, QueueFactory queueFactory, String instanceName, List<TaskExecutorSpec> specs, AutoCleanerHolder autoCleanerHolder, Serializer serializer, Executor taskRunnerService)
    {
        this.taskRunnerService = Preconditions.checkNotNull(taskRunnerService, "taskRunnerService cannot be null");
        this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
        autoCleanerHolder = Preconditions.checkNotNull(autoCleanerHolder, "autoCleanerHolder cannot be null");
        this.curator = Preconditions.checkNotNull(curator, "curator cannot be null");
        queueFactory = Preconditions.checkNotNull(queueFactory, "queueFactory cannot be null");
        this.instanceName = Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
        specs = Preconditions.checkNotNull(specs, "specs cannot be null");

        consumers = makeTaskConsumers(queueFactory, specs);
        schedulerSelector = new SchedulerSelector(this, queueFactory, autoCleanerHolder);
    }

    public CuratorFramework getCurator()
    {
        return curator;
    }

    @VisibleForTesting
    volatile boolean debugDontStartConsumers = false;

    @Override
    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        if ( !debugDontStartConsumers )
        {
            startQueueConsumers();
        }
        schedulerSelector.start();
    }

    @VisibleForTesting
    void startQueueConsumers()
    {
        consumers.forEach(QueueConsumer::start);
    }

    @Override
    public WorkflowListenerManager newWorkflowListenerManager()
    {
        return new WorkflowListenerManagerImpl(this);
    }

    @Override
    public Map<TaskId, TaskDetails> getTaskDetails(RunId runId)
    {
        try
        {
            String runPath = ZooKeeperConstants.getRunPath(runId);
            byte[] runnableTaskBytes = curator.getData().forPath(runPath);
            RunnableTask runnableTask = serializer.deserialize(runnableTaskBytes, RunnableTask.class);
            return runnableTask.getTasks()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    ExecutableTask executableTask = entry.getValue();
                    TaskType taskType = executableTask.getTaskType().equals(nullTaskType) ? null : executableTask.getTaskType();
                    return new TaskDetails(entry.getKey(), taskType, executableTask.getMetaData());
                }))
                ;
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            return ImmutableMap.of();
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RunId submitTask(Task task)
    {
        return submitSubTask(new RunId(), null, task);
    }

    @Override
    public RunId submitTask(RunId runId, Task task)
    {
        return submitSubTask(runId, null, task);
    }

    @Override
    public RunId submitSubTask(RunId parentRunId, Task task)
    {
        return submitSubTask(new RunId(), parentRunId, task);
    }

    public volatile long debugLastSubmittedTimeMs = 0;

    @Override
    public RunId submitSubTask(RunId runId, RunId parentRunId, Task task)
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");

        RunnableTaskDagBuilder builder = new RunnableTaskDagBuilder(task);
        Map<TaskId, ExecutableTask> tasks = builder
            .getTasks()
            .values()
            .stream()
            .collect(Collectors.toMap(Task::getTaskId, t -> new ExecutableTask(runId, t.getTaskId(), t.isExecutable() ? t.getTaskType() : nullTaskType, t.getMetaData(), t.isExecutable())));
        RunnableTask runnableTask = new RunnableTask(tasks, builder.getEntries(), LocalDateTime.now(), null, parentRunId);

        try
        {
            byte[] runnableTaskBytes = serializer.serialize(runnableTask);
            String runPath = ZooKeeperConstants.getRunPath(runId);
            debugLastSubmittedTimeMs = System.currentTimeMillis();
            curator.create().creatingParentContainersIfNeeded().forPath(runPath, runnableTaskBytes);
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }

        return runId;
    }
    
    public void updateTaskProgress(RunId runId, TaskId taskId, int progress)
    {   
        Preconditions.checkArgument((progress >= 0) && (progress <= 100), "progress must be between 0 and 100");
         
        String path = ZooKeeperConstants.getStartedTaskPath(runId, taskId);
        try
        {
            byte[] bytes = curator.getData().forPath(path);
            StartedTask startedTask = serializer.deserialize(bytes, StartedTask.class);
            StartedTask updatedStartedTask = new StartedTask(startedTask.getInstanceName(), startedTask.getStartDateUtc(), progress);
            byte[] data = getSerializer().serialize(updatedStartedTask);
            curator.setData().forPath(path, data);
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore - must have been deleted in the interim, for example before we update 
            // progress the task is completed
        }
        catch ( Exception e )
        {
            throw new RuntimeException("Trying to read started task info from: " + path, e);
        }
    }
    
    @Override
    public boolean cancelRun(RunId runId)
    {
        log.info("Attempting to cancel run " + runId);

        String runPath = ZooKeeperConstants.getRunPath(runId);
        try
        {
            Stat stat = new Stat();
            byte[] bytes = curator.getData().storingStatIn(stat).forPath(runPath);
            RunnableTask runnableTask = serializer.deserialize(bytes, RunnableTask.class);
            Scheduler.completeRunnableTask(log, this, runId, runnableTask, stat.getVersion());
            return true;
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            return false;
        }
        catch ( Exception e )
        {
            throw new RuntimeException("Could not cancel runId " + runId, e);
        }
    }

    @Override
    public Optional<TaskExecutionResult> getTaskExecutionResult(RunId runId, TaskId taskId)
    {
        String completedTaskPath = ZooKeeperConstants.getCompletedTaskPath(runId, taskId);
        try
        {
            byte[] bytes = curator.getData().forPath(completedTaskPath);
            TaskExecutionResult taskExecutionResult = serializer.deserialize(bytes, TaskExecutionResult.class);
            return Optional.of(taskExecutionResult);
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            // dummy
        }
        catch ( Exception e )
        {
            throw new RuntimeException(String.format("No data for runId %s taskId %s", runId, taskId), e);
        }
        return Optional.empty();
    }

    public String getInstanceName()
    {
        return instanceName;
    }

    @VisibleForTesting
    public void debugValidateClosed()
    {
        consumers.forEach(QueueConsumer::debugValidateClosed);
        schedulerSelector.debugValidateClosed();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            CloseableUtils.closeQuietly(schedulerSelector);
            consumers.forEach(CloseableUtils::closeQuietly);
        }
    }

    @Override
    public WorkflowAdmin getAdmin()
    {
        return this;
    }

    @Override
    public WorkflowManagerState getWorkflowManagerState()
    {
        return new WorkflowManagerState(
            curator.getZookeeperClient().isConnected(),
            schedulerSelector.getState(),
            consumers.stream().map(QueueConsumer::getState).collect(Collectors.toList())
        );
    }

    @Override
    public boolean clean(RunId runId)
    {
        String runPath = ZooKeeperConstants.getRunPath(runId);
        try
        {
            byte[] bytes = curator.getData().forPath(runPath);
            RunnableTask runnableTask = serializer.deserialize(bytes, RunnableTask.class);
            runnableTask.getTasks().keySet().forEach(taskId -> {
                String startedTaskPath = ZooKeeperConstants.getStartedTaskPath(runId, taskId);
                try
                {
                    curator.delete().forPath(startedTaskPath);
                }
                catch ( KeeperException.NoNodeException ignore )
                {
                    // ignore
                }
                catch ( Exception e )
                {
                    throw new RuntimeException("Could not delete started task at: " + startedTaskPath, e);
                }

                String completedTaskPath = ZooKeeperConstants.getCompletedTaskPath(runId, taskId);
                try
                {
                    curator.delete().forPath(completedTaskPath);
                }
                catch ( KeeperException.NoNodeException ignore )
                {
                    // ignore
                }
                catch ( Exception e )
                {
                    throw new RuntimeException("Could not delete completed task at: " + completedTaskPath, e);
                }
            });

            try
            {
                curator.delete().forPath(runPath);
            }
            catch ( Exception e )
            {
                // at this point, the node should exist
                throw new RuntimeException(e);
            }

            return true;
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            return false;
        }
        catch ( Throwable e )
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RunInfo getRunInfo(RunId runId)
    {
        try
        {
            String runPath = ZooKeeperConstants.getRunPath(runId);
            byte[] bytes = curator.getData().forPath(runPath);
            RunnableTask runnableTask = serializer.deserialize(bytes, RunnableTask.class);
            return new RunInfo(runId, runnableTask.getStartTimeUtc(), runnableTask.getCompletionTimeUtc().orElse(null));
        }
        catch ( Exception e )
        {
            log.error("Error getting RunInfo for runId: {}", runId, e);
            throw new RuntimeException("Could not read run: " + runId, e);
        }
    }

    @Override
    public List<RunId> getRunIds()
    {
        try
        {
            String runParentPath = ZooKeeperConstants.getRunParentPath();
            return curator.getChildren().forPath(runParentPath).stream()
                .map(RunId::new)
                .collect(Collectors.toList());
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore if parent node is missing
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
        return Collections.emptyList();
    }

    @Override
    public List<RunInfo> getRunInfo()
    {
        try
        {
            String runParentPath = ZooKeeperConstants.getRunParentPath();
            return curator.getChildren().forPath(runParentPath).stream()
                .map(child -> {
                    String fullPath = ZKPaths.makePath(runParentPath, child);
                    try
                    {
                        RunId runId = new RunId(ZooKeeperConstants.getRunIdFromRunPath(fullPath));
                        byte[] bytes = curator.getData().forPath(fullPath);
                        RunnableTask runnableTask = serializer.deserialize(bytes, RunnableTask.class);
                        return new RunInfo(runId, runnableTask.getStartTimeUtc(), runnableTask.getCompletionTimeUtc().orElse(null));
                    }
                    catch ( KeeperException.NoNodeException ignore )
                    {
                        // ignore - must have been deleted in the interim
                    }
                    catch ( Exception e )
                    {
                        throw new RuntimeException("Trying to read run info from: " + fullPath, e);
                    }
                    return null;
                })
                .filter(info -> (info != null))
                .collect(Collectors.toList());
        }
        catch ( Throwable e )
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<TaskInfo> getTaskInfo(RunId runId)
    {
        List<TaskInfo> taskInfos = Lists.newArrayList();
        String startedTasksParentPath = ZooKeeperConstants.getStartedTasksParentPath();
        String completedTaskParentPath = ZooKeeperConstants.getCompletedTaskParentPath();
        try
        {
            String runPath = ZooKeeperConstants.getRunPath(runId);
            byte[] runBytes = curator.getData().forPath(runPath);
            RunnableTask runnableTask = serializer.deserialize(runBytes, RunnableTask.class);

            Set<TaskId> notStartedTasks = runnableTask.getTasks().values().stream().filter(ExecutableTask::isExecutable).map(ExecutableTask::getTaskId).collect(Collectors.toSet());
            Map<TaskId, StartedTask> startedTasks = Maps.newHashMap();

            curator.getChildren().forPath(startedTasksParentPath).forEach(child -> {
                String fullPath = ZKPaths.makePath(startedTasksParentPath, child);
                TaskId taskId = new TaskId(ZooKeeperConstants.getTaskIdFromStartedTasksPath(fullPath));
                try
                {
                    byte[] bytes = curator.getData().forPath(fullPath);
                    StartedTask startedTask = serializer.deserialize(bytes, StartedTask.class);
                    startedTasks.put(taskId, startedTask);
                    notStartedTasks.remove(taskId);
                }
                catch ( KeeperException.NoNodeException ignore )
                {
                    // ignore - must have been deleted in the interim
                }
                catch ( Exception e )
                {
                    throw new RuntimeException("Trying to read started task info from: " + fullPath, e);
                }
            });

            curator.getChildren().forPath(completedTaskParentPath).forEach(child -> {
                String fullPath = ZKPaths.makePath(completedTaskParentPath, child);
                TaskId taskId = new TaskId(ZooKeeperConstants.getTaskIdFromCompletedTasksPath(fullPath));

                StartedTask startedTask = startedTasks.remove(taskId);
                if ( startedTask != null )  // otherwise it must have been deleted
                {
                    try
                    {
                        byte[] bytes = curator.getData().forPath(fullPath);
                        TaskExecutionResult taskExecutionResult = serializer.deserialize(bytes, TaskExecutionResult.class);
                        taskInfos.add(new TaskInfo(taskId, startedTask.getInstanceName(), startedTask.getStartDateUtc(), startedTask.getProgress(), taskExecutionResult));
                        notStartedTasks.remove(taskId);
                    }
                    catch ( KeeperException.NoNodeException ignore )
                    {
                        // ignore - must have been deleted in the interim
                    }
                    catch ( Exception e )
                    {
                        throw new RuntimeException("Trying to read completed task info from: " + fullPath, e);
                    }
                }               
            });

            // remaining started tasks have not completed
            startedTasks.forEach((key, startedTask) -> taskInfos.add(new TaskInfo(key, startedTask.getInstanceName(), startedTask.getStartDateUtc(), startedTask.getProgress())));

            // finally, taskIds not added have not started
            notStartedTasks.forEach(taskId -> taskInfos.add(new TaskInfo(taskId)));
        }
        catch ( Throwable e )
        {
            throw new RuntimeException(e);
        }
        return taskInfos;
    }

    public Serializer getSerializer()
    {
        return serializer;
    }

    @VisibleForTesting
    SchedulerSelector getSchedulerSelector()
    {
        return schedulerSelector;
    }

    private void executeTask(TaskExecutor taskExecutor, ExecutableTask executableTask)
    {
        if ( state.get() != State.STARTED )
        {
            return;
        }

        String path = ZooKeeperConstants.getCompletedTaskPath(executableTask.getRunId(), executableTask.getTaskId());
        try
        {
            if ( curator.checkExists().forPath(path) != null )
            {
                log.warn("Attempt to execute an already complete task - skipping - most likely due to a system restart: " + executableTask);
                return;
            }
        }
        catch ( Exception e )
        {
            log.error("Could not check task completion: " + executableTask, e);
            throw new RuntimeException(e);
        }

        log.info("Executing task: " + executableTask);
        TaskExecution taskExecution = taskExecutor.newTaskExecution(this, executableTask);

        TaskExecutionResult result;
        try
        {
            FutureTask<TaskExecutionResult> futureTask = new FutureTask<>(taskExecution::execute);
            taskRunnerService.execute(futureTask);
            result = futureTask.get();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            return;
        }
        catch ( ExecutionException e )
        {
            log.error("Could not execute task: " + executableTask, e);
            throw new RuntimeException(e);
        }
        if ( result == null )
        {
            throw new RuntimeException(String.format("null returned from task executor for run: %s, task %s", executableTask.getRunId(), executableTask.getTaskId()));
        }
        byte[] bytes = serializer.serialize(result);
        try
        {
            curator.create().creatingParentContainersIfNeeded().forPath(path, bytes);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // this is an edge case - the system was interrupted before the Curator queue recipe could remove the entry in the queue
            log.warn("Task executed twice - most likely due to a system restart. Task is idempotent so there should be no issues: " + executableTask);
        }
        catch ( Exception e )
        {
            log.error("Could not set completed data for executable task: " + executableTask, e);
            throw new RuntimeException(e);
        }
    }

    private List<QueueConsumer> makeTaskConsumers(QueueFactory queueFactory, List<TaskExecutorSpec> specs)
    {
        ImmutableList.Builder<QueueConsumer> builder = ImmutableList.builder();
        specs.forEach(spec -> IntStream.range(0, spec.getQty()).forEach(i -> {
            QueueConsumer consumer = queueFactory.createQueueConsumer(this, t -> executeTask(spec.getTaskExecutor(), t), spec.getTaskType());
            builder.add(consumer);
        }));

        return builder.build();
    }
    
    @Override
	public void gracefulShutdown(long timeOut, TimeUnit timeUnit) {
		 if ( state.compareAndSet(State.STARTED, State.CLOSED) )
	        {
	            CloseableUtils.closeQuietly(schedulerSelector);
	            consumers.forEach(consumer -> consumer.closeGraceFully(timeOut, timeUnit));
	        }
	}
}
