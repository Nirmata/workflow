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
import com.google.common.collect.Sets;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.admin.RunInfo;
import com.nirmata.workflow.admin.TaskDetails;
import com.nirmata.workflow.admin.TaskInfo;
import com.nirmata.workflow.admin.WorkflowAdmin;
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
import com.nirmata.workflow.queue.TaskRunner;
import com.nirmata.workflow.serialization.Serializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.joda.time.DateTimeZone.UTC;

public class WorkflowManagerImpl implements WorkflowManager, WorkflowAdmin
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework curator;
    private final String instanceName;
    private final List<QueueConsumer> consumers;
    private final SchedulerSelector schedulerSelector;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final Serializer serializer;

    private static final TaskType nullTaskType = new TaskType("", "", false);

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public WorkflowManagerImpl(CuratorFramework curator, QueueFactory queueFactory, String instanceName, List<TaskExecutorSpec> specs, AutoCleanerHolder autoCleanerHolder, Serializer serializer)
    {
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
        for (QueueConsumer consumer : consumers)
        {
            consumer.start();
        }
    }

    @Override
    public WorkflowListenerManager newWorkflowListenerManager()
    {
        return new WorkflowListenerManagerImpl(this);
    }

    @Override
    public RunId submitTask(Task task)
    {
        return submitSubTask(null, task);
    }

    @Override
    public Map<TaskId, TaskDetails> getTaskDetails(RunId runId)
    {
        try
        {
            String runPath = ZooKeeperConstants.getRunPath(runId);
            byte[] runnableTaskBytes = curator.getData().forPath(runPath);
            RunnableTask runnableTask = serializer.deserialize(runnableTaskBytes, RunnableTask.class);
            ImmutableMap.Builder<TaskId, TaskDetails> result = ImmutableMap.builder();
            for (Map.Entry<TaskId, ExecutableTask> entry : runnableTask.getTasks().entrySet())
            {
                ExecutableTask executableTask = entry.getValue();
                TaskType taskType = executableTask.getTaskType().equals(nullTaskType) ? null : executableTask.getTaskType();
                result.put(entry.getKey(), new TaskDetails(entry.getKey(), taskType, executableTask.getMetaData()));
            }
            return result.build();
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
    public RunId submitSubTask(RunId parentRunId, Task task)
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");

        RunId runId = new RunId();
        RunnableTaskDagBuilder builder = new RunnableTaskDagBuilder(task);
        ImmutableMap.Builder<TaskId, ExecutableTask> tasks = ImmutableMap.builder();
        for (Task t : builder.getTasks().values())
        {
            tasks.put(t.getTaskId(), new ExecutableTask(runId, t.getTaskId(), t.isExecutable() ? t.getTaskType() : nullTaskType, t.getMetaData(), t.isExecutable()));
        }
        RunnableTask runnableTask = new RunnableTask(tasks.build(), builder.getEntries(), LocalDateTime.now(UTC), null, parentRunId);

        try
        {
            byte[] runnableTaskBytes = serializer.serialize(runnableTask);
            String runPath = ZooKeeperConstants.getRunPath(runId);
            curator.create().creatingParentsIfNeeded().forPath(runPath, runnableTaskBytes);
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }

        return runId;
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
    public TaskExecutionResult getTaskExecutionResult(RunId runId, TaskId taskId)
    {
        String completedTaskPath = ZooKeeperConstants.getCompletedTaskPath(runId, taskId);
        try
        {
            byte[] bytes = curator.getData().forPath(completedTaskPath);
            return serializer.deserialize(bytes, TaskExecutionResult.class);
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            // dummy
        }
        catch ( Exception e )
        {
            throw new RuntimeException(String.format("No data for runId %s taskId %s", runId, taskId), e);
        }
        return null;
    }

    public String getInstanceName()
    {
        return instanceName;
    }

    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            CloseableUtils.closeQuietly(schedulerSelector);
            for (QueueConsumer consumer: consumers)
            {
                CloseableUtils.closeQuietly(consumer);
            }
        }
    }

    @Override
    public WorkflowAdmin getAdmin()
    {
        return this;
    }

    @Override
    public boolean clean(RunId runId)
    {
        String runPath = ZooKeeperConstants.getRunPath(runId);
        try
        {
            byte[] bytes = curator.getData().forPath(runPath);
            RunnableTask runnableTask = serializer.deserialize(bytes, RunnableTask.class);
            for (TaskId taskId: runnableTask.getTasks().keySet())
            {
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
            }

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
            return new RunInfo(runId, runnableTask.getStartTimeUtc(), runnableTask.getCompletionTimeUtc());
        }
        catch ( Exception e )
        {
            throw new RuntimeException("Could not read run: " + runId, e);
        }
    }

    @Override
    public List<RunInfo> getRunInfo()
    {
        try
        {
            String runParentPath = ZooKeeperConstants.getRunParentPath();
            List<RunInfo> result = Lists.newArrayList();
            for (String child: curator.getChildren().forPath(runParentPath))
            {
                String fullPath = ZKPaths.makePath(runParentPath, child);
                try
                {
                    RunId runId = new RunId(ZooKeeperConstants.getRunIdFromRunPath(fullPath));
                    byte[] bytes = curator.getData().forPath(fullPath);
                    RunnableTask runnableTask = serializer.deserialize(bytes, RunnableTask.class);
                    result.add(new RunInfo(runId, runnableTask.getStartTimeUtc(), runnableTask.getCompletionTimeUtc()));
                }
                catch ( KeeperException.NoNodeException ignore )
                {
                    // ignore - must have been deleted in the interim
                }
                catch ( Exception e )
                {
                    throw new RuntimeException("Trying to read run info from: " + fullPath, e);
                }
            }
            return result;
        }
        catch ( Exception e )
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

            Set<TaskId> notStartedTasks = Sets.newLinkedHashSet();
            for ( ExecutableTask task : runnableTask.getTasks().values() )
            {
                if ( task.isExecutable() )
                {
                    notStartedTasks.add(task.getTaskId());
                }
            }
            Map<TaskId, StartedTask> startedTasks = Maps.newLinkedHashMap();

            for ( String child: curator.getChildren().forPath(startedTasksParentPath) )
            {
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
            }

            for ( String child: curator.getChildren().forPath(completedTaskParentPath) )
            {
                String fullPath = ZKPaths.makePath(completedTaskParentPath, child);
                TaskId taskId = new TaskId(ZooKeeperConstants.getTaskIdFromCompletedTasksPath(fullPath));
                StartedTask startedTask = startedTasks.remove(taskId);
                if ( startedTask != null )  // otherwise it must have been deleted
                {
                    try
                    {
                        byte[] bytes = curator.getData().forPath(fullPath);
                        TaskExecutionResult taskExecutionResult = serializer.deserialize(bytes, TaskExecutionResult.class);
                        taskInfos.add(new TaskInfo(taskId, startedTask.getInstanceName(), startedTask.getStartDateUtc(), taskExecutionResult));
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
            }

            // remaining started tasks have not completed
            for ( Map.Entry<TaskId, StartedTask> entry: startedTasks.entrySet() )
            {
                StartedTask startedTask = entry.getValue();
                taskInfos.add(new TaskInfo(entry.getKey(), startedTask.getInstanceName(), startedTask.getStartDateUtc()));
            }

            // finally, taskIds not added have not started
            for ( TaskId taskId: notStartedTasks )
            {
                taskInfos.add(new TaskInfo(taskId));
            }
        }
        catch ( Exception e )
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

        TaskExecutionResult result = taskExecution.execute();
        if ( result == null )
        {
            throw new RuntimeException(String.format("null returned from task executor for run: %s, task %s", executableTask.getRunId(), executableTask.getTaskId()));
        }
        byte[] bytes = serializer.serialize(result);
        try
        {
            curator.create().creatingParentsIfNeeded().forPath(path, bytes);
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
        for ( final TaskExecutorSpec spec : specs )
        {
            for ( int i = 0; i < spec.getQty(); i++ )
            {
                QueueConsumer consumer = queueFactory.createQueueConsumer(this, new TaskRunner()
                {
                    @Override
                    public void executeTask(ExecutableTask t)
                    {
                        WorkflowManagerImpl.this.executeTask(spec.getTaskExecutor(), t);
                    }
                }, spec.getTaskType());
                builder.add(consumer);
            }
        }
        return builder.build();
    }
}
