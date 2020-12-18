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
package com.nirmata.workflow;

import com.nirmata.workflow.admin.WorkflowAdmin;
import com.nirmata.workflow.events.WorkflowListenerManager;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import org.apache.curator.framework.CuratorFramework;
import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Main API - create via {@link WorkflowManagerBuilder}
 */
public interface WorkflowManager extends Closeable
{
    /**
     * <p>
     *     The manager must be started before use. Call {@link #close()} when done
     *     with the manager. Every instance that starts a manager using
     *     the same {@link WorkflowManagerBuilder#withCurator(CuratorFramework, String, String)} <code>namespace</code>
     *     and <code>version</code> will participate in the workflow.
     * </p>
     *
     * <p>
     *     One instance will be nominated as the scheduler and will be
     *     responsible for starting tasks and advancing the workflow.
     * </p>
     *
     * <p>
     *     Task executors will be started based on the values in the {@link WorkflowManagerBuilder}.
     *     Each WorkflowManager can declare a different combination of task executors
     *     as needed.
     * </p>
     */
    void start();

    /**
     * Submit a task for execution. The task will start nearly immediately. There's no
     * guarantee which instances will execute the various tasks. This method can be called
     * by any instance. i.e. it's not necessary that this be called from the currently
     * nominated scheduler instance.
     *
     * @param task task to execute
     * @return the Run ID for the task
     */
    RunId submitTask(Task task);

    /**
     * Submit a task for execution. The task will start nearly immediately. There's no
     * guarantee which instances will execute the various tasks. This method can be called
     * by any instance. i.e. it's not necessary that this be called from the currently
     * nominated scheduler instance.
     *
     * @param runId the RunId to use - MUST BE GLOBALLY UNIQUE
     * @param task task to execute
     * @return the Run ID for the task
     */
    RunId submitTask(RunId runId, Task task);

    /**
     * Same as {@link #submitTask(Task)} except that, when completed, the parent run will
     * be notified. This method is meant to be used inside of {@link TaskExecutor} for a task
     * that needs to initiate a sub-run and have the parent run wait for the sub-run to complete.
     *
     * @param parentRunId run id of the parent run
     * @param task task to execute
     * @return sub-Run ID
     */
    RunId submitSubTask(RunId parentRunId, Task task);

    /**
     * Same as {@link #submitTask(Task)} except that, when completed, the parent run will
     * be notified. This method is meant to be used inside of {@link TaskExecutor} for a task
     * that needs to initiate a sub-run and have the parent run wait for the sub-run to complete.
     *
     * @param runId the RunId to use - MUST BE GLOBALLY UNIQUE
     * @param parentRunId run id of the parent run
     * @param task task to execute
     * @return sub-Run ID
     */
    RunId submitSubTask(RunId runId, RunId parentRunId, Task task);

    /**
     * Update task progress info. This method is meant to be used inside of {@link TaskExecutor}
     * for a running task to update its execution progress(0-100).
     *
     * @param runId run id of the task
     * @param taskId the task
     * @param progress progress to be set
     */
    void updateTaskProgress(RunId runId, TaskId taskId, int progress);

    /**
     * Attempt to cancel the given run. NOTE: the cancellation is scheduled and does not
     * occur immediately. Currently executing tasks will continue. Only tasks that have not yet
     * executed can be canceled.
     *
     * @param runId the run to cancel
     * @return true if the run was found and the cancellation was scheduled
     */
    boolean cancelRun(RunId runId);

    /**
     * Return the result for a given task of a given run
     *
     * @param runId the run
     * @param taskId the task
     * @return if found, a loaded optional with the result. Otherwise, an empty optional.
     */
    Optional<TaskExecutionResult> getTaskExecutionResult(RunId runId, TaskId taskId);

    /**
     * Return administration operations
     *
     * @return admin
     */
    WorkflowAdmin getAdmin();

    /**
     * Allocate a new WorkflowListenerManager
     *
     * @return new WorkflowListenerManager
     */
    WorkflowListenerManager newWorkflowListenerManager();
    
    /**
     *  Attempt to stop executing new tasks. 
     *  Already running tasks will continue till timeout occurs.
     *  If tasks running are supposed to take long time, its better to give bigger timeOut value.
     *  
     * @param timeOut
     * @param timeUnit the timeunit of timeout (e.g SECONDS, MINUTES)
     */
    void gracefulShutdown(long timeOut, TimeUnit timeUnit);
}
