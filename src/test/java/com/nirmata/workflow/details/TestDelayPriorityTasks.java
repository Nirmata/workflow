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

import com.google.common.collect.Lists;
import com.nirmata.workflow.BaseForTests;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.WorkflowManagerBuilder;
import com.nirmata.workflow.executor.TaskExecutionStatus;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskMode;
import com.nirmata.workflow.models.TaskType;
import org.testng.annotations.Test;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import static com.nirmata.workflow.WorkflowAssertions.assertThat;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestDelayPriorityTasks extends BaseForTests
{
    @Test
    public void testDelay() throws Exception
    {
        final long delayMs = 5000;

        BlockingQueue<Long> queue = new LinkedBlockingQueue<>();
        TaskExecutor taskExecutor = (workflowManager, executableTask) -> () ->
        {
            queue.add(System.currentTimeMillis());
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };
        TaskType taskType = new TaskType("test", "1", true, TaskMode.DELAY);
        try ( WorkflowManager workflowManager = WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 10, taskType)
            .withCurator(curator, "test", "1")
            .build() )
        {
            workflowManager.start();

            Task task = new Task(new TaskId(), taskType);
            long startMs = System.currentTimeMillis();
            workflowManager.submitTask(task);

            assertThat(queue.poll(1000, MILLISECONDS))
                    .isLessThan(startMs + 1000); // should have executed immediately

            task = new Task(new TaskId(), taskType, Lists.newArrayList(), Task.makeSpecialMeta(System.currentTimeMillis() + delayMs));
            startMs = System.currentTimeMillis();
            workflowManager.submitTask(task);

            assertThat(queue.poll(delayMs * 2, MILLISECONDS))
                    .isGreaterThanOrEqualTo(startMs + delayMs);
        }
    }

    @Test
    public void testPriority() throws Exception
    {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        TaskExecutor taskExecutor = (workflowManager, executableTask) -> () ->
        {
            queue.add(executableTask.getTaskId().getId());
            try
            {
                Thread.sleep(10);
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
            return new TaskExecutionResult(TaskExecutionStatus.SUCCESS, "");
        };
        TaskType taskType = new TaskType("test", "1", true, TaskMode.PRIORITY);
        try ( WorkflowManagerImpl workflowManager = (WorkflowManagerImpl) WorkflowManagerBuilder.builder()
            .addingTaskExecutor(taskExecutor, 1, taskType)
            .withCurator(curator, "test", "1")
            .build() )
        {
            Scheduler.debugQueuedTasks = new Semaphore(0);
            workflowManager.debugDontStartConsumers = true; // make sure all tasks are added to ZK before they start getting consumed
            workflowManager.start();

            Task task1 = new Task(new TaskId("1"), taskType, Lists.newArrayList(), Task.makeSpecialMeta(1));
            Task task2 = new Task(new TaskId("2"), taskType, Lists.newArrayList(), Task.makeSpecialMeta(10));
            Task task3 = new Task(new TaskId("3"), taskType, Lists.newArrayList(), Task.makeSpecialMeta(5));
            Task task4 = new Task(new TaskId("4"), taskType, Lists.newArrayList(), Task.makeSpecialMeta(30));
            Task task5 = new Task(new TaskId("5"), taskType, Lists.newArrayList(), Task.makeSpecialMeta(20));
            workflowManager.submitTask(task1);
            workflowManager.submitTask(task2);
            workflowManager.submitTask(task3);
            workflowManager.submitTask(task4);
            workflowManager.submitTask(task5);

            assertThat(Scheduler.debugQueuedTasks.tryAcquire(5, 5000, MILLISECONDS)).isTrue();
            workflowManager.startQueueConsumers();

            assertThat(queue.poll(1000, MILLISECONDS)).isEqualTo("1");
            assertThat(queue.poll(1000, MILLISECONDS)).isEqualTo("3");
            assertThat(queue.poll(1000, MILLISECONDS)).isEqualTo("2");
            assertThat(queue.poll(1000, MILLISECONDS)).isEqualTo("5");
            assertThat(queue.poll(1000, MILLISECONDS)).isEqualTo("4");
        }
        finally
        {
            Scheduler.debugQueuedTasks = null;
        }
    }
}
