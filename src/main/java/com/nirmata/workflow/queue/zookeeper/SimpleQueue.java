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
package com.nirmata.workflow.queue.zookeeper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskMode;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.TaskRunner;
import com.nirmata.workflow.serialization.Serializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.EnsureContainers;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

// copied and modified from org.apache.curator.framework.recipes.queue.SimpleDistributedQueue
@VisibleForTesting
public class SimpleQueue implements Closeable, QueueConsumer
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final TaskRunner taskRunner;
    private final Serializer serializer;
    private final String path;
    private final String lockPath;
    private final boolean idempotent;
    private final ExecutorService executorService = ThreadUtils.newSingleThreadExecutor("SimpleQueue");
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final EnsureContainers ensurePath;
    private final NodeFunc nodeFunc;
    private final KeyFunc keyFunc;
    private final AtomicReference<WorkflowManagerState.State> state = new AtomicReference<>(WorkflowManagerState.State.LATENT);

    private static final String PREFIX = "qn-";
    private static final String SEPARATOR = "|";

    @FunctionalInterface
    private interface KeyFunc
    {
        String apply(String key, long value);
    }
    private static final Map<TaskMode, KeyFunc> keyFuncs;
    static
    {
        ImmutableMap.Builder<TaskMode, KeyFunc> builder = ImmutableMap.builder();
        builder.put(TaskMode.STANDARD, (key, value) -> key);
        builder.put(TaskMode.PRIORITY, (key, value) -> key + priorityToString(value));
        builder.put(TaskMode.DELAY, (key, value) -> key + epochToString(value));
        keyFuncs = builder.build();
    }

    private static class NodeAndDelay
    {
        final Optional<String> node;
        final Optional<Long> delay;

        public NodeAndDelay()
        {
            node = Optional.empty();
            delay = Optional.empty();
        }

        public NodeAndDelay(String node)
        {
            this.node = Optional.of(node);
            delay = Optional.empty();
        }

        public NodeAndDelay(Optional<String> node)
        {
            this.node = node;
            delay = Optional.empty();
        }

        public NodeAndDelay(long delay)
        {
            node = Optional.empty();
            this.delay = Optional.of(delay);
        }
    }
    @FunctionalInterface
    private interface NodeFunc
    {
        NodeAndDelay getNode(List<String> nodes);
    }
    private static final Map<TaskMode, NodeFunc> nodeFuncs;
    static
    {
        Random random = new Random();
        ImmutableMap.Builder<TaskMode, NodeFunc> builder = ImmutableMap.builder();
        builder.put(TaskMode.STANDARD, nodes -> new NodeAndDelay(nodes.get(random.nextInt(nodes.size()))));
        builder.put(TaskMode.PRIORITY, nodes -> new NodeAndDelay(nodes.stream().sorted().findFirst()));
        builder.put(TaskMode.DELAY, nodes -> {
            final long sortTime = System.currentTimeMillis();
            Optional<String> first = nodes.stream().sorted(delayComparator(sortTime)).findFirst();
            if ( first.isPresent() )
            {
                long delay = getDelay(first.get(), sortTime);
                return (delay > 0) ? new NodeAndDelay(delay) : new NodeAndDelay(first.get());
            }
            return new NodeAndDelay();
        });
        nodeFuncs = builder.build();
    }
    private static Comparator<String> delayComparator(long sortTime)
    {
        return (s1, s2) -> {
            long        diff = getDelay(s1, sortTime) - getDelay(s2, sortTime);
            return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
        };
    }
    private static long getDelay(String itemNode, long sortTime)
    {
        long epoch = getEpoch(itemNode);
        return epoch - sortTime;
    }

    SimpleQueue(CuratorFramework client, TaskRunner taskRunner, Serializer serializer, String queuePath, String lockPath, TaskMode mode, boolean idempotent)
    {
        this.client = client;
        this.taskRunner = taskRunner;
        this.serializer = serializer;
        this.path = queuePath;
        this.lockPath = lockPath;
        this.idempotent = idempotent;
        ensurePath = new EnsureContainers(client, path);
        nodeFunc = nodeFuncs.getOrDefault(mode, nodeFuncs.get(TaskMode.STANDARD));
        keyFunc = keyFuncs.getOrDefault(mode, keyFuncs.get(TaskMode.STANDARD));
    }

    @VisibleForTesting
    public static volatile Semaphore debugQueuedTasks = null;

    void put(byte[] data, long value) throws Exception
    {
        BackgroundCallback debugBackgroundCallback = null;
        if ( debugQueuedTasks != null )
        {
            debugBackgroundCallback = (client, event) -> {
                if ( debugQueuedTasks != null )
                {
                    debugQueuedTasks.release();
                }
            };
        }

        String basePath = ZKPaths.makePath(path, PREFIX);
        String path = keyFunc.apply(basePath, value);
        client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).inBackground(debugBackgroundCallback).forPath(path, data);
    }

    @Override
    public WorkflowManagerState.State getState()
    {
        return state.get();
    }

    @Override
    public void debugValidateClosed()
    {
        Preconditions.checkState(executorService.isTerminated());
    }

    public void start()
    {
        if ( started.compareAndSet(false, true) )
        {
            executorService.submit(this::runLoop);
        }
    }

    @Override
    public void close()
    {
        if ( started.compareAndSet(true, false) )
        {
            executorService.shutdownNow();
        }
    }

    private void runLoop()
    {
        log.info("Starting runLoop");

        try
        {
            while ( started.get() && !Thread.currentThread().isInterrupted() )
            {
                state.set(WorkflowManagerState.State.SLEEPING);
                try
                {
                    ensurePath.ensure();

                    CountDownLatch latch = new CountDownLatch(1);
                    Watcher watcher = event -> latch.countDown();
                    List<String> nodes = client.getChildren().usingWatcher(watcher).forPath(path);
                    if ( nodes.size() == 0 )
                    {
                        latch.await();
                    }
                    else
                    {
                        NodeAndDelay nodeAndDelay = nodeFunc.getNode(nodes);
                        if ( nodeAndDelay.delay.isPresent() )
                        {
                            latch.await(nodeAndDelay.delay.get(), TimeUnit.MILLISECONDS);
                        }
                        if ( nodeAndDelay.node.isPresent() )
                        {
                            state.set(WorkflowManagerState.State.PROCESSING);
                            processNode(nodeAndDelay.node.get());
                        }
                    }
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    break;
                }
                catch ( KeeperException.NoNodeException ignore )
                {
                    log.debug("Got KeeperException.NoNodeException - resetting EnsureContainers");
                    ensurePath.reset();
                }
                catch ( Exception e )
                {
                    log.error("Could not process queue", e);
                }
            }
        }
        finally
        {
            log.info("Exiting runLoop");
            state.set(WorkflowManagerState.State.CLOSED);
        }
    }

    private void processNode(String node) throws Exception
    {
        String lockNodePath = ZKPaths.makePath(lockPath, node);
        boolean lockCreated = false;
        try
        {
            try
            {
                if ( idempotent )
                {
                    client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(lockNodePath);
                    lockCreated = true;
                }

                String itemPath = ZKPaths.makePath(this.path, node);
                Stat stat = new Stat();
                byte[] bytes = client.getData().storingStatIn(stat).forPath(itemPath);
                ExecutableTask executableTask = serializer.deserialize(bytes, ExecutableTask.class);
                try
                {
                    if ( !idempotent )
                    {
                        client.delete().withVersion(stat.getVersion()).forPath(itemPath);
                    }
                    taskRunner.executeTask(executableTask);
                    if ( idempotent )
                    {
                        client.delete().guaranteed().forPath(itemPath);
                    }
                }
                catch ( Throwable e )
                {
                    log.error("Exception processing task at: " + itemPath, e);
                }
            }
            catch ( KeeperException.NodeExistsException ignore )
            {
                // another process got it
            }
        }
        catch ( KeeperException.NoNodeException | KeeperException.BadVersionException ignore )
        {
            // another process got this node already - ignore
        }
        finally
        {
            if ( lockCreated )
            {
                client.delete().guaranteed().forPath(lockNodePath);
            }
        }
    }

    private static String priorityToString(long priority)
    {
        // the padded hex val of the number prefixed with a 0 for negative numbers
        // and a 1 for positive (so that it sorts correctly)
        long        l = priority & 0xFFFFFFFFL;
        return String.format("%s%08X", (priority >= 0) ? "1" : "0", l);
    }

    private static String epochToString(long epoch)
    {
        return SEPARATOR + String.format("%08X", epoch) + SEPARATOR;
    }

    private static long getEpoch(String itemNode)
    {
        int     index2 = itemNode.lastIndexOf(SEPARATOR);
        int     index1 = (index2 > 0) ? itemNode.lastIndexOf(SEPARATOR, index2 - 1) : -1;
        if ( (index1 > 0) && (index2 > (index1 + 1)) )
        {
            try
            {
                String  epochStr = itemNode.substring(index1 + 1, index2);
                return Long.parseLong(epochStr, 16);
            }
            catch ( NumberFormatException ignore )
            {
                // ignore
            }
        }
        return 0;
    }
    
    @Override
	public void closeGraceFully(long timeOut, TimeUnit timeUnit) {
		 if ( started.compareAndSet(true, false) )
	        {
	            executorService.shutdown();
	            try {
					executorService.awaitTermination(timeOut, timeUnit);
				} catch (InterruptedException e) {
					log.error("Exception occurred while waiting for running tasks to complete", e);
					Thread.currentThread().interrupt();
				}
	        }
		
	}
}
