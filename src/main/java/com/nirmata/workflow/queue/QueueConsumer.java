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
package com.nirmata.workflow.queue;

import com.google.common.annotations.VisibleForTesting;
import com.nirmata.workflow.admin.WorkflowManagerState;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public interface QueueConsumer extends Closeable
{
    void start();

    @Override
    void close();

    WorkflowManagerState.State getState();

    @VisibleForTesting
    void debugValidateClosed();
    
    public void closeGraceFully(long timeOut, TimeUnit timeUnit);
}
