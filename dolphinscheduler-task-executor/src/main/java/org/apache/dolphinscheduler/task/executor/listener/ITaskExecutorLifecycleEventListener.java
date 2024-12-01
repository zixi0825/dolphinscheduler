/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.task.executor.listener;

import org.apache.dolphinscheduler.task.executor.events.TaskExecutorDispatchedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorFailedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorFinalizeLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorKillLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorKilledLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorPauseLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorPausedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorRuntimeContextChangedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorStartedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorSuccessLifecycleEvent;

public interface ITaskExecutorLifecycleEventListener {

    void onTaskExecutorDispatchedLifecycleEvent(final TaskExecutorDispatchedLifecycleEvent event);

    void onTaskExecutorStartedLifecycleEvent(final TaskExecutorStartedLifecycleEvent event);

    void onTaskExecutorRuntimeContextChangedEvent(final TaskExecutorRuntimeContextChangedLifecycleEvent event);

    void onTaskExecutorPauseLifecycleEvent(final TaskExecutorPauseLifecycleEvent event);

    void onTaskExecutorPausedLifecycleEvent(final TaskExecutorPausedLifecycleEvent event);

    void onTaskExecutorKillLifecycleEvent(final TaskExecutorKillLifecycleEvent event);

    void onTaskExecutorKilledLifecycleEvent(final TaskExecutorKilledLifecycleEvent event);

    void onTaskExecutorSuccessLifecycleEvent(final TaskExecutorSuccessLifecycleEvent event);

    void onTaskExecutorFailLifecycleEvent(final TaskExecutorFailedLifecycleEvent event);

    void onTaskExecutorFinalizeLifecycleEvent(final TaskExecutorFinalizeLifecycleEvent event);

}
