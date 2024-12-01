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

package org.apache.dolphinscheduler.server.master.engine.executor;

import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.server.master.engine.executor.plugin.LogicTaskPluginFactoryBuilder;
import org.apache.dolphinscheduler.task.executor.ITaskExecutor;
import org.apache.dolphinscheduler.task.executor.ITaskExecutorFactory;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LogicTaskExecutorFactory implements ITaskExecutorFactory {

    private final LogicTaskPluginFactoryBuilder logicTaskPluginFactoryBuilder;

    public LogicTaskExecutorFactory(final LogicTaskPluginFactoryBuilder logicTaskPluginFactoryBuilder) {
        this.logicTaskPluginFactoryBuilder = logicTaskPluginFactoryBuilder;
    }

    @Override
    public ITaskExecutor createTaskExecutor(final TaskExecutionContext taskExecutionContext) {
        assemblyTaskLogPath(taskExecutionContext);

        final LogicTaskExecutorBuilder logicTaskExecutorBuilder = LogicTaskExecutorBuilder.builder()
                .taskExecutionContext(taskExecutionContext)
                .logicTaskPluginFactoryBuilder(logicTaskPluginFactoryBuilder)
                .build();
        return new LogicTaskExecutor(logicTaskExecutorBuilder);
    }

    private void assemblyTaskLogPath(final TaskExecutionContext taskExecutionContext) {
        taskExecutionContext.setLogPath(LogUtils.getTaskInstanceLogFullPath(taskExecutionContext));
    }

}
