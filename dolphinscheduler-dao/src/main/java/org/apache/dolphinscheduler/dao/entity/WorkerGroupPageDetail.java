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

package org.apache.dolphinscheduler.dao.entity;

import org.apache.dolphinscheduler.common.enums.WorkerGroupSource;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class WorkerGroupPageDetail extends WorkerGroup {

    private WorkerGroupSource source;

    public WorkerGroupPageDetail(WorkerGroup workerGroup) {
        this.setId(workerGroup.getId());
        this.setName(workerGroup.getName());
        this.setAddrList(workerGroup.getAddrList());
        this.setCreateTime(workerGroup.getCreateTime());
        this.setUpdateTime(workerGroup.getUpdateTime());
        this.setDescription(workerGroup.getDescription());
        this.setSystemDefault(workerGroup.isSystemDefault());
    }
}
