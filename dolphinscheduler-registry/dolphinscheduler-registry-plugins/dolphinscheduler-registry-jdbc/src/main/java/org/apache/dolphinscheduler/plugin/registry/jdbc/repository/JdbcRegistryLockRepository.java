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

package org.apache.dolphinscheduler.plugin.registry.jdbc.repository;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.plugin.registry.jdbc.mapper.JdbcRegistryLockMapper;
import org.apache.dolphinscheduler.plugin.registry.jdbc.model.DO.JdbcRegistryLock;
import org.apache.dolphinscheduler.plugin.registry.jdbc.model.DTO.JdbcRegistryLockDTO;

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcRegistryLockRepository {

    @Autowired
    private JdbcRegistryLockMapper jdbcRegistryLockMapper;

    public List<JdbcRegistryLockDTO> queryAll() {
        return jdbcRegistryLockMapper.selectList(null)
                .stream()
                .map(JdbcRegistryLockDTO::fromJdbcRegistryLock)
                .collect(Collectors.toList());
    }

    public void deleteByClientIds(List<Long> clientIds) {
        if (CollectionUtils.isEmpty(clientIds)) {
            return;
        }
        jdbcRegistryLockMapper.deleteByClientIds(clientIds);
    }

    public void insert(JdbcRegistryLockDTO jdbcRegistryLock) {
        checkNotNull(jdbcRegistryLock);
        JdbcRegistryLock jdbcRegistryLockDO = JdbcRegistryLockDTO.toJdbcRegistryLock(jdbcRegistryLock);
        jdbcRegistryLockMapper.insert(jdbcRegistryLockDO);
        jdbcRegistryLock.setId(jdbcRegistryLockDO.getId());
    }

    public void deleteById(Long id) {
        jdbcRegistryLockMapper.deleteById(id);
    }
}