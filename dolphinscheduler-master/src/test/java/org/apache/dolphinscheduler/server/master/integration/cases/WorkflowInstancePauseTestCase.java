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

package org.apache.dolphinscheduler.server.master.integration.cases;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.extract.master.command.RunWorkflowCommandParam;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.server.master.AbstractMasterIntegrationTestCase;
import org.apache.dolphinscheduler.server.master.integration.WorkflowOperator;
import org.apache.dolphinscheduler.server.master.integration.WorkflowTestCaseContext;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The integration test for pausing a workflow instance.
 */
public class WorkflowInstancePauseTestCase extends AbstractMasterIntegrationTestCase {

    @Test
    @DisplayName("Test pause a workflow with one success task")
    public void testPauseWorkflow_with_oneSuccessTask() {
        final String yaml = "/it/pause/workflow_with_one_fake_task_success.yaml";
        final WorkflowTestCaseContext context = workflowTestCaseContextFactory.initializeContextFromYaml(yaml);
        final WorkflowDefinition workflow = context.getOneWorkflow();

        final WorkflowOperator.WorkflowTriggerDTO workflowTriggerDTO = WorkflowOperator.WorkflowTriggerDTO.builder()
                .workflowDefinition(workflow)
                .runWorkflowCommandParam(new RunWorkflowCommandParam())
                .build();
        final Integer workflowInstanceId = workflowOperator.manualTriggerWorkflow(workflowTriggerDTO);

        await()
                .pollInterval(Duration.ofMillis(100))
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    assertThat(repository.queryWorkflowInstance(workflowInstanceId))
                            .satisfies(workflowInstance -> {
                                assertThat(workflowInstance.getState())
                                        .isEqualTo(WorkflowExecutionStatus.RUNNING_EXECUTION);
                            });
                    assertThat(repository.queryTaskInstance(workflowInstanceId))
                            .satisfiesExactly(taskInstance -> {
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.RUNNING_EXECUTION);
                            });
                });

        assertThat(workflowOperator.pauseWorkflowInstance(workflowInstanceId).isSuccess()).isTrue();

        await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    assertThat(repository.queryWorkflowInstance(workflowInstanceId))
                            .satisfies(workflowInstance -> {
                                assertThat(workflowInstance.getState()).isEqualTo(WorkflowExecutionStatus.SUCCESS);
                            });
                    assertThat(repository.queryTaskInstance(workflowInstanceId))
                            .satisfiesExactly(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("A");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.SUCCESS);
                            });
                });
        masterContainer.assertAllResourceReleased();
    }

    @Test
    @DisplayName("Test pause a workflow with one failed task")
    public void testPauseWorkflow_with_oneFailedTask() {
        final String yaml = "/it/pause/workflow_with_one_fake_task_failed.yaml";
        final WorkflowTestCaseContext context = workflowTestCaseContextFactory.initializeContextFromYaml(yaml);
        final WorkflowDefinition workflow = context.getOneWorkflow();

        final WorkflowOperator.WorkflowTriggerDTO workflowTriggerDTO = WorkflowOperator.WorkflowTriggerDTO.builder()
                .workflowDefinition(workflow)
                .runWorkflowCommandParam(new RunWorkflowCommandParam())
                .build();
        final Integer workflowInstanceId = workflowOperator.manualTriggerWorkflow(workflowTriggerDTO);

        await()
                .pollInterval(Duration.ofMillis(100))
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    assertThat(repository.queryWorkflowInstance(workflowInstanceId))
                            .satisfies(workflowInstance -> {
                                assertThat(workflowInstance.getState())
                                        .isEqualTo(WorkflowExecutionStatus.RUNNING_EXECUTION);
                            });
                    assertThat(
                            repository.queryTaskInstance(workflowInstanceId))
                                    .satisfiesExactly(taskInstance -> {
                                        assertThat(taskInstance.getState())
                                                .isEqualTo(TaskExecutionStatus.RUNNING_EXECUTION);
                                    });
                });

        assertThat(workflowOperator.pauseWorkflowInstance(workflowInstanceId).isSuccess()).isTrue();

        await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    assertThat(repository.queryWorkflowInstance(workflowInstanceId))
                            .satisfies(workflowInstance -> {
                                assertThat(workflowInstance.getState()).isEqualTo(WorkflowExecutionStatus.FAILURE);
                            });
                    assertThat(repository.queryTaskInstance(workflowInstanceId))
                            .satisfiesExactly(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("A");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.FAILURE);
                            });
                });
        masterContainer.assertAllResourceReleased();
    }

    @Test
    @DisplayName("Test pause a workflow with three parallel three fake tasks(A1->A2->A3, B1->B2->B3, C1->C2->C3) success")
    public void testPauseWorkflow_with_threeParallelSuccessTask() {
        final String yaml = "/it/pause/workflow_with_three_parallel_three_fake_task_success.yaml";
        final WorkflowTestCaseContext context = workflowTestCaseContextFactory.initializeContextFromYaml(yaml);
        final WorkflowDefinition workflow = context.getOneWorkflow();

        final WorkflowOperator.WorkflowTriggerDTO workflowTriggerDTO = WorkflowOperator.WorkflowTriggerDTO.builder()
                .workflowDefinition(workflow)
                .runWorkflowCommandParam(new RunWorkflowCommandParam())
                .build();
        final Integer workflowInstanceId = workflowOperator.manualTriggerWorkflow(workflowTriggerDTO);

        await()
                .pollInterval(Duration.ofMillis(50))
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    assertThat(repository.queryWorkflowInstance(workflowInstanceId))
                            .satisfies(workflowInstance -> {
                                assertThat(workflowInstance.getState())
                                        .isEqualTo(WorkflowExecutionStatus.RUNNING_EXECUTION);
                            });
                });

        await()
                .pollInterval(Duration.ofMillis(50))
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    assertThat(repository.queryTaskInstance(workflowInstanceId))
                            .anySatisfy(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("A1");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.RUNNING_EXECUTION);
                            })
                            .anySatisfy(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("B1");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.RUNNING_EXECUTION);
                            })
                            .anySatisfy(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("C1");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.RUNNING_EXECUTION);
                            });
                });

        assertThat(workflowOperator.pauseWorkflowInstance(workflowInstanceId).isSuccess()).isTrue();

        await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    assertThat(repository.queryWorkflowInstance(workflowInstanceId))
                            .satisfies(workflowInstance -> {
                                assertThat(workflowInstance.getState()).isEqualTo(WorkflowExecutionStatus.PAUSE);
                            });
                    assertThat(repository.queryTaskInstance(workflowInstanceId))
                            .hasSize(6)
                            .anySatisfy(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("A1");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.SUCCESS);
                            })
                            .anySatisfy(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("B1");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.SUCCESS);
                            })
                            .anySatisfy(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("C1");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.SUCCESS);
                            })
                            .anySatisfy(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("A2");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.PAUSE);
                            })
                            .anySatisfy(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("B2");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.PAUSE);
                            })
                            .anySatisfy(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("C2");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.PAUSE);
                            });
                });
        masterContainer.assertAllResourceReleased();
    }

    @Test
    @DisplayName("Test pause a workflow with one sub workflow task")
    public void testPauseWorkflow_with_subWorkflowTask_success() {
        final String yaml = "/it/pause/workflow_with_sub_workflow_task_success.yaml";
        final WorkflowTestCaseContext context = workflowTestCaseContextFactory.initializeContextFromYaml(yaml);
        final WorkflowDefinition workflow = context.getOneWorkflow();

        final WorkflowOperator.WorkflowTriggerDTO workflowTriggerDTO = WorkflowOperator.WorkflowTriggerDTO.builder()
                .workflowDefinition(workflow)
                .runWorkflowCommandParam(new RunWorkflowCommandParam())
                .build();
        final Integer workflowInstanceId = workflowOperator.manualTriggerWorkflow(workflowTriggerDTO);

        final WorkflowDefinition subWorkflowDefinition = context.getWorkflows().get(1);
        await()
                .pollInterval(Duration.ofMillis(50))
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    assertThat(repository.queryWorkflowInstance(workflowInstanceId))
                            .satisfies(workflowInstance -> {
                                assertThat(workflowInstance.getState())
                                        .isEqualTo(WorkflowExecutionStatus.RUNNING_EXECUTION);
                            });
                    assertThat(repository.queryTaskInstance(workflowInstanceId))
                            .satisfiesExactly(taskInstance -> {
                                assertThat(taskInstance.getState())
                                        .isEqualTo(TaskExecutionStatus.RUNNING_EXECUTION);
                            });
                    // Wait the sub workflow instance's task is running, then pause it
                    // Otherwise the subworkflow instance's first task might be paused
                    assertThat(repository.queryWorkflowInstance(subWorkflowDefinition))
                            .satisfiesExactly(workflowInstance -> {
                                assertThat(workflowInstance.getState())
                                        .isEqualTo(WorkflowExecutionStatus.RUNNING_EXECUTION);
                            });
                    assertThat(repository.queryTaskInstance(subWorkflowDefinition))
                            .satisfiesExactly(taskInstance -> {
                                assertThat(taskInstance.getState())
                                        .isEqualTo(TaskExecutionStatus.RUNNING_EXECUTION);
                            });
                });

        assertThat(workflowOperator.pauseWorkflowInstance(workflowInstanceId).isSuccess()).isTrue();

        await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    assertThat(repository.queryWorkflowInstance(workflowInstanceId))
                            .satisfies(workflowInstance -> {
                                assertThat(workflowInstance.getState()).isEqualTo(WorkflowExecutionStatus.PAUSE);
                                assertThat(workflowInstance.getIsSubWorkflow()).isEqualTo(Flag.NO);
                            });
                    assertThat(repository.queryTaskInstance(workflowInstanceId))
                            .satisfiesExactly(taskInstance -> {
                                assertThat(taskInstance.getName()).isEqualTo("sub_logic_task");
                                assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.PAUSE);
                            });

                    assertThat(repository.queryWorkflowInstance(subWorkflowDefinition))
                            .satisfiesExactly(workflowInstance -> {
                                assertThat(workflowInstance.getState()).isEqualTo(WorkflowExecutionStatus.PAUSE);
                                assertThat(workflowInstance.getIsSubWorkflow()).isEqualTo(Flag.YES);
                            });

                    final List<TaskInstance> taskInstances = repository.queryTaskInstance(subWorkflowDefinition);
                    assertThat(taskInstances)
                            .hasSize(2);
                    assertThat(taskInstances.get(0).getState())
                            .isEqualTo(TaskExecutionStatus.SUCCESS);
                    assertThat(taskInstances.get(1).getState())
                            .isEqualTo(TaskExecutionStatus.PAUSE);

                });

        masterContainer.assertAllResourceReleased();
    }

    @Test
    @DisplayName("Test pause a workflow with failed retrying task")
    public void testPauseWorkflow_with_failedRetryingTask() {
        final String yaml = "/it/pause/workflow_with_fake_task_failed_retrying.yaml";
        final WorkflowTestCaseContext context = workflowTestCaseContextFactory.initializeContextFromYaml(yaml);
        final WorkflowDefinition workflow = context.getOneWorkflow();

        final WorkflowOperator.WorkflowTriggerDTO workflowTriggerDTO = WorkflowOperator.WorkflowTriggerDTO.builder()
                .workflowDefinition(workflow)
                .runWorkflowCommandParam(new RunWorkflowCommandParam())
                .build();
        final Integer workflowInstanceId = workflowOperator.manualTriggerWorkflow(workflowTriggerDTO);

        await()
                .pollInterval(Duration.ofMillis(100))
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    assertThat(repository.queryWorkflowInstance(workflowInstanceId).getState())
                            .isEqualTo(WorkflowExecutionStatus.RUNNING_EXECUTION);

                    assertThat(repository.queryTaskInstance(workflowInstanceId))
                            .satisfiesExactly(
                                    taskInstance -> {
                                        assertThat(taskInstance.getName()).isEqualTo("FAILED-RETRY");
                                        assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.FAILURE);
                                    });
                });

        assertThat(workflowOperator.pauseWorkflowInstance(workflowInstanceId).isSuccess());

        await()
                .pollInterval(Duration.ofMillis(100))
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    assertThat(repository.queryWorkflowInstance(workflowInstanceId))
                            .satisfies(
                                    workflowInstance -> {
                                        assertThat(workflowInstance.getState())
                                                .isEqualTo(WorkflowExecutionStatus.PAUSE);
                                    });

                    assertThat(repository.queryTaskInstance(workflowInstanceId))
                            .satisfiesExactly(
                                    taskInstance -> {
                                        assertThat(taskInstance.getName()).isEqualTo("FAILED-RETRY");
                                        assertThat(taskInstance.getState()).isEqualTo(TaskExecutionStatus.PAUSE);
                                    });
                });
        masterContainer.assertAllResourceReleased();
    }
}
