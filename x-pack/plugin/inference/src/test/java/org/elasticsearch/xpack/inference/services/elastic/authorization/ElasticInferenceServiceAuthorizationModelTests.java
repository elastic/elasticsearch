/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceAuthorizationModelTests extends ESTestCase {
    public static ElasticInferenceServiceAuthorizationModel createEnabledAuth() {
        return ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel("model-1", EnumSet.of(TaskType.TEXT_EMBEDDING))
                )
            )
        );
    }

    public void testIsAuthorized_ReturnsFalse_WithEmptyMap() {
        assertFalse(ElasticInferenceServiceAuthorizationModel.newDisabledService().isAuthorized());
    }

    public void testExcludes_ModelsWithoutTaskTypes() {
        var response = new ElasticInferenceServiceAuthorizationResponseEntity(
            List.of(new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel("model-1", EnumSet.noneOf(TaskType.class)))
        );
        var auth = ElasticInferenceServiceAuthorizationModel.of(response);
        assertTrue(auth.getAuthorizedTaskTypes().isEmpty());
        assertFalse(auth.isAuthorized());
    }

    public void testEnabledTaskTypes_MergesFromSeparateModels() {
        var auth = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel("model-1", EnumSet.of(TaskType.TEXT_EMBEDDING)),
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel("model-2", EnumSet.of(TaskType.SPARSE_EMBEDDING))
                )
            )
        );
        assertThat(auth.getAuthorizedTaskTypes(), is(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)));
        assertThat(auth.getAuthorizedModelIds(), is(Set.of("model-1", "model-2")));
    }

    public void testEnabledTaskTypes_FromSingleEntry() {
        var auth = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-1",
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                    )
                )
            )
        );

        assertThat(auth.getAuthorizedTaskTypes(), is(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)));
        assertThat(auth.getAuthorizedModelIds(), is(Set.of("model-1")));
    }

    public void testNewLimitToTaskTypes_SingleModel() {
        var auth = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-1",
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                    ),
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel("model-2", EnumSet.of(TaskType.CHAT_COMPLETION))
                )
            )
        );

        assertThat(
            auth.newLimitedToTaskTypes(EnumSet.of(TaskType.TEXT_EMBEDDING)),
            is(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-1",
                                EnumSet.of(TaskType.TEXT_EMBEDDING)
                            )
                        )
                    )
                )
            )
        );
    }

    public void testNewLimitToTaskTypes_MultipleModels_OnlyTextEmbedding() {
        var auth = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-1",
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                    ),
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel("model-2", EnumSet.of(TaskType.TEXT_EMBEDDING))
                )
            )
        );

        assertThat(
            auth.newLimitedToTaskTypes(EnumSet.of(TaskType.TEXT_EMBEDDING)),
            is(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-1",
                                EnumSet.of(TaskType.TEXT_EMBEDDING)
                            ),
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-2",
                                EnumSet.of(TaskType.TEXT_EMBEDDING)
                            )
                        )
                    )
                )
            )
        );
    }

    public void testNewLimitToTaskTypes_MultipleModels_MultipleTaskTypes() {
        var auth = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-text-sparse",
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                    ),
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-sparse",
                        EnumSet.of(TaskType.SPARSE_EMBEDDING)
                    ),
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-chat-completion",
                        EnumSet.of(TaskType.CHAT_COMPLETION)
                    )
                )
            )
        );

        var limitedAuth = auth.newLimitedToTaskTypes(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.CHAT_COMPLETION));
        assertThat(
            limitedAuth,
            is(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-text-sparse",
                                EnumSet.of(TaskType.TEXT_EMBEDDING)
                            ),
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-chat-completion",
                                EnumSet.of(TaskType.CHAT_COMPLETION)
                            )
                        )
                    )
                )
            )
        );
    }

    public void testNewLimitToTaskTypes_DuplicateModelNames() {
        var auth = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-1",
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                    ),
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-1",
                        EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING, TaskType.RERANK)
                    )
                )
            )
        );

        var limitedAuth = auth.newLimitedToTaskTypes(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING, TaskType.RERANK));
        assertThat(
            limitedAuth,
            is(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-1",
                                EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING, TaskType.RERANK)
                            )
                        )
                    )
                )
            )
        );
    }

    public void testNewLimitToTaskTypes_ReturnsDisabled_WhenNoOverlapForTaskTypes() {
        var auth = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-1",
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                    ),
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-2",
                        EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING)
                    )
                )
            )
        );

        var limitedAuth = auth.newLimitedToTaskTypes(EnumSet.of(TaskType.CHAT_COMPLETION, TaskType.RERANK));
        assertThat(limitedAuth, is(ElasticInferenceServiceAuthorizationModel.newDisabledService()));
    }

    public void testMerge_CombinesCorrectly() {
        var auth1 = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-1",
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                    )
                )
            )
        );

        var auth2 = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel("model-2", EnumSet.of(TaskType.SPARSE_EMBEDDING))
                )
            )
        );

        assertThat(
            auth1.merge(auth2),
            is(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-1",
                                EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                            ),
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-2",
                                EnumSet.of(TaskType.SPARSE_EMBEDDING)
                            )
                        )
                    )
                )
            )
        );
    }

    public void testMerge_AddsNewTaskType() {
        var auth1 = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-1",
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                    )
                )
            )
        );

        var auth2 = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel("model-2", EnumSet.of(TaskType.CHAT_COMPLETION))
                )
            )
        );

        assertThat(
            auth1.merge(auth2),
            is(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-1",
                                EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                            ),
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-2",
                                EnumSet.of(TaskType.CHAT_COMPLETION)
                            )
                        )
                    )
                )
            )
        );
    }

    public void testMerge_IgnoresDuplicates() {
        var auth1 = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-1",
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                    )
                )
            )
        );

        var auth2 = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-1",
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                    )
                )
            )
        );

        assertThat(
            auth1.merge(auth2),
            is(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-1",
                                EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                            )
                        )
                    )
                )
            )
        );
    }

    public void testMerge_CombinesCorrectlyWithEmptyModel() {
        var auth1 = ElasticInferenceServiceAuthorizationModel.of(
            new ElasticInferenceServiceAuthorizationResponseEntity(
                List.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                        "model-1",
                        EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                    )
                )
            )
        );

        var auth2 = ElasticInferenceServiceAuthorizationModel.newDisabledService();

        assertThat(
            auth1.merge(auth2),
            is(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-1",
                                EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING)
                            )
                        )
                    )
                )
            )
        );
    }
}
