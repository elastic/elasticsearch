/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.util.Set;

import static org.elasticsearch.xpack.inference.InferenceFeatures.EMBEDDING_TASK_TYPE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportPutInferenceModelActionTests extends ESTestCase {

    public void testResolveTaskType() {

        assertEquals(TaskType.SPARSE_EMBEDDING, ServiceUtils.resolveTaskType(TaskType.SPARSE_EMBEDDING, null));
        assertEquals(TaskType.SPARSE_EMBEDDING, ServiceUtils.resolveTaskType(TaskType.ANY, TaskType.SPARSE_EMBEDDING.toString()));

        var e = expectThrows(ElasticsearchStatusException.class, () -> ServiceUtils.resolveTaskType(TaskType.ANY, null));
        assertThat(e.getMessage(), containsString("model is missing required setting [task_type]"));

        e = expectThrows(ElasticsearchStatusException.class, () -> ServiceUtils.resolveTaskType(TaskType.ANY, TaskType.ANY.toString()));
        assertThat(e.getMessage(), containsString("task_type [any] is not valid type for inference"));

        e = expectThrows(
            ElasticsearchStatusException.class,
            () -> ServiceUtils.resolveTaskType(TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING.toString())
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Cannot resolve conflicting task_type parameter in the request URL [sparse_embedding] and the request body [text_embedding]"
            )
        );
    }

    public void testEmbeddingTaskType_withUnsupportedNodeFeature_returnsStatusException() throws Exception {
        var featureServiceMock = mock(FeatureService.class);
        var clusterServiceMock = mock(ClusterService.class);
        when(featureServiceMock.clusterHasFeature(any(), eq(EMBEDDING_TASK_TYPE))).thenReturn(false);
        when(clusterServiceMock.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(InferencePlugin.SKIP_VALIDATE_AND_START))
        );
        var action = new TransportPutInferenceModelAction(
            mock(),
            clusterServiceMock,
            mock(),
            mock(),
            mock(),
            mock(),
            mock(),
            Settings.EMPTY,
            mock(),
            featureServiceMock
        );

        var taskMock = mock(Task.class);
        var request = new PutInferenceModelAction.Request(
            TaskType.EMBEDDING,
            randomIdentifier(),
            new BytesArray(""),
            XContentType.JSON,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );
        var state = mock(ClusterState.class);
        var listener = new PlainActionFuture<PutInferenceModelAction.Response>();

        action.masterOperation(taskMock, request, state, listener);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(
                "task_type ["
                    + TaskType.EMBEDDING
                    + "] is not supported by all nodes in the cluster; "
                    + "please complete upgrades before creating an endpoint with this task_type"
            )
        );
    }
}
