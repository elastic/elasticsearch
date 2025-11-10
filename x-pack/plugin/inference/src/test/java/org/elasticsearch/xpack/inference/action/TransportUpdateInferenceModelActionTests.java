/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportUpdateInferenceModelActionTests extends ESTestCase {

    private MockLicenseState licenseState;
    private TransportUpdateInferenceModelAction action;
    private ThreadPool threadPool;
    private ModelRegistry mockModelRegistry;
    private InferenceServiceRegistry mockInferenceServiceRegistry;

    @Before
    public void createAction() throws Exception {
        super.setUp();
        threadPool = mock(ThreadPool.class);
        mockModelRegistry = mock(ModelRegistry.class);
        mockInferenceServiceRegistry = mock(InferenceServiceRegistry.class);
        licenseState = MockLicenseState.createMock();
        action = new TransportUpdateInferenceModelAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            threadPool,
            mock(ActionFilters.class),
            licenseState,
            mockModelRegistry,
            mockInferenceServiceRegistry,
            mock(Client.class),
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );

    }

    public void testLicenseCheck_NotAllowed() {
        mocks("enterprise_licensed_service", false);

        var listener = new PlainActionFuture<UpdateInferenceModelAction.Response>();

        String requestBody = "{\"service_settings\": {\"api_key\": \"<API_KEY>\"}}";

        action.masterOperation(
            mock(Task.class),
            new UpdateInferenceModelAction.Request(
                "model-id",
                new BytesArray(requestBody),
                XContentType.JSON,
                TaskType.TEXT_EMBEDDING,
                TimeValue.timeValueSeconds(1)
            ),
            ClusterState.EMPTY_STATE,
            listener
        );

        var exception = expectThrows(ElasticsearchSecurityException.class, () -> listener.actionGet(TimeValue.timeValueSeconds(5)));
        assertThat(exception.getMessage(), is("current license is non-compliant for [inference]"));
    }

    private void mocks(String serviceName, boolean isAllowed) {
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onResponse(new UnparsedModel("model_id", TaskType.COMPLETION, serviceName, Map.of(), Map.of()));
            return Void.TYPE;
        }).when(mockModelRegistry).getModelWithSecrets(anyString(), any());

        var mockService = mock(InferenceService.class);
        when(mockService.name()).thenReturn(serviceName);
        when(mockInferenceServiceRegistry.getService(anyString())).thenReturn(Optional.of(mockService));

        when(licenseState.isAllowed(any())).thenReturn(isAllowed);
    }
}
