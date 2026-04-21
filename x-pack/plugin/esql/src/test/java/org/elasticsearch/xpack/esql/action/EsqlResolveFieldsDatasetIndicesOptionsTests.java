/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests that ES|QL resolve-fields consistently enables {@link IndicesOptions.IndexAbstractionOptions#resolveDatasets()}
 * for planning and that the flag is coord-local on the wire (see {@link IndicesOptions} serialization).
 */
public class EsqlResolveFieldsDatasetIndicesOptionsTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getSimpleName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testIndexResolverDefaultFieldCapsOptionsEnableResolveDatasets() {
        assertThat(IndexResolver.DEFAULT_OPTIONS.indexAbstractionOptions().resolveDatasets(), is(true));
    }

    public void testTransportRoundTripClearsResolveDatasets() throws IOException {
        FieldCapabilitiesRequest original = new FieldCapabilitiesRequest();
        original.indices("idx");
        original.fields("f");
        original.indicesOptions(IndexResolver.DEFAULT_OPTIONS);
        assertThat(original.indicesOptions().indexAbstractionOptions().resolveDatasets(), is(true));

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());
        original.writeTo(out);

        var in = out.bytes().streamInput();
        in.setTransportVersion(TransportVersion.current());
        FieldCapabilitiesRequest deserialized = new FieldCapabilitiesRequest(in);
        assertThat(deserialized.indicesOptions().indexAbstractionOptions().resolveDatasets(), is(false));
    }

    public void testEsqlResolveFieldsActionPatchesResolveDatasetsBeforeDelegating() {
        TransportFieldCapabilitiesAction fieldCaps = mock(TransportFieldCapabilitiesAction.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<EsqlResolveFieldsResponse> listener = invocation.getArgument(3);
            listener.onResponse(new EsqlResolveFieldsResponse(FieldCapabilitiesResponse.empty()));
            return null;
        }).when(fieldCaps).executeRequest(any(), any(), any(), any());

        TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(mock(TaskManager.class));
        lenient().doNothing()
            .when(transportService)
            .registerRequestHandler(anyString(), any(Executor.class), anyBoolean(), anyBoolean(), any(), any());

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).build())
            .build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        ProjectResolver projectResolver = mock(ProjectResolver.class);
        when(projectResolver.getProjectState(clusterState)).thenReturn(clusterState.projectState(ProjectId.DEFAULT));

        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );

        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);

        EsqlResolveFieldsAction action = new EsqlResolveFieldsAction(
            transportService,
            actionFilters,
            fieldCaps,
            clusterService,
            indexNameExpressionResolver,
            projectResolver
        );

        IndicesOptions withoutDatasets = IndicesOptions.builder(IndicesOptions.DEFAULT)
            .indexAbstractionOptions(
                IndicesOptions.IndexAbstractionOptions.builder(IndicesOptions.DEFAULT.indexAbstractionOptions())
                    .resolveDatasets(false)
                    .build()
            )
            .build();

        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.indices("idx");
        request.fields("f");
        request.indicesOptions(withoutDatasets);
        assertThat(request.indicesOptions().indexAbstractionOptions().resolveDatasets(), is(false));

        CancellableTask task = new CancellableTask(1L, "test", EsqlResolveFieldsAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
        PlainActionFuture<EsqlResolveFieldsResponse> future = new PlainActionFuture<>();
        action.execute(task, request, future);
        future.actionGet();

        ArgumentCaptor<FieldCapabilitiesRequest> captor = ArgumentCaptor.forClass(FieldCapabilitiesRequest.class);
        verify(fieldCaps).executeRequest(any(), captor.capture(), any(), any());
        assertThat(captor.getValue().indicesOptions().indexAbstractionOptions().resolveDatasets(), is(true));
    }
}
