/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TransportPutSampleConfigurationAction.
 * Tests the logic in isolation using mocks for dependencies.
 */
public class TransportPutSampleConfigurationActionTests extends ESTestCase {

    private ProjectResolver projectResolver;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private SamplingService samplingService;
    private TransportPutSampleConfigurationAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        TransportService transportService = mock(TransportService.class);
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        projectResolver = mock(ProjectResolver.class);
        when(projectResolver.getProjectMetadata((ClusterState) any())).thenReturn(
            ClusterState.EMPTY_STATE.projectState(ProjectId.DEFAULT).metadata()
        );
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        samplingService = mock(SamplingService.class);

        // Mock thread context
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        action = new TransportPutSampleConfigurationAction(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            projectResolver,
            indexNameExpressionResolver,
            samplingService
        );
    }

    /**
     * Tests successful masterOperation execution and verifies sampling metadata updates.
     */
    public void testMasterOperationSuccess() throws Exception {
        // Setup test data
        ProjectId projectId = randomProjectIdOrDefault();
        String indexName = randomIdentifier();
        SamplingConfiguration config = createRandomSamplingConfiguration();
        TimeValue masterNodeTimeout = randomTimeValue(100, 200);
        TimeValue ackTimeout = randomTimeValue(100, 200);

        PutSampleConfigurationAction.Request request = new PutSampleConfigurationAction.Request(config, masterNodeTimeout, ackTimeout);
        request.indices(indexName);

        // Create initial cluster state with project metadata
        ProjectMetadata initialProjectMetadata = ProjectMetadata.builder(projectId).build();
        ClusterState initialClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(initialProjectMetadata)
            .build();

        Task task = mock(Task.class);

        // Setup mocks
        when(projectResolver.getProjectId()).thenReturn(projectId);

        // Create updated cluster state with sampling metadata
        Map<String, SamplingConfiguration> indexToSampleConfigMap = new HashMap<>();
        indexToSampleConfigMap.put(indexName, config);
        SamplingMetadata samplingMetadata = new SamplingMetadata(indexToSampleConfigMap);

        ProjectMetadata updatedProjectMetadata = ProjectMetadata.builder(projectId)
            .putCustom(SamplingMetadata.TYPE, samplingMetadata)
            .build();
        ClusterState updatedClusterState = ClusterState.builder(initialClusterState).putProjectMetadata(updatedProjectMetadata).build();

        // Mock samplingService to simulate the actual cluster state update
        AtomicReference<ClusterState> capturedClusterState = new AtomicReference<>();
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(5);

            // Simulate the sampling service updating cluster state and calling listener
            capturedClusterState.set(updatedClusterState);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(samplingService)
            .updateSampleConfiguration(
                eq(projectId),
                eq(indexName),
                eq(config),
                eq(request.masterNodeTimeout()),
                eq(request.ackTimeout()),
                any()
            );

        // Test execution with CountDownLatch to handle async operation
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        ActionListener<AcknowledgedResponse> testListener = new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                responseRef.set(response);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        };

        action.masterOperation(task, request, initialClusterState, testListener);

        // Wait for async operation to complete
        assertTrue("Operation should complete within timeout", latch.await(5, TimeUnit.SECONDS));

        // Verify results
        assertThat(responseRef.get(), not(nullValue()));
        assertThat(responseRef.get().isAcknowledged(), equalTo(true));
        assertThat(exceptionRef.get(), nullValue());

        // Verify interactions
        verify(projectResolver).getProjectId();
        verify(samplingService).updateSampleConfiguration(
            eq(projectId),
            eq(indexName),
            eq(config),
            eq(request.masterNodeTimeout()),
            eq(request.ackTimeout()),
            any()
        );

        // Verify cluster state change
        ClusterState resultClusterState = capturedClusterState.get();
        assertThat(resultClusterState, not(nullValue()));

        ProjectMetadata resultProjectMetadata = resultClusterState.getMetadata().getProject(projectId);
        assertThat(resultProjectMetadata, not(nullValue()));

        SamplingMetadata resultSamplingMetadata = resultProjectMetadata.custom(SamplingMetadata.TYPE);
        assertThat(resultSamplingMetadata, not(nullValue()));
        assertThat(resultSamplingMetadata.getIndexToSamplingConfigMap(), hasKey(indexName));
        assertThat(resultSamplingMetadata.getIndexToSamplingConfigMap().get(indexName), equalTo(config));
    }

    /**
     * Tests action name and configuration.
     */
    public void testActionConfiguration() {
        // Verify action is properly configured
        assertThat(action.actionName, equalTo(PutSampleConfigurationAction.NAME));
    }

    private SamplingConfiguration createRandomSamplingConfiguration() {
        return new SamplingConfiguration(
            randomDoubleBetween(0.0, 1.0, true),
            randomBoolean() ? null : randomIntBetween(1, SamplingConfiguration.MAX_SAMPLES_LIMIT),
            randomBoolean() ? null : ByteSizeValue.ofMb(randomLongBetween(1, 100)),
            randomBoolean() ? null : TimeValue.timeValueDays(randomLongBetween(1, SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS)),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }
}
