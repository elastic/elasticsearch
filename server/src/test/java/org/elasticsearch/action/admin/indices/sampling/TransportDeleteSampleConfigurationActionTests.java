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
import org.elasticsearch.index.IndexNotFoundException;
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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TransportDeleteSampleConfigurationAction}.
 * <p>
 * Tests the transport action logic in isolation using mocks for dependencies,
 * covering successful deletion, error handling, and proper interaction with the sampling service.
 * </p>
 */
public class TransportDeleteSampleConfigurationActionTests extends ESTestCase {

    private ProjectResolver projectResolver;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private SamplingService samplingService;
    private TransportDeleteSampleConfigurationAction action;

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

        action = new TransportDeleteSampleConfigurationAction(
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
     * Tests successful masterOperation execution and verifies sampling configuration deletion.
     */
    public void testMasterOperationSuccess() throws Exception {
        // Setup test data
        ProjectId projectId = randomProjectIdOrDefault();
        String indexName = randomIdentifier();
        TimeValue masterNodeTimeout = randomTimeValue(100, 200);
        TimeValue ackTimeout = randomTimeValue(100, 200);

        DeleteSampleConfigurationAction.Request request = new DeleteSampleConfigurationAction.Request(masterNodeTimeout, ackTimeout);
        request.indices(indexName);

        // Create initial cluster state with existing sampling configuration
        SamplingConfiguration existingConfig = createRandomSamplingConfiguration();
        Map<String, SamplingConfiguration> indexToSampleConfigMap = new HashMap<>();
        indexToSampleConfigMap.put(indexName, existingConfig);
        SamplingMetadata samplingMetadata = new SamplingMetadata(indexToSampleConfigMap);

        ProjectMetadata initialProjectMetadata = ProjectMetadata.builder(projectId)
            .putCustom(SamplingMetadata.TYPE, samplingMetadata)
            .build();
        ClusterState initialClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(initialProjectMetadata)
            .build();

        Task task = mock(Task.class);

        // Setup mocks
        when(projectResolver.getProjectId()).thenReturn(projectId);
        when(indexNameExpressionResolver.concreteIndexNames(initialClusterState, request)).thenReturn(new String[] { indexName });

        // Create updated cluster state with configuration removed
        Map<String, SamplingConfiguration> updatedConfigMap = new HashMap<>();
        SamplingMetadata updatedSamplingMetadata = new SamplingMetadata(updatedConfigMap);

        ProjectMetadata updatedProjectMetadata = ProjectMetadata.builder(projectId)
            .putCustom(SamplingMetadata.TYPE, updatedSamplingMetadata)
            .build();
        ClusterState updatedClusterState = ClusterState.builder(initialClusterState).putProjectMetadata(updatedProjectMetadata).build();

        // Mock samplingService to simulate the actual cluster state update
        AtomicReference<ClusterState> capturedClusterState = new AtomicReference<>();
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(4);

            // Simulate the sampling service updating cluster state and calling listener
            capturedClusterState.set(updatedClusterState);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(samplingService)
            .deleteSampleConfiguration(eq(projectId), eq(indexName), eq(request.masterNodeTimeout()), eq(request.ackTimeout()), any());

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
        verify(indexNameExpressionResolver).concreteIndexNames(initialClusterState, request);
        verify(samplingService).deleteSampleConfiguration(
            eq(projectId),
            eq(indexName),
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
        assertThat(resultSamplingMetadata.getIndexToSamplingConfigMap().isEmpty(), equalTo(true));
    }

    /**
     * Tests masterOperation when index name resolution fails.
     * Should propagate IndexNotFoundException through the listener.
     */
    public void testMasterOperationIndexNotFound() throws Exception {
        // Setup test data
        ProjectId projectId = randomProjectIdOrDefault();
        String indexName = randomIdentifier();
        TimeValue masterNodeTimeout = randomTimeValue(100, 200);
        TimeValue ackTimeout = randomTimeValue(100, 200);

        DeleteSampleConfigurationAction.Request request = new DeleteSampleConfigurationAction.Request(masterNodeTimeout, ackTimeout);
        request.indices(indexName);

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).build();
        Task task = mock(Task.class);

        // Setup mocks
        IndexNotFoundException indexNotFoundException = new IndexNotFoundException(indexName);
        when(indexNameExpressionResolver.concreteIndexNames(clusterState, request)).thenThrow(indexNotFoundException);

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

        action.masterOperation(task, request, clusterState, testListener);

        // Wait for async operation to complete
        assertTrue("Operation should complete within timeout", latch.await(5, TimeUnit.SECONDS));

        // Verify results
        assertThat(responseRef.get(), nullValue());
        assertThat(exceptionRef.get(), not(nullValue()));
        assertThat(exceptionRef.get(), sameInstance(indexNotFoundException));

        // Verify interactions
        verify(indexNameExpressionResolver).concreteIndexNames(clusterState, request);
    }

    /**
     * Tests masterOperation when sampling service deletion fails.
     * Should propagate the exception through the listener.
     */
    public void testMasterOperationSamplingServiceFailure() throws Exception {
        // Setup test data
        ProjectId projectId = randomProjectIdOrDefault();
        String indexName = randomIdentifier();
        TimeValue masterNodeTimeout = randomTimeValue(100, 200);
        TimeValue ackTimeout = randomTimeValue(100, 200);

        DeleteSampleConfigurationAction.Request request = new DeleteSampleConfigurationAction.Request(masterNodeTimeout, ackTimeout);
        request.indices(indexName);

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).build();
        Task task = mock(Task.class);

        // Setup mocks
        when(projectResolver.getProjectId()).thenReturn(projectId);
        when(indexNameExpressionResolver.concreteIndexNames(clusterState, request)).thenReturn(new String[] { indexName });

        RuntimeException serviceException = new RuntimeException("Sampling service error");
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(4);
            listener.onFailure(serviceException);
            return null;
        }).when(samplingService)
            .deleteSampleConfiguration(eq(projectId), eq(indexName), eq(request.masterNodeTimeout()), eq(request.ackTimeout()), any());

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

        action.masterOperation(task, request, clusterState, testListener);

        // Wait for async operation to complete
        assertTrue("Operation should complete within timeout", latch.await(5, TimeUnit.SECONDS));

        // Verify results
        assertThat(responseRef.get(), nullValue());
        assertThat(exceptionRef.get(), not(nullValue()));
        assertThat(exceptionRef.get(), sameInstance(serviceException));

        // Verify interactions
        verify(projectResolver).getProjectId();
        verify(indexNameExpressionResolver).concreteIndexNames(clusterState, request);
        verify(samplingService).deleteSampleConfiguration(
            eq(projectId),
            eq(indexName),
            eq(request.masterNodeTimeout()),
            eq(request.ackTimeout()),
            any()
        );
    }

    /**
     * Tests action name and configuration.
     */
    public void testActionConfiguration() {
        // Verify action is properly configured
        assertThat(action.actionName, equalTo(DeleteSampleConfigurationAction.NAME));
    }

    /**
     * Tests that multiple indices in request causes IllegalArgumentException.
     * The action should only accept single index operations.
     */
    public void testMasterOperationMultipleIndices() {
        // Setup test data
        String indexName1 = randomIdentifier();
        String indexName2 = randomIdentifier();
        TimeValue masterNodeTimeout = randomTimeValue(100, 200);
        TimeValue ackTimeout = randomTimeValue(100, 200);

        DeleteSampleConfigurationAction.Request request = new DeleteSampleConfigurationAction.Request(masterNodeTimeout, ackTimeout);

        // Verify that setting multiple indices throws IllegalArgumentException
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> request.indices(indexName1, indexName2));
        assertThat(exception.getMessage(), equalTo("[indices] must contain only one index"));
    }

    /**
     * Creates a random sampling configuration for testing purposes.
     */
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
