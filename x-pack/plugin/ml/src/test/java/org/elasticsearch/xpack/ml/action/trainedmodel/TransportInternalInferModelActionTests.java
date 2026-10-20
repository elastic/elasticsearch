/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action.trainedmodel;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.adaptiveallocations.AdaptiveAllocationsScalerService;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentService;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportInternalInferModelActionTests extends ESTestCase {

    /** The id on the client request: the model id, not the deployment id (issue #146835). */
    private static final String MODEL_ID = ".elser_model_2_linux-x86_64";
    private static final String DEPLOYMENT_ID = ".elser-2-elasticsearch";

    private record Waiter(
        String deploymentId,
        Predicate<ClusterState> predicate,
        TrainedModelAssignmentService.WaitForAssignmentListener listener
    ) {}

    private final List<Waiter> waiters = new ArrayList<>();

    private ClusterService clusterService;
    private AdaptiveAllocationsScalerService adaptiveAllocationsScalerService;
    private Client client;
    private TransportInternalInferModelAction action;

    @Before
    public void setUpAction() {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create("node-1"));

        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(MachineLearning.INFERENCE_AGG_FEATURE)).thenReturn(true);

        adaptiveAllocationsScalerService = mock(AdaptiveAllocationsScalerService.class);

        // The real service resolves waiters through a ClusterStateObserver on a started ClusterService,
        // which a unit test does not have. The mock records each waiter so tests can deliver cluster
        // states via deliverClusterState(), reproducing waitForAssignmentCondition's behaviour.
        TrainedModelAssignmentService assignmentService = mock(TrainedModelAssignmentService.class);
        doAnswer(invocation -> {
            waiters.add(new Waiter(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(3)));
            return null;
        }).when(assignmentService).waitForAssignmentCondition(any(), any(), any(), any());

        action = new TransportInternalInferModelAction(
            transportService,
            mock(ActionFilters.class),
            mock(ModelLoadingService.class),
            client,
            clusterService,
            licenseState,
            mock(TrainedModelProvider.class),
            adaptiveAllocationsScalerService,
            assignmentService,
            threadPool
        );
    }

    /**
     * Scale-from-zero regression test: the client addresses the deployment by model id, the
     * deployment exists in cluster state (with no allocations yet) under a different deployment
     * id. The queued waiter must resolve cluster state by the deployment id and keep waiting,
     * not fail with a spurious 409 "model assignment has been removed".
     */
    public void testQueuedRequestKeepsWaitingWhenRequestIdIsModelId() {
        ClusterState scaledToZero = stateWithAssignment(assignmentScaledToZero());
        when(clusterService.state()).thenReturn(scaledToZero);
        when(adaptiveAllocationsScalerService.maybeStartAllocation(any())).thenReturn(true);

        AtomicReference<Exception> failure = new AtomicReference<>();
        action.doExecute(
            task(),
            inferRequest(),
            ActionListener.wrap(response -> fail("request should be queued waiting for an allocation"), failure::set)
        );
        assertThat(waiters, hasSize(1));

        deliverClusterState(scaledToZero);

        assertThat("queued request must not fail while the deployment scales up", failure.get(), nullValue());
        assertThat(waiters, hasSize(1));
    }

    /**
     * Once the deployment has a started allocation the queued request must be inferred against
     * the deployment id resolved from the assignment, not the client-supplied model id.
     */
    public void testQueuedRequestInfersOnDeploymentOnceScaledUp() {
        ClusterState scaledToZero = stateWithAssignment(assignmentScaledToZero());
        when(clusterService.state()).thenReturn(scaledToZero);
        when(adaptiveAllocationsScalerService.maybeStartAllocation(any())).thenReturn(true);

        AtomicReference<InferTrainedModelDeploymentAction.Request> deploymentRequest = new AtomicReference<>();
        doAnswer(invocation -> {
            deploymentRequest.set(invocation.getArgument(1));
            ActionListener<InferTrainedModelDeploymentAction.Response> listener = invocation.getArgument(2);
            listener.onResponse(new InferTrainedModelDeploymentAction.Response(List.of(new WarningInferenceResults("ok"))));
            return null;
        }).when(client).execute(eq(InferTrainedModelDeploymentAction.INSTANCE), any(), any());

        AtomicReference<InferModelAction.Response> response = new AtomicReference<>();
        action.doExecute(task(), inferRequest(), ActionTestUtils.assertNoFailureListener(response::set));
        assertThat(waiters, hasSize(1));

        ClusterState scaledUp = stateWithAssignment(
            assignmentScaledToZero().addRoutingEntry("ml-node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
        );
        deliverClusterState(scaledUp);

        assertThat(deploymentRequest.get(), notNullValue());
        assertThat(deploymentRequest.get().getId(), equalTo(DEPLOYMENT_ID));
        assertThat(response.get(), notNullValue());
        assertThat(response.get().getInferenceResults(), hasSize(1));
    }

    /**
     * Mimics {@link TrainedModelAssignmentService#waitForAssignmentCondition}: the cluster state
     * observer tests the predicate and, once it matches, resolves the assignment for the listener
     * by the id the waiter registered with.
     */
    private void deliverClusterState(ClusterState state) {
        var iterator = waiters.iterator();
        while (iterator.hasNext()) {
            Waiter waiter = iterator.next();
            if (waiter.predicate().test(state)) {
                iterator.remove();
                waiter.listener()
                    .onResponse(TrainedModelAssignmentMetadata.assignmentForDeploymentId(state, waiter.deploymentId()).orElse(null));
            }
        }
    }

    private static InferModelAction.Request inferRequest() {
        return InferModelAction.Request.forTextInput(
            MODEL_ID,
            new EmptyConfigUpdate(),
            List.of("one text"),
            true,
            TimeValue.timeValueSeconds(10)
        );
    }

    private static TrainedModelAssignment.Builder assignmentScaledToZero() {
        return TrainedModelAssignment.Builder.empty(
            new StartTrainedModelDeploymentAction.TaskParams(MODEL_ID, DEPLOYMENT_ID, 100, 0, 1, 100, null, Priority.NORMAL, 0, 0),
            null
        );
    }

    private static ClusterState stateWithAssignment(TrainedModelAssignment.Builder assignment) {
        return ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty().addNewAssignment(DEPLOYMENT_ID, assignment).build()
                    )
                    .build()
            )
            .build();
    }

    private static Task task() {
        return new CancellableTask(1L, "type", "action", "", TaskId.EMPTY_TASK_ID, Map.of());
    }
}
