/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class InferenceWaitForAllocationTests extends ESTestCase {

    private static final String DEPLOYMENT_ID = "deployment-1";

    private record CapturedWaiter(Predicate<ClusterState> predicate, TrainedModelAssignmentService.WaitForAssignmentListener listener) {}

    private final List<CapturedWaiter> waiters = new ArrayList<>();
    private final AtomicInteger inferredCount = new AtomicInteger();

    private InferenceWaitForAllocation waitForAllocation;

    @Before
    public void initWaitForAllocation() throws Exception {
        // The real TrainedModelAssignmentService resolves the condition through a ClusterStateObserver on a
        // started ClusterService, which a unit test does not have; the mock records each waiter so the test can
        // drive the predicate and listener directly, mirroring waitForAssignmentCondition's behaviour.
        TrainedModelAssignmentService assignmentService = mock(TrainedModelAssignmentService.class);
        doAnswer(invocation -> {
            waiters.add(new CapturedWaiter(invocation.getArgument(1), invocation.getArgument(3)));
            return null;
        }).when(assignmentService).waitForAssignmentCondition(any(), any(), any(), any());

        waitForAllocation = new InferenceWaitForAllocation(assignmentService, (request, assignment) -> inferredCount.incrementAndGet());
    }

    /**
     * Regression test: when the deployment disappears while a request is queued, the waiter completes through the
     * predicate-exception path. That path must decrement the pending-request counter exactly once — not twice — so
     * the {@link InferenceWaitForAllocation#MAX_PENDING_REQUEST_COUNT} back-pressure does not drift negative.
     */
    public void testExceptionWhileWaitingDecrementsPendingCountOnce() {
        AtomicReference<Exception> failure = new AtomicReference<>();
        waitForAllocation.waitForAssignment(waitingRequest(failure));
        assertThat(waitForAllocation.pendingRequestCount(), is(1));
        assertThat(waiters, hasSize(1));

        // No assignment for the deployment id: the predicate records the "assignment has been removed" exception and
        // signals it is done, then the listener is invoked with a null assignment.
        CapturedWaiter waiter = waiters.get(0);
        assertThat(waiter.predicate().test(emptyClusterState()), is(true));
        waiter.listener().onResponse(null);

        assertThat("counter must return to zero, not drift negative", waitForAllocation.pendingRequestCount(), is(0));
        assertThat(inferredCount.get(), is(0));
        assertThat(failure.get(), notNullValue());
    }

    /**
     * The happy path must still decrement the counter exactly once and forward the request to the consumer.
     */
    public void testSuccessfulAllocationDecrementsPendingCountOnce() {
        AtomicReference<Exception> failure = new AtomicReference<>();
        waitForAllocation.waitForAssignment(waitingRequest(failure));
        assertThat(waitForAllocation.pendingRequestCount(), is(1));

        TrainedModelAssignment.Builder assignment = assignmentWithStartedRoute();
        CapturedWaiter waiter = waiters.get(0);
        assertThat(waiter.predicate().test(stateWithAssignment(assignment)), is(true));
        waiter.listener().onResponse(assignment.build());

        assertThat(waitForAllocation.pendingRequestCount(), is(0));
        assertThat(inferredCount.get(), is(1));
        assertThat(failure.get(), nullValue());
    }

    /**
     * A timeout completes through onFailure only and must also decrement the counter exactly once.
     */
    public void testTimeoutDecrementsPendingCountOnce() {
        AtomicReference<Exception> failure = new AtomicReference<>();
        waitForAllocation.waitForAssignment(waitingRequest(failure));
        assertThat(waitForAllocation.pendingRequestCount(), is(1));

        waiters.get(0).listener().onTimeout(TimeValue.timeValueSeconds(1));

        assertThat(waitForAllocation.pendingRequestCount(), is(0));
        assertThat(inferredCount.get(), is(0));
        assertThat(failure.get(), notNullValue());
    }

    /**
     * The cap admits up to the limit, rejects the next with a 429, and frees exactly one slot per completed waiter.
     * Draining through the predicate-exception path (the double-decrement path) must recover a single slot.
     */
    public void testBackPressureRejectsBeyondMaxPendingAndRecovers() {
        // The increment that reaches MAX is rejected, so MAX_PENDING_REQUEST_COUNT - 1 requests are admitted.
        int admitted = InferenceWaitForAllocation.MAX_PENDING_REQUEST_COUNT - 1;
        for (int i = 0; i < admitted; i++) {
            waitForAllocation.waitForAssignment(waitingRequest(new AtomicReference<>()));
        }
        assertThat(waitForAllocation.pendingRequestCount(), is(admitted));
        assertThat(waiters, hasSize(admitted));

        AtomicReference<Exception> rejected = new AtomicReference<>();
        waitForAllocation.waitForAssignment(waitingRequest(rejected));
        assertThat(rejected.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) rejected.get()).status(), is(RestStatus.TOO_MANY_REQUESTS));
        assertThat(waitForAllocation.pendingRequestCount(), is(admitted));
        assertThat(waiters, hasSize(admitted));

        CapturedWaiter waiter = waiters.get(0);
        assertThat(waiter.predicate().test(emptyClusterState()), is(true));
        waiter.listener().onResponse(null);
        assertThat(waitForAllocation.pendingRequestCount(), is(admitted - 1));

        waitForAllocation.waitForAssignment(waitingRequest(new AtomicReference<>()));
        assertThat(waitForAllocation.pendingRequestCount(), is(admitted));
    }

    private InferenceWaitForAllocation.WaitingRequest waitingRequest(AtomicReference<Exception> failure) {
        InferModelAction.Request request = InferModelAction.Request.forTextInput(
            DEPLOYMENT_ID,
            new EmptyConfigUpdate(),
            List.of("input"),
            true,
            TimeValue.timeValueSeconds(10)
        );
        return new InferenceWaitForAllocation.WaitingRequest(
            DEPLOYMENT_ID,
            request,
            InferModelAction.Response.builder(),
            TaskId.EMPTY_TASK_ID,
            ActionListener.wrap(response -> {}, failure::set)
        );
    }

    private static ClusterState emptyClusterState() {
        return ClusterState.builder(new ClusterName("test")).build();
    }

    private static TrainedModelAssignment.Builder assignmentWithStartedRoute() {
        return TrainedModelAssignment.Builder.empty(
            new StartTrainedModelDeploymentAction.TaskParams(DEPLOYMENT_ID, DEPLOYMENT_ID, 100, 1, 1, 100, null, Priority.NORMAL, 0, 0),
            null
        ).addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""));
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
}
