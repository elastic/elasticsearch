/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class TransportPutShutdownNodeActionTests extends ESTestCase {

    public void testNoop() {
        final var clusterService = ClusterServiceUtils.createSingleThreadedClusterService();
        final var appliedStateCount = new AtomicInteger();
        clusterService.addListener(event -> appliedStateCount.incrementAndGet());
        final var action = new TransportPutShutdownNodeAction(
            mock(TransportService.class),
            clusterService,
            clusterService.getClusterApplierService().threadPool(),
            new ActionFilters(Collections.emptySet()),
            TestIndexNameExpressionResolver.newInstance()
        );
        final var initialState = clusterService.state();
        final var type = randomFrom(Type.REMOVE, Type.REPLACE, Type.RESTART);
        final var allocationDelay = type == Type.RESTART ? TimeValue.timeValueMinutes(randomIntBetween(1, 3)) : null;
        final var targetNodeName = type == Type.REPLACE ? randomAlphaOfLength(5) : null;
        final var request = new PutShutdownNodeAction.Request("node1", type, "sunsetting", allocationDelay, targetNodeName);

        // run the request against the initial state - the master service should compute and apply the update
        action.masterOperation(null, request, initialState, ActionListener.noop());
        assertThat(appliedStateCount.get(), equalTo(1));
        var stableState = clusterService.state();
        assertNotSame(initialState, stableState);

        // run the request again with stale state to bypass the action's noop detection - the master service should compute an update based
        // on the last-applied state, determine it's a no-op, and skip publication
        action.masterOperation(null, request, initialState, ActionListener.noop());
        assertThat(appliedStateCount.get(), equalTo(1));
        assertSame(stableState, clusterService.state());

        // run the request again but with the latest state - the action should not even submit a task to the master
        clusterService.getMasterService().setClusterStateSupplier(() -> { throw new AssertionError("should not submit task"); });
        action.masterOperation(null, request, stableState, ActionListener.noop());
        assertThat(appliedStateCount.get(), equalTo(1));
        assertSame(stableState, clusterService.state());
    }
}
