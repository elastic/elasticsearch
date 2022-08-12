/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.NodesShutdownMetadata.TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class TransportDeleteShutdownNodeActionTests extends ESTestCase {

    public void testNoop() throws Exception {
        final var clusterService = ClusterServiceUtils.createSingleThreadedClusterService();
        final var appliedStateCount = new AtomicInteger();
        clusterService.addListener(event -> appliedStateCount.incrementAndGet());
        final var action = new TransportDeleteShutdownNodeAction(
            mock(TransportService.class),
            clusterService,
            clusterService.getClusterApplierService().threadPool(),
            new ActionFilters(Collections.emptySet()),
            TestIndexNameExpressionResolver.newInstance()
        );
        clusterService.getClusterApplierService()
            .onNewClusterState(
                "setup",
                () -> clusterService.state()
                    .copyAndUpdateMetadata(
                        mb -> mb.putCustom(TYPE, new NodesShutdownMetadata(Map.of("node1", mock(SingleNodeShutdownMetadata.class))))
                    ),
                ActionListener.noop()
            );
        assertThat(appliedStateCount.get(), equalTo(1));
        final var initialState = clusterService.state();
        final var request = new DeleteShutdownNodeAction.Request("node1");

        // run the action to remove the shutdown entry
        action.masterOperation(null, request, initialState, ActionListener.noop());
        assertThat(appliedStateCount.get(), equalTo(2));
        final var removedState = clusterService.state();

        // run the action again (with stale state) and observe that no further state is published
        action.masterOperation(null, request, initialState, ActionListener.noop());
        assertThat(appliedStateCount.get(), equalTo(2));
        assertSame(removedState, clusterService.state());
    }
}
