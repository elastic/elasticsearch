/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterStateObserverTests extends ESTestCase {

    public void testClusterStateListenerToStringIncludesListenerToString() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final AtomicBoolean listenerAdded = new AtomicBoolean();

        doAnswer(invocation -> {
            assertThat(Arrays.toString(invocation.getArguments()), containsString("test-listener"));
            listenerAdded.set(true);
            return null;
        }).when(clusterApplierService).addTimeoutListener(any(), any());

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );
        clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {}

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}

            @Override
            public String toString() {
                return "test-listener";
            }
        });

        assertTrue(listenerAdded.get());
    }

}
