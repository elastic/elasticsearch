/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.admin.indices.resolve.ResolveClusterActionRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterActionResponse;
import org.elasticsearch.action.admin.indices.resolve.TransportResolveClusterAction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ResolveClusterTimeoutIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER_1 = "cluster-a";
    private static final String REMOTE_CLUSTER_2 = "cluster-b";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    public void testTimeoutParameterWithoutStallingRemotes() throws Exception {
        var maxTimeoutInMillis = 500L;
        var resolveClusterActionRequest = new ResolveClusterActionRequest(new String[] { "*:*" });

        // We set a timeout but won't stall any cluster; expectation is that we get back response just fine.
        resolveClusterActionRequest.setTimeout(TimeValue.timeValueMillis(randomLongBetween(100, maxTimeoutInMillis)));

        final AtomicReference<ResolveClusterActionResponse> resolveClusterActionResponse = new AtomicReference<>();
        assertBusy(() -> {
            var futureAction = client().execute(TransportResolveClusterAction.TYPE, resolveClusterActionRequest);
            resolveClusterActionResponse.set(futureAction.get());
        }, maxTimeoutInMillis, TimeUnit.MILLISECONDS);

        var clusterInfo = resolveClusterActionResponse.get().getResolveClusterInfo();

        // Both remotes are connected and error message is null.
        assertThat(clusterInfo.get(REMOTE_CLUSTER_1).isConnected(), equalTo(true));
        assertThat(clusterInfo.get(REMOTE_CLUSTER_1).getError(), is(nullValue()));
        assertThat(clusterInfo.get(REMOTE_CLUSTER_2).isConnected(), equalTo(true));
        assertThat(clusterInfo.get(REMOTE_CLUSTER_2).getError(), is(nullValue()));
    }

    public void testTimeoutParameterWithStallingARemote() throws Exception {
        var maxSleepInMillis = 500L;
        CountDownLatch latch = new CountDownLatch(1);

        // Add an override so that the remote cluster receives the TransportResolveClusterAction request but stalls.
        for (var nodes : cluster(REMOTE_CLUSTER_1).getNodeNames()) {
            ((MockTransportService) cluster(REMOTE_CLUSTER_1).getInstance(TransportService.class, nodes)).addRequestHandlingBehavior(
                TransportResolveClusterAction.REMOTE_TYPE.name(),
                (requestHandler, transportRequest, transportChannel, transportTask) -> {
                    // Wait until the TransportResolveRequestAction times out following which the latch is released.
                    latch.await();
                    requestHandler.messageReceived(transportRequest, transportChannel, transportTask);
                }
            );
        }

        var resolveClusterActionRequest = new ResolveClusterActionRequest(new String[] { "*:*" });
        var randomlyChosenTimeout = randomLongBetween(100, maxSleepInMillis);
        resolveClusterActionRequest.setTimeout(TimeValue.timeValueMillis(randomlyChosenTimeout));

        final AtomicReference<ResolveClusterActionResponse> resolveClusterActionResponse = new AtomicReference<>();
        try {
            assertBusy(() -> {
                var futureAction = client().execute(TransportResolveClusterAction.TYPE, resolveClusterActionRequest);
                resolveClusterActionResponse.set(futureAction.get());
            }, maxSleepInMillis, TimeUnit.MILLISECONDS);
        } finally {
            latch.countDown();
        }

        var clusterInfo = resolveClusterActionResponse.get().getResolveClusterInfo();

        // Ensure that the request timed out and that the remote is marked as not connected.
        assertThat(clusterInfo.get(REMOTE_CLUSTER_1).isConnected(), equalTo(false));
        assertThat(clusterInfo.get(REMOTE_CLUSTER_1).getError(), equalTo("Timed out: did not receive a response from the cluster"));

        // We never stalled the 2nd remote cluster. It should respond fine and be marked as connected with no error message set.
        assertThat(clusterInfo.get(REMOTE_CLUSTER_2).isConnected(), equalTo(true));
        assertThat(clusterInfo.get(REMOTE_CLUSTER_2).getError(), is(nullValue()));
    }
}
