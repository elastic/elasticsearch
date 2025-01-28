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
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterInfo;
import org.elasticsearch.action.admin.indices.resolve.TransportResolveClusterAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ResolveClusterTimeoutIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER_1 = "cluster-a";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1);
    }

    public void testTimeoutParameter() {
        long maxTimeoutInMillis = 500;

        // First part: we query _resolve/cluster without any indices to prove we can hit remote1 fine.
        ResolveClusterActionRequest requestWithoutIndices;
        if (randomBoolean()) {
            requestWithoutIndices = new ResolveClusterActionRequest(new String[0], IndicesOptions.DEFAULT, true, true);
        } else {
            requestWithoutIndices = new ResolveClusterActionRequest(new String[] { "*:*" });
        }

        // We set a timeout but won't stall any cluster; expectation is that we get back response just fine.
        requestWithoutIndices.setTimeout(TimeValue.timeValueMillis(randomLongBetween(100, maxTimeoutInMillis)));
        ResolveClusterActionResponse responseWithoutIndices = safeGet(
            client().execute(TransportResolveClusterAction.TYPE, requestWithoutIndices)
        );
        Map<String, ResolveClusterInfo> clusterInfoWithoutIndices = responseWithoutIndices.getResolveClusterInfo();

        // Remote is connected and error message is null.
        assertThat(clusterInfoWithoutIndices.get(REMOTE_CLUSTER_1).isConnected(), equalTo(true));
        assertThat(clusterInfoWithoutIndices.get(REMOTE_CLUSTER_1).getError(), is(nullValue()));

        // Second part: now we stall the remote and utilise the timeout feature.
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

        ResolveClusterActionRequest requestWithIndices = new ResolveClusterActionRequest(new String[] { "*:*" });
        long randomlyChosenTimeout = randomLongBetween(100, maxTimeoutInMillis);
        requestWithIndices.setTimeout(TimeValue.timeValueMillis(randomlyChosenTimeout));

        ResolveClusterActionResponse responseWithIndices = safeGet(
            client().execute(TransportResolveClusterAction.TYPE, requestWithIndices)
        );
        latch.countDown();

        Map<String, ResolveClusterInfo> clusterInfoWithIndices = responseWithIndices.getResolveClusterInfo();

        // Ensure that the request timed out and that the remote is marked as not connected.
        assertThat(clusterInfoWithIndices.get(REMOTE_CLUSTER_1).isConnected(), equalTo(false));
        assertThat(
            clusterInfoWithIndices.get(REMOTE_CLUSTER_1).getError(),
            equalTo("Request timed out before receiving a response from the remote cluster")
        );
    }
}
