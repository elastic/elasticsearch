/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;

import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymsSet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SynonymsManagementAPIServiceTests extends ESTestCase {

    /**
     * Verifies that {@code bulkUpdateSynonymsSet} issues multiple sequential bulk requests when
     * the number of rules exceeds the chunk size, and that each document is sent exactly once.
     */
    public void testBulkUpdateChunksLargeSet() throws Exception {
        int chunkSize = 3;
        int numRules = 7; // expects ceil(7/3) = 3 chunks
        SynonymRule[] rules = randomSynonymsSet(numRules);

        try (var threadPool = createThreadPool()) {
            var countingClient = new BulkCountingClient(new NoOpNodeClient(threadPool));
            var service = new SynonymsManagementAPIService(
                countingClient,
                null,
                numRules,
                SynonymsManagementAPIService.PIT_BATCH_SIZE,
                chunkSize
            );

            var future = new PlainActionFuture<Void>();
            service.bulkUpdateSynonymsSet("my-set", rules, future);
            future.actionGet();

            assertThat(countingClient.bulkRequestCount.get(), equalTo(3));
            // +1 for the synonym set document written in the first chunk
            assertThat(countingClient.totalDocCount.get(), equalTo(numRules + 1));
        }
    }

    /**
     * Verifies that {@code bulkUpdateSynonymsSet} issues a single bulk request when the number
     * of rules fits within one chunk.
     */
    public void testBulkUpdateSingleChunkSmallSet() throws Exception {
        int chunkSize = 10;
        int numRules = 5;
        SynonymRule[] rules = randomSynonymsSet(numRules);

        try (var threadPool = createThreadPool()) {
            var countingClient = new BulkCountingClient(new NoOpNodeClient(threadPool));
            var service = new SynonymsManagementAPIService(
                countingClient,
                null,
                numRules,
                SynonymsManagementAPIService.PIT_BATCH_SIZE,
                chunkSize
            );

            var future = new PlainActionFuture<Void>();
            service.bulkUpdateSynonymsSet("my-set", rules, future);
            future.actionGet();

            assertThat(countingClient.bulkRequestCount.get(), equalTo(1));
            assertThat(countingClient.totalDocCount.get(), equalTo(numRules + 1));
        }
    }

    /**
     * Verifies that an empty synonym set results in exactly one bulk request containing
     * only the synonym set document.
     */
    public void testBulkUpdateEmptySet() throws Exception {
        try (var threadPool = createThreadPool()) {
            var countingClient = new BulkCountingClient(new NoOpNodeClient(threadPool));
            var service = new SynonymsManagementAPIService(
                countingClient,
                null,
                SynonymsManagementAPIService.PRE_LARGE_SETS_LIMIT,
                SynonymsManagementAPIService.PIT_BATCH_SIZE,
                SynonymsManagementAPIService.BULK_CHUNK_SIZE
            );

            var future = new PlainActionFuture<Void>();
            service.bulkUpdateSynonymsSet("my-set", new SynonymRule[0], future);
            future.actionGet();

            assertThat(countingClient.bulkRequestCount.get(), equalTo(1));
            assertThat(countingClient.totalDocCount.get(), equalTo(1)); // just the set doc
        }
    }

    /**
     * Verifies that {@code putSynonymsSet} rejects writes of more than {@code PRE_LARGE_SETS_LIMIT}
     * rules when the cluster's minimum transport version predates large synonym set support, and
     * that no bulk requests are issued.
     */
    public void testTransportVersionGateRejectsLargeSetsOnOldCluster() {
        ClusterState oldClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putCompatibilityVersions("node1", TransportVersion.minimumCompatible(), SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .build();

        try (var threadPool = createThreadPool()) {
            ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            ClusterService clusterService = ClusterServiceUtils.createClusterService(oldClusterState, threadPool, clusterSettings);
            try {
                var countingClient = new BulkCountingClient(new NoOpNodeClient(threadPool));
                var service = new SynonymsManagementAPIService(
                    countingClient,
                    clusterService,
                    SynonymsManagementAPIService.PRE_LARGE_SETS_LIMIT + 10,
                    SynonymsManagementAPIService.PIT_BATCH_SIZE,
                    SynonymsManagementAPIService.BULK_CHUNK_SIZE
                );

                SynonymRule[] rules = randomSynonymsSet(SynonymsManagementAPIService.PRE_LARGE_SETS_LIMIT + 1);
                boolean[] failed = new boolean[] { false };
                Exception[] exceptionHolder = new Exception[1];

                service.putSynonymsSet("my-set", rules, false, ActionListener.wrap(r -> fail("expected failure"), e -> {
                    failed[0] = true;
                    exceptionHolder[0] = e;
                }));

                assertTrue("expected listener.onFailure to be called synchronously", failed[0]);
                assertThat(exceptionHolder[0], instanceOf(IllegalStateException.class));
                assertThat(exceptionHolder[0].getMessage(), containsString("all nodes in the cluster have been upgraded"));
                assertThat(countingClient.bulkRequestCount.get(), equalTo(0));
            } finally {
                clusterService.close();
            }
        }
    }

    /**
     * A {@link FilterClient} that counts bulk requests and returns successful empty responses,
     * without requiring a live Elasticsearch cluster. Calls to other action types are forwarded
     * to the underlying client unchanged.
     */
    private static class BulkCountingClient extends FilterClient {
        final AtomicInteger bulkRequestCount = new AtomicInteger();
        final AtomicInteger totalDocCount = new AtomicInteger();

        BulkCountingClient(NoOpNodeClient in) {
            super(in);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof BulkRequest bulkRequest) {
                bulkRequestCount.incrementAndGet();
                totalDocCount.addAndGet(bulkRequest.numberOfActions());
                ((ActionListener<BulkResponse>) listener).onResponse(new BulkResponse(new BulkItemResponse[0], 0L));
                return;
            }
            super.doExecute(action, request, listener);
        }
    }
}
