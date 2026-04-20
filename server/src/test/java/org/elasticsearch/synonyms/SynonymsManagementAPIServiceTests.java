/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.synonyms;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymsSet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SynonymsManagementAPIServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool();
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void tearDown() throws Exception {
        clusterService.close();
        terminate(threadPool);
        super.tearDown();
    }

    private SynonymsManagementAPIService buildService(Client client, ClusterService cs, int maxRules, int chunkSize) {
        return new SynonymsManagementAPIService(client, cs, maxRules, SynonymsManagementAPIService.PIT_BATCH_SIZE, chunkSize);
    }

    /**
     * All rules plus the synonym set document are written regardless of whether the rule count
     * falls below or above the chunk size boundary.
     */
    public void testBulkUpdateWritesAllDocs() throws Exception {
        int chunkSize = randomIntBetween(2, 10);
        // randomize below and above the chunk boundary
        int numRules = randomBoolean() ? randomIntBetween(1, chunkSize) : randomIntBetween(chunkSize + 1, chunkSize * 4);
        SynonymRule[] rules = randomSynonymsSet(numRules);

        var countingClient = new BulkCountingClient(threadPool);
        var service = buildService(countingClient, clusterService, numRules, chunkSize);

        var future = new PlainActionFuture<Void>();
        service.bulkUpdateSynonymsSet("my-set", rules, future);
        safeGet(future);

        // +1 for the synonym set document written in the first chunk
        assertThat(countingClient.totalDocCount.get(), equalTo(numRules + 1));
    }

    /**
     * An empty rule set still writes the synonym set document in one bulk request.
     */
    public void testBulkUpdateEmptySet() throws Exception {
        var countingClient = new BulkCountingClient(threadPool);
        var service = buildService(
            countingClient,
            clusterService,
            SynonymsManagementAPIService.PRE_LARGE_SETS_LIMIT,
            SynonymsManagementAPIService.BULK_CHUNK_SIZE
        );

        var future = new PlainActionFuture<Void>();
        service.bulkUpdateSynonymsSet("my-set", new SynonymRule[0], future);
        safeGet(future);

        assertThat(countingClient.bulkRequestCount.get(), equalTo(1));
        assertThat(countingClient.totalDocCount.get(), equalTo(1)); // just the set doc
    }

    /**
     * putSynonymsSet rejects writes above PRE_LARGE_SETS_LIMIT on a cluster that predates large synonym set support.
     */
    public void testTransportVersionGateRejectsLargeSetsOnOldCluster() {
        ClusterState oldClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putCompatibilityVersions(
                "node1",
                TransportVersionUtils.randomVersionNotSupporting(SynonymsManagementAPIService.SYNONYMS_LARGE_SETS),
                SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS
            )
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService oldClusterService = ClusterServiceUtils.createClusterService(oldClusterState, threadPool, clusterSettings);
        try {
            var countingClient = new BulkCountingClient(threadPool);
            var service = buildService(
                countingClient,
                oldClusterService,
                SynonymsManagementAPIService.PRE_LARGE_SETS_LIMIT + 10,
                SynonymsManagementAPIService.BULK_CHUNK_SIZE
            );

            SynonymRule[] rules = randomSynonymsSet(SynonymsManagementAPIService.PRE_LARGE_SETS_LIMIT + 1);
            var future = new PlainActionFuture<SynonymsManagementAPIService.SynonymsReloadResult>();
            service.putSynonymsSet("my-set", rules, false, future);

            Exception ex = expectThrows(ElasticsearchException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(ex.getMessage(), containsString("all nodes in the cluster have been upgraded"));
            assertThat(countingClient.bulkRequestCount.get(), equalTo(0));
        } finally {
            oldClusterService.close();
        }
    }

    /**
     * putSynonymRule rejects a rule that would push the count above PRE_LARGE_SETS_LIMIT on a cluster that predates
     * large synonym set support.
     */
    public void testPutSynonymRuleUpgradeGuardRejectsOnOldCluster() {
        ClusterState oldClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putCompatibilityVersions(
                "node1",
                TransportVersionUtils.randomVersionNotSupporting(SynonymsManagementAPIService.SYNONYMS_LARGE_SETS),
                SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS
            )
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService oldClusterService = ClusterServiceUtils.createClusterService(oldClusterState, threadPool, clusterSettings);
        try {
            // Set already at the limit; adding one more rule would cross it.
            var client = new SynonymSetExistsAndCountClient(threadPool, SynonymsManagementAPIService.PRE_LARGE_SETS_LIMIT);
            var service = buildService(
                client,
                oldClusterService,
                SynonymsManagementAPIService.PRE_LARGE_SETS_LIMIT + 10,
                SynonymsManagementAPIService.BULK_CHUNK_SIZE
            );

            var future = new PlainActionFuture<SynonymsManagementAPIService.SynonymsReloadResult>();
            service.putSynonymRule("my-set", new SynonymRule(null, "foo => bar"), false, future);

            Exception ex = expectThrows(ElasticsearchException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(ex.getMessage(), containsString("all nodes in the cluster have been upgraded"));
            assertThat("no index request should have been issued", client.indexRequestCount.get(), equalTo(0));
        } finally {
            oldClusterService.close();
        }
    }

    /**
     * A mid-sequence chunk failure propagates to the listener and halts subsequent chunks.
     */
    public void testBulkUpdateChunkFailurePropagatesToListener() throws Exception {
        int chunkSize = 1;
        int numRules = 3; // 3 chunks: chunk 0 succeeds, chunk 1 fails, chunk 2 must not run
        SynonymRule[] rules = randomSynonymsSet(numRules);

        var failingClient = new FailOnChunkClient(threadPool, 1);
        var service = buildService(failingClient, clusterService, numRules, chunkSize);

        var future = new PlainActionFuture<Void>();
        service.bulkUpdateSynonymsSet("my-set", rules, future);

        Exception ex = expectThrows(Exception.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(ex.getMessage(), containsString("Error updating synonyms"));
        assertThat("chunk 2 must not be attempted after chunk 1 fails", failingClient.bulkRequestCount.get(), equalTo(2));
    }

    /**
     * Fakes an existing synonym set of a known size: GET returns found, SEARCH returns {@code ruleCount} total hits,
     * and INDEX requests are counted so tests can assert none were issued.
     */
    private static class SynonymSetExistsAndCountClient extends NoOpClient {
        final AtomicInteger indexRequestCount = new AtomicInteger();
        private final long ruleCount;

        SynonymSetExistsAndCountClient(ThreadPool threadPool, long ruleCount) {
            super(threadPool);
            this.ruleCount = ruleCount;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof GetRequest getRequest) {
                var getResult = new GetResult(
                    getRequest.index(),
                    getRequest.id(),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                    1,
                    true,
                    null,
                    null,
                    null
                );
                ((ActionListener<GetResponse>) listener).onResponse(new GetResponse(getResult));
                return;
            }
            if (request instanceof SearchRequest) {
                SearchHits hits = SearchHits.empty(new TotalHits(ruleCount, TotalHits.Relation.EQUAL_TO), Float.NaN);
                ActionListener.respondAndRelease((ActionListener<SearchResponse>) listener, SearchResponseUtils.successfulResponse(hits));
                return;
            }
            if (request instanceof IndexRequest) {
                indexRequestCount.incrementAndGet();
            }
            super.doExecute(action, request, listener);
        }
    }

    /**
     * Succeeds on all bulk requests until {@code failOnChunk} (0-based), then returns a bulk failure.
     */
    private static class FailOnChunkClient extends NoOpClient {
        final AtomicInteger bulkRequestCount = new AtomicInteger();
        private final int failOnChunk;

        FailOnChunkClient(ThreadPool threadPool, int failOnChunk) {
            super(threadPool);
            this.failOnChunk = failOnChunk;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof BulkRequest bulkRequest) {
                int chunk = bulkRequestCount.getAndIncrement();
                if (chunk == failOnChunk) {
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(
                        "synonyms",
                        bulkRequest.requests().get(0).id(),
                        new RuntimeException("simulated shard failure")
                    );
                    BulkItemResponse failed = BulkItemResponse.failure(0, DocWriteRequest.OpType.INDEX, failure);
                    ((ActionListener<BulkResponse>) listener).onResponse(new BulkResponse(new BulkItemResponse[] { failed }, 0L));
                } else {
                    ((ActionListener<BulkResponse>) listener).onResponse(new BulkResponse(new BulkItemResponse[0], 0L));
                }
                return;
            }
            super.doExecute(action, request, listener);
        }
    }

    /**
     * Counts bulk requests and documents without requiring a live cluster.
     */
    private static class BulkCountingClient extends NoOpClient {
        final AtomicInteger bulkRequestCount = new AtomicInteger();
        final AtomicInteger totalDocCount = new AtomicInteger();

        BulkCountingClient(ThreadPool threadPool) {
            super(threadPool);
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
