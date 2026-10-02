/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersRequest;
import org.elasticsearch.action.admin.indices.analyze.TransportReloadAnalyzersAction;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageRequest;
import org.elasticsearch.action.admin.indices.diskusage.TransportAnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsAction;
import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration tests documenting the current behaviour of Elasticsearch APIs against a
 * shard being restored from a snapshot.
 *
 * <p>Most read/write paths return HTTP 503. Term vectors are an exception: they route to the shard
 * directly via {@code TransportSingleShardAction}, hit the read-allowed-states check, and return
 * HTTP 404 ({@link IllegalIndexShardStateException}) wrapped in an
 * {@code ElasticsearchException}. Search paths ({@code _search}, {@code _msearch}) are different
 * again: {@code SearchReadyGate} parks the request rather than failing immediately.
 *
 * <p>Setup: a single-shard, no-replica index is deleted before restore so that chunk-blob reads
 * are required, which is what {@code blockAllDataNodes} actually blocks.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class RestoringShardIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";
    private static final String DOC_ID = "some_id";
    private static final String KNN_INDEX = "test-restore-knn-index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class);
    }

    /**
     * Creates the repository and snapshot once for the entire suite. Each test restores from this
     * snapshot. The index is deleted (not closed) so repo reads are required during recovery, which
     * is what {@code blockAllDataNodes} actually blocks.
     */
    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        createIndexWithContent(INDEX);

        // Create a dense-vector index for the knn search test
        indicesAdmin().prepareCreate(KNN_INDEX)
            .setSettings(SINGLE_SHARD_NO_REPLICA)
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("vector")
                    .field("type", "dense_vector")
                    .field("dims", 3)
                    .field("index", true)
                    .field("similarity", "cosine")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        client().prepareIndex(KNN_INDEX)
            .setSource(XContentFactory.jsonBuilder().startObject().field("vector", new float[] { 1.0f, 2.0f, 3.0f }).endObject())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        ensureGreen(INDEX, KNN_INDEX);
        createRepository(REPO, "mock");
        createFullSnapshot(REPO, SNAPSHOT);
        assertAcked(indicesAdmin().prepareDelete(INDEX, KNN_INDEX));
    }

    // -------------------------------------------------------------------------
    // Read path — mostly HTTP 503 via NoShardAvailableActionException;
    // term vectors are HTTP 404 via IllegalIndexShardStateException
    // -------------------------------------------------------------------------

    /**
     * {@code GET /{index}/_doc/{id}} and {@code GET /{index}/_source/{id}} against a restoring
     * shard currently return {@link NoShardAvailableActionException} (HTTP 503) — the shard
     * iterator is empty because the shard is INITIALIZING. Both REST endpoints share
     * {@code TransportGetAction}, so a single transport-layer test covers both.
     */
    public void testGetDocWhileRestoringReturnsServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            NoShardAvailableActionException e = expectThrows(NoShardAvailableActionException.class, client().prepareGet(INDEX, DOC_ID));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code GET /{index}/_doc/{id}} with {@code realtime=false} falls through to the
     * {@code TransportSingleShardAction} read-from-any-copy path and also throws
     * {@link NoShardAvailableActionException} (HTTP 503) when all copies are restoring.
     */
    public void testGetDocNonRealtimeWhileRestoringReturnsServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            NoShardAvailableActionException e = expectThrows(
                NoShardAvailableActionException.class,
                client().prepareGet(INDEX, DOC_ID).setRealtime(false)
            );
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code GET /{index}/_explain/{id}} returns {@link NoShardAvailableActionException} (HTTP 503)
     * via {@code TransportSingleShardAction}.
     */
    public void testExplainWhileRestoringReturnsServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            NoShardAvailableActionException e = expectThrows(
                NoShardAvailableActionException.class,
                client().prepareExplain(INDEX, DOC_ID).setQuery(QueryBuilders.matchAllQuery())
            );
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code GET /{index}/_termvectors/{id}} routes through {@code TransportSingleShardAction},
     * which can reach an INITIALIZING shard (routing state; the node-local {@code IndexShardState}
     * is {@code RECOVERING}). It then hits the read-allowed-states check in {@code IndexShard},
     * which throws {@link IllegalIndexShardStateException} (HTTP 404). That is wrapped by the
     * transport action in a plain {@code ElasticsearchException("failed to execute term vector
     * request")} before being surfaced to the caller.
     *
     * <p>The wrapping matters: {@code ElasticsearchException#status()} only defers to a cause's
     * status when the exception implements {@code ElasticsearchWrapperException}. A plain
     * {@code ElasticsearchException} does not, so it reports its own default —
     * {@code INTERNAL_SERVER_ERROR} (500) — regardless of the wrapped cause's 404. That 500 is
     * what actually reaches the REST layer; the inner 404 is only visible by inspecting the cause
     * directly, which no real client does.
     */
    public void testTermVectorsWhileRestoringReturnsInternalServerError() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            Exception e = expectThrows(Exception.class, client().prepareTermVectors(INDEX, DOC_ID));
            assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
            assertThat(e.getCause(), instanceOf(IllegalIndexShardStateException.class));
            assertThat(((IllegalIndexShardStateException) e.getCause()).status(), equalTo(RestStatus.NOT_FOUND));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Write path — Category A: HTTP 503 via UnavailableShardsException (after timeout)
    //
    // Unlike reads, writes retry until the request timeout elapses before failing.
    // A short timeout is used here to keep the suite fast.
    // -------------------------------------------------------------------------

    /**
     * {@code POST /{index}/_doc} against a restoring primary retries until the request
     * timeout elapses, then returns {@link UnavailableShardsException} (HTTP 503).
     */
    public void testIndexDocWhileRestoringReturnsServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            UnavailableShardsException e = expectThrows(
                UnavailableShardsException.class,
                client().prepareIndex(INDEX).setSource("field", "value").setTimeout(TimeValue.timeValueMillis(100))
            );
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code DELETE /{index}/_doc/{id}} retries until timeout and returns
     * {@link UnavailableShardsException} (HTTP 503).
     */
    public void testDeleteDocWhileRestoringReturnsServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            UnavailableShardsException e = expectThrows(
                UnavailableShardsException.class,
                client().prepareDelete(INDEX, DOC_ID).setTimeout(TimeValue.timeValueMillis(100))
            );
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code POST /{index}/_update/{id}} retries until timeout and returns
     * {@link UnavailableShardsException} (HTTP 503).
     */
    public void testUpdateDocWhileRestoringReturnsServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            UnavailableShardsException e = expectThrows(
                UnavailableShardsException.class,
                client().prepareUpdate(INDEX, DOC_ID).setDoc("field", "new-value").setTimeout(TimeValue.timeValueMillis(100))
            );
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Search — current behaviour: SearchReadyGate parks the request (not 503)
    //
    // Unlike read/write operations, searches against an INITIALIZING shard are not
    // rejected immediately. SearchService.rewriteAndFetchShardRequest calls
    // IndexShard.waitForSearchReady(), which parks the request until the shard
    // transitions to POST_RECOVERY or STARTED. While the repository is blocked
    // the shard never advances, so the search hangs indefinitely.
    //
    // Each test below asserts this parking behaviour with a short get() timeout.
    // -------------------------------------------------------------------------

    /**
     * {@code POST /{index}/_search} with {@code allow_partial_search_results=false} against a
     * fully-restoring index does NOT return HTTP 503 immediately. Instead the search is parked by
     * {@code SearchReadyGate} and waits for the primary shard to become search-ready.
     */
    public void testSearchWithPartialResultsFalseAgainstRestoringShardParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        var future = client().prepareSearch(INDEX).setAllowPartialSearchResults(false).execute();
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            // Drain: after unblocking the repo the shard recovers (onReady fires) or the index
            // is deleted (onClosed fires); either way the parked future must be consumed.
            try {
                future.get(10, TimeUnit.SECONDS).decRef();
            } catch (Exception ignored) {}
        }
    }

    /**
     * {@code POST /{index}/_search} with {@code allow_partial_search_results=true} also parks
     * the search via {@code SearchReadyGate}. The behaviour is identical to
     * {@code allow_partial_search_results=false}: both wait rather than returning 503.
     */
    public void testSearchWithPartialResultsTrueAgainstRestoringShardParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        var future = client().prepareSearch(INDEX).setAllowPartialSearchResults(true).execute();
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try {
                future.get(10, TimeUnit.SECONDS).decRef();
            } catch (Exception ignored) {}
        }
    }

    /**
     * Opening a scroll ({@code GET /{index}/_search?scroll=1m}) goes through
     * {@code TransportSearchAction} and parks in {@code SearchReadyGate} identically to a plain
     * {@code _search}. The scroll ID is never returned while the repository is blocked.
     *
     * <p>Note: continuing a scroll ({@code POST _search/scroll}) is not testable against a
     * restoring shard in this setup — scroll contexts are tied to open searcher copies, and none
     * exist on a shard that was deleted before being restored.
     */
    public void testScrollSearchWhileRestoringParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        var future = client().prepareSearch(INDEX).setScroll(TimeValue.timeValueMinutes(1)).execute();
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try {
                future.get(10, TimeUnit.SECONDS).decRef();
            } catch (Exception ignored) {}
        }
    }

    /**
     * kNN search via {@code SearchSourceBuilder.knnSearch} also routes through
     * {@code TransportSearchAction} and parks in {@code SearchReadyGate} when the target shard is
     * INITIALIZING. The behaviour is identical to {@code _search}: the request hangs until the
     * shard becomes search-ready.
     *
     * <p>This only holds for a single-shard target: {@code TransportSearchAction#adjustSearchType}
     * always forces {@code DFS_QUERY_THEN_FETCH} when a kNN clause is present, which is otherwise
     * harmless, but if the index has more than one shard it also makes
     * {@code TransportSearchAction#shouldPreFilterSearchShards} run a {@code canMatch} pre-filter
     * phase. That phase fails fast on a restoring shard (rather than parking) and, since
     * {@code allow_partial_search_results} defaults to {@code true}, the overall search then
     * completes immediately with zero hits instead of hanging. Hence {@code KNN_INDEX} is pinned to
     * {@link #SINGLE_SHARD_NO_REPLICA} here, matching {@code INDEX}.
     */
    public void testKnnSearchWhileRestoringParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, KNN_INDEX);
        SearchRequest knnRequest = new SearchRequest(KNN_INDEX);
        knnRequest.source(
            new SearchSourceBuilder().knnSearch(
                List.of(new KnnSearchBuilder("vector", new float[] { 1.0f, 2.0f, 3.0f }, 5, 50, null, null, null))
            )
        );
        var future = client().search(knnRequest);
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, KNN_INDEX);
            try {
                future.get(10, TimeUnit.SECONDS).decRef();
            } catch (Exception ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Bulk — Category B: HTTP 200 envelope; per-item 503
    // -------------------------------------------------------------------------

    /**
     * {@code POST /_bulk} keeps its HTTP 200 envelope but each item routed to a restoring shard
     * currently has {@code items[].<op>.status == 503} ({@link UnavailableShardsException}).
     * A short per-item timeout is used to avoid waiting for the default 1-minute write timeout.
     */
    public void testBulkWhileRestoringReturnsPerItemServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            // Timeout must be set on the BulkRequestBuilder — individual item timeouts are ignored by
            // BulkShardRequest, which inherits its timeout from bulkRequest.timeout() in BulkOperation.
            BulkResponse bulk = client().prepareBulk()
                .add(client().prepareIndex(INDEX).setId("new1").setSource("field", "value"))
                .add(client().prepareIndex(INDEX).setId("new2").setSource("field", "value"))
                .setTimeout(TimeValue.timeValueMillis(100))
                .get();

            assertThat(bulk.hasFailures(), equalTo(true));
            for (BulkItemResponse item : bulk) {
                assertThat(item.isFailed(), equalTo(true));
                assertThat(item.getFailure().getStatus(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
                assertThat(ExceptionsHelper.unwrapCause(item.getFailure().getCause()), instanceOf(UnavailableShardsException.class));
            }
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Multi-get — Category B: HTTP 200 envelope; per-doc 503
    // -------------------------------------------------------------------------

    /**
     * {@code GET /_mget} keeps its HTTP 200 envelope but each failed item currently carries a
     * {@link NoShardAvailableActionException} (HTTP 503) in {@code docs[].error}.
     */
    public void testMgetWhileRestoringReturnsPerDocServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            MultiGetResponse response = client().prepareMultiGet().add(INDEX, DOC_ID).add(INDEX, "other-id").get();

            assertThat(response.getResponses().length, greaterThan(0));
            for (var item : response.getResponses()) {
                assertThat(item.isFailed(), equalTo(true));
                assertThat(ExceptionsHelper.status(item.getFailure().getFailure()), equalTo(RestStatus.SERVICE_UNAVAILABLE));
                assertThat(ExceptionsHelper.unwrapCause(item.getFailure().getFailure()), instanceOf(NoShardAvailableActionException.class));
            }
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Multi-search — current behaviour: sub-searches are also parked
    //
    // _msearch wraps individual search requests; each sub-search hits
    // SearchReadyGate and parks in the same way as a standalone _search call.
    // -------------------------------------------------------------------------

    /**
     * {@code GET /_msearch} parks its sub-searches via {@code SearchReadyGate} when the target
     * index is fully restoring. The outer request therefore also hangs.
     */
    public void testMsearchWhileRestoringParksSubSearchRatherThan503() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        var future = client().prepareMultiSearch().add(client().prepareSearch(INDEX).setAllowPartialSearchResults(false)).execute();
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try {
                future.get(10, TimeUnit.SECONDS).decRef();
            } catch (Exception ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Broadcast ops — Category C: unchanged, _shards.failed stays 0
    // -------------------------------------------------------------------------

    /**
     * {@code POST /{index}/_refresh} silently swallows the shard-unavailable exception via
     * {@code isShardNotAvailableException} — {@code _shards.failed} stays 0.
     */
    public void testRefreshWhileRestoringSwallowsExceptionAndReportsNoFailedShards() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            assertThat(indicesAdmin().prepareRefresh(INDEX).get().getFailedShards(), equalTo(0));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code POST /{index}/_flush} silently swallows the shard-unavailable exception —
     * {@code _shards.failed} stays 0.
     */
    public void testFlushWhileRestoringSwallowsExceptionAndReportsNoFailedShards() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code POST /{index}/_forcemerge} silently swallows the shard-unavailable exception —
     * {@code _shards.failed} stays 0.
     */
    public void testForceMergeWhileRestoringSwallowsExceptionAndReportsNoFailedShards() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            assertThat(indicesAdmin().prepareForceMerge(INDEX).get().getFailedShards(), equalTo(0));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code GET /{index}/_segments} silently swallows the shard-unavailable exception —
     * {@code _shards.failed} stays 0.
     */
    public void testSegmentsWhileRestoringSwallowsExceptionAndReportsNoFailedShards() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            assertThat(indicesAdmin().prepareSegments(INDEX).get().getFailedShards(), equalTo(0));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code GET /{index}/_stats} silently swallows the shard-unavailable exception —
     * {@code _shards.failed} stays 0.
     */
    public void testStatsWhileRestoringSwallowsExceptionAndReportsNoFailedShards() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            assertThat(indicesAdmin().prepareStats(INDEX).get().getFailedShards(), equalTo(0));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code POST /{index}/_cache/clear} silently swallows the shard-unavailable exception —
     * {@code _shards.failed} stays 0.
     */
    public void testClearCacheWhileRestoringSwallowsExceptionAndReportsNoFailedShards() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            assertThat(indicesAdmin().prepareClearCache(INDEX).get().getFailedShards(), equalTo(0));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code GET|POST /{index}/_validate/query} uses {@code TransportBroadcastAction} (not
     * {@code TransportBroadcastByNodeAction}), so shard failures are accumulated in the response
     * rather than silently dropped. While the primary is INITIALIZING, {@code _shards.failed} is
     * greater than zero.
     */
    public void testValidateQueryWhileRestoringReportsShardFailure() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            assertThat(
                indicesAdmin().prepareValidateQuery(INDEX).setQuery(QueryBuilders.matchAllQuery()).get().getFailedShards(),
                greaterThan(0)
            );
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code GET /{index}/_field_usage_stats} silently swallows the shard-unavailable exception —
     * {@code _shards.failed} stays 0.
     */
    public void testFieldUsageStatsWhileRestoringSwallowsExceptionAndReportsNoFailedShards() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            assertThat(
                client().execute(FieldUsageStatsAction.INSTANCE, new FieldUsageStatsRequest(new String[] { INDEX }))
                    .actionGet()
                    .getFailedShards(),
                equalTo(0)
            );
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code POST /{index}/_reload_search_analyzers} silently swallows the shard-unavailable
     * exception — {@code _shards.failed} stays 0.
     */
    public void testReloadSearchAnalyzersWhileRestoringSwallowsExceptionAndReportsNoFailedShards() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            assertThat(
                client().execute(TransportReloadAnalyzersAction.TYPE, new ReloadAnalyzersRequest(null, false, INDEX))
                    .actionGet()
                    .getFailedShards(),
                equalTo(0)
            );
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Master-node read operations — read cluster state only, never contact data nodes
    // -------------------------------------------------------------------------

    /**
     * {@code GET /{index}/_search_shards} reads routing information from the master node's cluster
     * state. It does not contact data nodes or open a shard connection, so it succeeds even while
     * the primary is INITIALIZING. The response includes the shard routing group for the restoring
     * shard.
     */
    public void testSearchShardsWhileRestoringSucceeds() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            var resp = client().execute(
                TransportClusterSearchShardsAction.TYPE,
                new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, INDEX)
            ).actionGet();
            assertThat(resp.getGroups().length, greaterThan(0));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code GET /{index}/_shard_stores} reads shard store information from the master node's
     * cluster state. It does not contact data nodes, so it succeeds even while the primary is
     * INITIALIZING.
     */
    public void testShardStoresWhileRestoringSucceeds() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            // Succeeds — reads shard allocation state from master cluster state only.
            client().execute(TransportIndicesShardStoresAction.TYPE, new IndicesShardStoresRequest(INDEX)).actionGet();
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code GET /{index}/_recovery} succeeds during a restore and exposes the in-progress
     * recovery state for the restoring shard. Unlike the other broadcast operations above,
     * recovery info is served from cluster state and shard metadata — it does not require an
     * active shard connection, so there is nothing to swallow.
     */
    public void testRecoveryWhileRestoringSucceeds() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            assertThat(indicesAdmin().prepareRecoveries(INDEX).get().shardRecoveryInfos().containsKey(INDEX), equalTo(true));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Additional read-path APIs — all HTTP 503 via NoShardAvailableActionException
    // -------------------------------------------------------------------------

    /**
     * {@code POST /{index}/_create/{id}} goes through {@code TransportIndexAction} with
     * {@code op_type=CREATE}. Like plain index/delete operations, it retries until the
     * request timeout elapses, then returns {@link UnavailableShardsException} (HTTP 503).
     */
    public void testCreateDocWhileRestoringReturnsServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            UnavailableShardsException e = expectThrows(
                UnavailableShardsException.class,
                client().prepareIndex(INDEX)
                    .setId("new-create-id")
                    .setSource("field", "value")
                    .setOpType(DocWriteRequest.OpType.CREATE)
                    .setTimeout(TimeValue.timeValueMillis(100))
            );
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code POST /{index}/_mtermvectors} returns HTTP 200 with per-item failures when the
     * target shard is restoring. Each item carries the same wrapped
     * {@link IllegalIndexShardStateException} seen in single-doc {@code _termvectors}.
     */
    public void testMtermvectorsWhileRestoringReturnsPerItemError() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            MultiTermVectorsResponse resp = client().multiTermVectors(new MultiTermVectorsRequest().add(INDEX, DOC_ID)).actionGet();
            assertThat(resp.getResponses().length, greaterThan(0));
            for (var item : resp) {
                assertThat(item.isFailed(), equalTo(true));
                Exception cause = item.getFailure().getCause();
                assertThat(ExceptionsHelper.status(cause), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
                assertThat(cause.getCause(), instanceOf(IllegalIndexShardStateException.class));
                assertThat(((IllegalIndexShardStateException) cause.getCause()).status(), equalTo(RestStatus.NOT_FOUND));
            }
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Count — parks like _search (delegates to TransportSearchAction)
    // -------------------------------------------------------------------------

    /**
     * {@code GET /{index}/_count} delegates to {@code TransportSearchAction} internally
     * ({@code RestCountAction} builds a {@code SearchRequest} with size=0). It parks in
     * {@code SearchReadyGate} identically to {@code _search}.
     */
    public void testCountWhileRestoringParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        var future = client().prepareSearch(INDEX).setSize(0).execute();
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try {
                future.get(10, TimeUnit.SECONDS).decRef();
            } catch (Exception ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Schema discovery — _field_caps
    // -------------------------------------------------------------------------

    /**
     * {@code GET /{index}/_field_caps} against a fully-restoring index fails fast with HTTP 404
     * ({@code IllegalIndexShardStateException}). The exception is thrown directly rather than
     * collected as a per-index failure in the response.
     */
    public void testFieldCapsWhileRestoringFailsFastWithNotFound() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            Exception e = expectThrows(
                Exception.class,
                () -> client().fieldCaps(new FieldCapabilitiesRequest().indices(INDEX).fields("*")).actionGet()
            );
            assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.NOT_FOUND));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Disk usage — _disk_usage
    // -------------------------------------------------------------------------

    /**
     * {@code POST /{index}/_disk_usage} is a broadcast-style shard action.
     * Against a restoring shard it does not throw at the top level, but unlike other broadcast actions
     * it surfaces the failure via {@code _shards.failed} rather than silently swallowing it.
     */
    public void testDiskUsageWhileRestoringReportsShardFailures() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            var resp = client().execute(
                TransportAnalyzeIndexDiskUsageAction.TYPE,
                new AnalyzeIndexDiskUsageRequest(new String[] { INDEX }, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, false)
            ).actionGet();
            assertThat(resp.getTotalShards(), greaterThan(0));
            assertThat(resp.getFailedShards(), greaterThan(0));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Text analysis — _analyze
    // -------------------------------------------------------------------------

    /**
     * {@code GET /{index}/_analyze} routes to a primary shard to pick up index-level analyzer settings.
     * While the primary is INITIALIZING it cannot be reached, so the request fails immediately with
     * HTTP 503 ({@code NoShardAvailableActionException}).
     */
    public void testAnalyzeWithIndexWhileRestoringReturnsServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            Exception e = expectThrows(
                NoShardAvailableActionException.class,
                () -> indicesAdmin().prepareAnalyze(INDEX, "test text").setAnalyzer("standard").get()
            );
            assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Open point-in-time (_pit) — fails fast, does not park
    // -------------------------------------------------------------------------

    /**
     * {@code POST /{index}/_pit} (open point-in-time) does NOT park in {@code SearchReadyGate}.
     * Unlike {@code _search}, PIT open uses {@code SearchService.openReaderContext()} rather than
     * {@code rewriteAndFetchShardRequest()}, so it bypasses the gate. When the shard is INITIALIZING,
     * {@code IndexShard.acquireExternalSearcherSupplier()} throws
     * {@link IllegalIndexShardStateException}, which {@code AbstractSearchAsyncAction} classifies as
     * a shard-not-available exception and wraps as {@link NoShardAvailableActionException} (HTTP 503).
     * With {@code allowPartialSearchResults=false} (the PIT default), the top-level response is
     * HTTP 503 rather than a successful (but partial) PIT ID.
     */
    public void testOpenPitWhileRestoringFailsFastWithServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            Exception e = expectThrows(
                Exception.class,
                () -> client().execute(
                    TransportOpenPointInTimeAction.TYPE,
                    new OpenPointInTimeRequest(INDEX).keepAlive(TimeValue.timeValueMinutes(1))
                ).actionGet(TimeValue.timeValueSeconds(10))
            );
            assertThat(ExceptionsHelper.status(ExceptionsHelper.unwrapCause(e)), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Field mapping — _mapping/field/{fields}
    // -------------------------------------------------------------------------

    /**
     * {@code GET /{index}/_mapping/field/{fields}} via {@code TransportGetFieldMappingsIndexAction}
     * ({@code TransportSingleShardAction}) does <strong>not</strong> require the Lucene shard to be
     * started to read field mappings — {@code MapperService} is populated from cluster state
     * independently of shard state. However {@code TransportGetFieldMappingsIndexAction#shards}
     * only considers {@code randomAllActiveShardsIt()}, i.e. shards in STARTED/RELOCATING. On this
     * single-shard, no-replica index there is no active copy while the primary is INITIALIZING, so
     * the per-index sub-action fails with {@link NoShardAvailableActionException}.
     *
     * <p>That failure never reaches the caller: the coordinating
     * {@code TransportGetFieldMappingsAction#merge} only accumulates successful per-index
     * responses and silently drops failed ones, so the top-level call still completes successfully
     * — just with an empty mappings map — rather than surfacing the shard-unavailable error.
     */
    public void testGetFieldMappingWhileRestoringReturnsEmptyMappings() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            var response = client().execute(GetFieldMappingsAction.INSTANCE, new GetFieldMappingsRequest().indices(INDEX).fields("*"))
                .actionGet();
            assertThat(response.mappings(), equalTo(Map.of()));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code GET /{index}/_mapping} (full mapping endpoint) reads the entire mapping from
     * {@code MapperService}, which is populated from cluster state. Like
     * {@code _mapping/field/{fields}}, it does not require the Lucene shard to be started and
     * succeeds while the primary is INITIALIZING.
     */
    public void testGetMappingsWhileRestoringSucceeds() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            var response = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, INDEX).get();
            assertThat(response.mappings().containsKey(INDEX), equalTo(true));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }
}
