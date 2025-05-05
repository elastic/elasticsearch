/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.reshard.MetadataReshardIndexService;
import co.elastic.elasticsearch.stateless.reshard.ReshardIndexRequest;
import co.elastic.elasticsearch.stateless.reshard.ReshardIndexResponse;
import co.elastic.elasticsearch.stateless.reshard.SplitTargetService;
import co.elastic.elasticsearch.stateless.reshard.TransportReshardAction;
import co.elastic.elasticsearch.stateless.reshard.TransportReshardSplitAction;
import co.elastic.elasticsearch.stateless.reshard.TransportUpdateSplitStateAction;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StatelessReshardIT extends AbstractStatelessIntegTestCase {

    public void testReshardWillRouteDocumentsToNewShard() throws Exception {
        String indexNode = startMasterAndIndexNode();
        String searchNode = startSearchNode();

        ensureStableCluster(2);

        final int multiple = randomIntBetween(2, 10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        assertThat(getIndexCount(client().admin().indices().prepareStats(indexName).execute().actionGet(), 0), equalTo((long) numDocs));

        var initialIndexMetadata = clusterService().state().projectState().metadata().index(indexName);
        // before resharding there should be no resharding metadata
        assertNull(initialIndexMetadata.getReshardingMetadata());

        // there should be split metadata at some point during resharding
        var splitState = waitForClusterState((state) -> state.projectState().metadata().index(indexName).getReshardingMetadata() != null);

        logger.info("starting reshard");
        var reshardAction = client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple));

        logger.info("getting reshard metadata");
        var reshardingMetadata = splitState.actionGet(SAFE_AWAIT_TIMEOUT)
            .projectState()
            .metadata()
            .index(indexName)
            .getReshardingMetadata();
        assertNotNull(reshardingMetadata.getSplit());
        assert reshardingMetadata.shardCountBefore() == 1;
        assert reshardingMetadata.shardCountAfter() == multiple;

        reshardAction.actionGet(SAFE_AWAIT_TIMEOUT);

        // resharding data should eventually be removed after split executes
        waitForClusterState((state) -> state.projectState().metadata().index(indexName).getReshardingMetadata() == null).actionGet(
            SAFE_AWAIT_TIMEOUT
        );

        // index documents until all the new shards have received at least one document
        final int[] docsPerShard = new int[multiple];
        int docsPerRequest = randomIntBetween(10, 100);
        int shards = 0;
        int numDocsRound2 = 0;
        do {
            for (var item : indexDocs(indexName, docsPerRequest).getItems()) {
                if (docsPerShard[item.getResponse().getShardId().getId()]++ == 0) {
                    shards++;
                }
            }
            numDocsRound2 += docsPerRequest;
        } while (shards < multiple);

        // include the original docs in the first shard
        docsPerShard[0] += numDocs;

        // verify that each shard id contains the expected number of documents indexed into it
        IndicesStatsResponse postReshardStatsResponse = client().admin().indices().prepareStats(indexName).execute().actionGet();

        IntStream.range(0, multiple)
            .forEach(shardId -> assertThat(getIndexCount(postReshardStatsResponse, shardId), equalTo((long) docsPerShard[shardId])));

        // index more documents to verify that a search query returns all indexed documents thus far
        final int numDocsRound3 = randomIntBetween(10, 100);
        indexDocs(indexName, numDocsRound3);

        refresh(indexName);

        assertHitCount(
            prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true),
            numDocs + numDocsRound2 + numDocsRound3
        );

        // verify that the index metadata returned matches the expected multiple of shards
        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(multiple)
        );
    }

    public void testReshardSearchShardWillNotBeAllocatedUntilIndexingShard() throws Exception {
        String indexNode = startMasterAndIndexNode();
        startSearchNode();
        startSearchNode();

        ensureStableCluster(3);

        /* This allocation rule is used to prevent the new reshard index shard from getting allocated.
         * This will also prevent the search shard from getting allocated, hence we have two search nodes above.
         */
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        ReshardIndexRequest request = new ReshardIndexRequest(indexName, 2);
        ActionFuture<ReshardIndexResponse> executed = client(indexNode).execute(TransportReshardAction.TYPE, request);

        assertBusy(() -> {
            GetSettingsResponse postReshardSettingsResponse = client().admin()
                .indices()
                .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                .get();
            assertThat(
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
                equalTo(2)
            );
        });

        // Briefly pause to give time for allocation to have theoretically occurred
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));

        final var indexingShardExplain = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex(indexName)
            .setShard(1)
            .setPrimary(true);
        assertThat(
            client().execute(TransportClusterAllocationExplainAction.TYPE, indexingShardExplain)
                .actionGet()
                .getExplanation()
                .getUnassignedInfo()
                .reason(),
            equalTo(UnassignedInfo.Reason.RESHARD_ADDED)
        );

        final var searchShardExplain = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex(indexName)
            .setShard(1)
            .setPrimary(false);
        assertThat(
            client().execute(TransportClusterAllocationExplainAction.TYPE, searchShardExplain)
                .actionGet()
                .getExplanation()
                .getUnassignedInfo()
                .reason(),
            equalTo(UnassignedInfo.Reason.RESHARD_ADDED)
        );

        // We have to clear the setting to prevent teardown issues with the cluster being red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), (String) null))
            .get();

        executed.actionGet();
    }

    public void testReshardFailsWithNullIndex() {
        String indexNode = startMasterAndIndexNode();

        ensureStableCluster(1);

        final String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        // Test null index
        ReshardIndexRequest request = new ReshardIndexRequest("", 2);
        expectThrows(IndexNotFoundException.class, () -> client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet());
    }

    public void testReshardFailsWithWildcardIndex() {
        String indexNode = startMasterAndIndexNode();

        ensureStableCluster(1);

        final String indexName1 = "test-1";
        final String indexName2 = "test-2";
        createIndex(indexName1, indexSettings(1, 0).build());
        createIndex(indexName2, indexSettings(1, 0).build());
        ensureGreen(indexName1);
        ensureGreen(indexName2);

        // Multiple indices not allowed "test*"
        ReshardIndexRequest request = new ReshardIndexRequest("test*", 2);
        expectThrows(IndexNotFoundException.class, () -> client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet());
    }

    public void testReshardFailsWithInvalidMultiple() {
        String indexNode = startMasterAndIndexNode();

        ensureStableCluster(1);

        final String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        // the multiple for resharding must be > 1, verify a few invalid multiples fail validation
        ReshardIndexRequest request = new ReshardIndexRequest("test-index", -1);
        expectThrows(
            ActionRequestValidationException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet()
        );

        // verify that the index metadata still contains only a sinlge shard
        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(1)
        );

        ReshardIndexRequest request2 = new ReshardIndexRequest("test-index", 0);
        expectThrows(
            ActionRequestValidationException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, request2).actionGet()
        );

        GetSettingsResponse postReshardSettingsResponse1 = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse1.getIndexToSettings().get(indexName)),
            equalTo(1)
        );

        ReshardIndexRequest request3 = new ReshardIndexRequest("test-index", 1);
        expectThrows(
            ActionRequestValidationException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, request3).actionGet()
        );

        GetSettingsResponse postReshardSettingsResponse2 = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse2.getIndexToSettings().get(indexName)),
            equalTo(1)
        );
    }

    public void testReshardWithIndexClose() throws Exception {
        String indexNode = startMasterAndIndexNode();
        String searchNode = startSearchNode();

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        indexDocs(indexName, 100);

        assertThat(getIndexCount(client().admin().indices().prepareStats(indexName).execute().actionGet(), 0), equalTo(100L));

        // assertAcked(indicesAdmin().prepareClose(indexName));
        assertBusy(() -> closeIndices(indexName));
        expectThrows(
            IndexClosedException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet()
        );

        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(1)
        );
    }

    public void testReshardSystemIndex() throws Exception {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        final String indexName = SYSTEM_INDEX_NAME + "-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        ReshardIndexRequest request = new ReshardIndexRequest(indexName, 2);
        expectThrows(IllegalArgumentException.class, () -> client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet());
    }

    public void testReshardTargetWillEqualToPrimaryTermOfSource() throws Exception {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client(indexNode).admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(1)
        );

        indexDocs(indexName, 100);

        IndicesStatsResponse statsResponse = client(indexNode).admin().indices().prepareStats(indexName).execute().actionGet();
        long indexCount = statsResponse.getAt(0).getStats().indexing.getTotal().getIndexCount();
        assertThat(indexCount, equalTo(100L));

        Index index = resolveIndex(indexName);
        long currentPrimaryTerm = getCurrentPrimaryTerm(index, 0);

        int primaryTermIncrements = randomIntBetween(2, 4);
        for (int i = 0; i < primaryTermIncrements; i++) {
            IndexShard indexShard = findIndexShard(index, 0);
            indexShard.failShard("broken", new Exception("boom local"));
            long finalCurrentPrimaryTerm = currentPrimaryTerm;
            assertBusy(() -> assertThat(getCurrentPrimaryTerm(index, 0), greaterThan(finalCurrentPrimaryTerm)));
            ensureGreen(indexName);
            currentPrimaryTerm = getCurrentPrimaryTerm(index, 0);
        }

        ReshardIndexRequest request = new ReshardIndexRequest(indexName, 2);
        client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet();
        ensureGreen(indexName);

        assertThat(getCurrentPrimaryTerm(index, 1), equalTo(currentPrimaryTerm));
    }

    @TestLogging(value = "co.elastic.elasticsearch.stateless.reshard.MetadataReshardIndexService:DEBUG", reason = "logging assertions")
    public void testReshardTargetWillNotTransitionToHandoffIfSourcePrimaryTermChanged() throws Exception {
        startMasterOnlyNode();
        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client(indexNode).admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(1)
        );

        indexDocs(indexName, 100);

        IndicesStatsResponse statsResponse = client(indexNode).admin().indices().prepareStats(indexName).execute().actionGet();
        long indexCount = statsResponse.getAt(0).getStats().indexing.getTotal().getIndexCount();
        assertThat(indexCount, equalTo(100L));

        Index index = resolveIndex(indexName);
        long currentPrimaryTerm = getCurrentPrimaryTerm(index, 0);

        MockTransportService mockTransportService = MockTransportService.getInstance(indexNode);
        CountDownLatch handoffAttemptedLatch = new CountDownLatch(1);
        CountDownLatch handoffLatch = new CountDownLatch(1);
        mockTransportService.addSendBehavior((connection, requestId, action, request1, options) -> {
            if (TransportUpdateSplitStateAction.TYPE.name().equals(action) && handoffAttemptedLatch.getCount() != 0) {
                try {
                    handoffAttemptedLatch.countDown();
                    handoffLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request1, options);
        });

        ReshardIndexRequest request = new ReshardIndexRequest(indexName, 2);
        ActionFuture<ReshardIndexResponse> reshard = client(indexNode).execute(TransportReshardAction.TYPE, request);

        handoffAttemptedLatch.await();

        IndexShard indexShard = findIndexShard(index, 0);
        indexShard.failShard("broken", new Exception("boom local"));
        final long finalPrimaryTerm = currentPrimaryTerm;
        assertBusy(() -> {
            assertThat(getCurrentPrimaryTerm(index, 0), greaterThan(finalPrimaryTerm));
            assertThat(
                client().admin()
                    .cluster()
                    .prepareHealth(TimeValue.timeValueSeconds(30))
                    .setIndices(indexName)
                    .get()
                    .getActivePrimaryShards(),
                equalTo(1)
            );
        });

        currentPrimaryTerm = getCurrentPrimaryTerm(index, 0);

        MockLog.assertThatLogger(() -> {
            // When we release the handoff block the recovery will progress. However, it will fail because the source shard primary term
            // has
            // advanced
            handoffLatch.countDown();
            reshard.actionGet();
        },
            MetadataReshardIndexService.class,
            new MockLog.PatternSeenEventExpectation(
                "split handoff failed",
                MetadataReshardIndexService.class.getCanonicalName(),
                Level.DEBUG,
                ".*\\[" + indexName + "\\]\\[1\\] cannot transition target state \\[HANDOFF\\] because source primary term advanced \\[.*"
            )
        );

        // After the target shard recovery tries again it will synchronize its primary term with the source and come online.
        ensureGreen(indexName);

        // The primary term has synchronized with the source
        assertThat(getCurrentPrimaryTerm(index, 1), equalTo(currentPrimaryTerm));
    }

    @TestLogging(value = "co.elastic.elasticsearch.stateless.reshard.MetadataReshardIndexService:DEBUG", reason = "logging assertions")
    public void testReshardTargetStateWillNotTransitionTargetPrimaryTermChanged() throws Exception {
        startMasterOnlyNode();
        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client(indexNode).admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(1)
        );

        indexDocs(indexName, 100);

        IndicesStatsResponse statsResponse = client(indexNode).admin().indices().prepareStats(indexName).execute().actionGet();
        long indexCount = statsResponse.getAt(0).getStats().indexing.getTotal().getIndexCount();
        assertThat(indexCount, equalTo(100L));

        Index index = resolveIndex(indexName);

        MockTransportService mockTransportService = MockTransportService.getInstance(indexNode);
        IndexReshardingState.Split.TargetShardState targetShardStateToDisrupt = randomFrom(
            IndexReshardingState.Split.TargetShardState.HANDOFF,
            IndexReshardingState.Split.TargetShardState.SPLIT,
            IndexReshardingState.Split.TargetShardState.DONE
        );
        CountDownLatch handoffAttemptedLatch = new CountDownLatch(switch (targetShardStateToDisrupt) {
            case HANDOFF -> 1;
            case SPLIT -> 2;
            case DONE -> 3;
            case CLONE -> throw new AssertionError();
        });
        CountDownLatch handoffLatch = new CountDownLatch(1);
        mockTransportService.addSendBehavior((connection, requestId, action, request1, options) -> {
            if (TransportUpdateSplitStateAction.TYPE.name().equals(action) && handoffAttemptedLatch.getCount() != 0) {
                try {
                    handoffAttemptedLatch.countDown();
                    if (handoffAttemptedLatch.getCount() == 0) {
                        handoffLatch.await();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request1, options);
        });

        ReshardIndexRequest request = new ReshardIndexRequest(indexName, 2);
        ActionFuture<ReshardIndexResponse> reshard = client(indexNode).execute(TransportReshardAction.TYPE, request);

        handoffAttemptedLatch.await();

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            final ProjectState projectState = state.projectState(state.metadata().projectFor(index).id());
            assertThat(projectState.routingTable().index(index).shard(1).primaryShard().allocationId(), notNullValue());
        });

        ClusterState state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final ProjectState projectState = state.projectState(state.metadata().projectFor(index).id());
        final ShardRouting shardRouting = projectState.routingTable().index(index).shard(1).primaryShard();
        final long currentPrimaryTerm = getCurrentPrimaryTerm(index, 1);

        ShardStateAction shardStateAction = internalCluster().getInstance(ShardStateAction.class, internalCluster().getRandomNodeName());
        PlainActionFuture<Void> listener = new PlainActionFuture<>();
        shardStateAction.localShardFailed(shardRouting, "broken", new Exception("boom remote"), listener);
        listener.actionGet();

        assertBusy(() -> assertThat(getCurrentPrimaryTerm(index, 1), greaterThan(currentPrimaryTerm)));

        MockLog.assertThatLogger(() -> {
            // When we release the handoff block the recovery will progress. However, it will fail because the source shard primary term
            // has
            // advanced
            handoffLatch.countDown();
            reshard.actionGet();
        },
            MetadataReshardIndexService.class,
            new MockLog.PatternSeenEventExpectation(
                "state transition failed",
                MetadataReshardIndexService.class.getCanonicalName(),
                Level.DEBUG,
                ".*\\["
                    + indexName
                    + "\\]\\[1\\] cannot transition target state \\["
                    + targetShardStateToDisrupt
                    + "\\] because target primary term advanced \\[.*"
            )
        );

        // After the target shard recovery tries again it will synchronize its primary term with the source and come online.
        ensureGreen(indexName);
    }

    public void testSplitTargetWillNotStartUntilHandoff() throws Exception {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client(indexNode).admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(1)
        );

        startIndexNode();

        MockTransportService mockTransportService = MockTransportService.getInstance(indexNode);
        CountDownLatch handoffAttemptedLatch = new CountDownLatch(1);
        CountDownLatch handoffLatch = new CountDownLatch(1);
        mockTransportService.addSendBehavior((connection, requestId, action, request1, options) -> {
            if (TransportReshardSplitAction.SPLIT_HANDOFF_ACTION_NAME.equals(action) && handoffAttemptedLatch.getCount() != 0) {
                try {
                    handoffAttemptedLatch.countDown();
                    handoffLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request1, options);
        });

        ReshardIndexRequest request = new ReshardIndexRequest(indexName, 2);
        ActionFuture<ReshardIndexResponse> reshard = client(indexNode).execute(TransportReshardAction.TYPE, request);

        handoffAttemptedLatch.await();

        ensureRed(indexName);

        // Allow handoff to proceed
        handoffLatch.countDown();
        reshard.actionGet();

        ensureGreen(indexName);

    }

    public void testSplitDoesNotTransitionToSplitUntilSearchShardsActive() throws Exception {
        String indexNode = startMasterAndIndexNode();
        startIndexNode();
        startSearchNode();

        ensureStableCluster(3);

        /* This allocation rule is used to prevent the new reshard index shard from getting allocated.
         * This will also prevent the search shard from getting allocated, hence we have two search nodes above.
         */
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        ReshardIndexRequest request = new ReshardIndexRequest(indexName, 2);
        ActionFuture<ReshardIndexResponse> executed = client(indexNode).execute(TransportReshardAction.TYPE, request);

        assertBusy(() -> {
            GetSettingsResponse postReshardSettingsResponse = client().admin()
                .indices()
                .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                .get();
            assertThat(
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
                equalTo(2)
            );
        });

        Index index = resolveIndex(indexName);

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).setMetadata(true).get().getState();
            final ProjectState projectState = state.projectState(state.metadata().projectFor(index).id());
            final IndexMetadata indexMetadata = projectState.metadata().getIndexSafe(index);
            final IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
            assertNotNull(reshardingMetadata);
            assertThat(reshardingMetadata.getSplit().getTargetShardState(1), equalTo(IndexReshardingState.Split.TargetShardState.HANDOFF));
        });

        final var searchShardExplain = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex(indexName)
            .setShard(1)
            .setPrimary(false);
        assertThat(
            client().execute(TransportClusterAllocationExplainAction.TYPE, searchShardExplain)
                .actionGet()
                .getExplanation()
                .getUnassignedInfo()
                .reason(),
            equalTo(UnassignedInfo.Reason.RESHARD_ADDED)
        );

        // Pause briefly to give chance for a buggy state transition to proceed
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));

        // Assert still handoff and has not transitioned to SPLIT
        ClusterState state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).setMetadata(true).get().getState();
        final ProjectState projectState = state.projectState(state.metadata().projectFor(index).id());
        final IndexMetadata indexMetadata = projectState.metadata().getIndexSafe(index);
        final IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
        assertNotNull(reshardingMetadata);
        assertThat(reshardingMetadata.getSplit().getTargetShardState(1), equalTo(IndexReshardingState.Split.TargetShardState.HANDOFF));

        // We have to clear the setting to prevent teardown issues with the cluster being red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), (String) null))
            .get();

        executed.actionGet();
    }

    public void testSplitWillTransitionToSplitIfSearchShardsActiveTimesOut() throws Exception {
        Settings settings = Settings.builder().put(SplitTargetService.RESHARD_SPLIT_SEARCH_SHARDS_ONLINE_TIMEOUT.getKey(), "200ms").build();
        String indexNode = startMasterAndIndexNode(settings);
        startIndexNode(settings);
        startSearchNode(settings);

        ensureStableCluster(3);

        /* This allocation rule is used to prevent the new reshard index shard from getting allocated.
         * This will also prevent the search shard from getting allocated, hence we have two search nodes above.
         */
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        ReshardIndexRequest request = new ReshardIndexRequest(indexName, 2);
        ActionFuture<ReshardIndexResponse> executed = client(indexNode).execute(TransportReshardAction.TYPE, request);

        assertBusy(() -> {
            GetSettingsResponse postReshardSettingsResponse = client().admin()
                .indices()
                .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                .get();
            assertThat(
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
                equalTo(2)
            );
        });

        // TODO: At this point we finish the reshard once all the target states are DONE. In the future we maybe need to switch this
        // assertion to reflect the usage of additional states
        executed.actionGet();

        final var searchShardExplain = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex(indexName)
            .setShard(1)
            .setPrimary(false);

        assertThat(
            client().execute(TransportClusterAllocationExplainAction.TYPE, searchShardExplain)
                .actionGet()
                .getExplanation()
                .getUnassignedInfo()
                .reason(),
            equalTo(UnassignedInfo.Reason.RESHARD_ADDED)
        );

        // We have to clear the setting to prevent teardown issues with the cluster being red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), (String) null))
            .get();
    }

    // only one resharding operation on a given index should be allowed to be in flight at a time

    public void testConcurrentReshardFails() throws Exception {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        // create index
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        checkNumberOfShardsSetting(indexNode, indexName, 1);

        // block allocation so that resharding will stall
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.enable", "none"));

        // start first resharding operation
        var splitState = waitForClusterState((state) -> state.projectState().metadata().index(indexName).getReshardingMetadata() != null);
        var reshardAction = client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2));

        // wait until we know it's in progress
        var ignored = splitState.actionGet(SAFE_AWAIT_TIMEOUT).projectState().metadata().index(indexName).getReshardingMetadata();

        // now start a second reshard, which should fail
        assertThrows(
            IllegalStateException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2))
                .actionGet(SAFE_AWAIT_TIMEOUT)
        );

        // unblock allocation to allow operations to proceed
        updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.enable"));

        reshardAction.actionGet(SAFE_AWAIT_TIMEOUT);

        checkNumberOfShardsSetting(indexNode, indexName, 2);

        // now we should be able to resplit
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet(SAFE_AWAIT_TIMEOUT);
        checkNumberOfShardsSetting(indexNode, indexName, 4);
    }

    private static long getCurrentPrimaryTerm(Index index, int shardId) {
        return client().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .setMetadata(true)
            .get()
            .getState()
            .getMetadata()
            .findIndex(index)
            .get()
            .primaryTerm(shardId);
    }

    private static long getIndexCount(IndicesStatsResponse statsResponse, int shardId) {
        ShardStats primaryStats = Arrays.stream(statsResponse.getShards())
            .filter(shardStat -> shardStat.getShardRouting().primary() && shardStat.getShardRouting().id() == shardId)
            .findAny()
            .get();
        return primaryStats.getStats().indexing.getTotal().getIndexCount();
    }

    private static void closeIndices(final String... indices) {
        closeIndices(indicesAdmin().prepareClose(indices));
    }

    private static void closeIndices(final CloseIndexRequestBuilder requestBuilder) {
        final CloseIndexResponse response = requestBuilder.get();
        assertThat(response.isAcknowledged(), is(true));
        assertThat(response.isShardsAcknowledged(), is(true));

        final String[] indices = requestBuilder.request().indices();
        if (indices != null) {
            assertThat(response.getIndices().size(), equalTo(indices.length));
            for (String index : indices) {
                CloseIndexResponse.IndexResult indexResult = response.getIndices()
                    .stream()
                    .filter(result -> index.equals(result.getIndex().getName()))
                    .findFirst()
                    .get();
                assertThat(indexResult, notNullValue());
                assertThat(indexResult.hasFailures(), is(false));
                assertThat(indexResult.getException(), nullValue());
                assertThat(indexResult.getShards(), notNullValue());
                Arrays.stream(indexResult.getShards()).forEach(shardResult -> {
                    assertThat(shardResult.hasFailures(), is(false));
                    assertThat(shardResult.getFailures(), notNullValue());
                    assertThat(shardResult.getFailures().length, equalTo(0));
                });
            }
        } else {
            assertThat(response.getIndices().size(), equalTo(0));
        }
    }

    private static void checkNumberOfShardsSetting(String indexNode, String indexName, int expected_shards) {
        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client(indexNode).admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(expected_shards)
        );
    }

    /**
     * A future that waits if necessary for cluster state to match a given predicate, and returns that state
     * @param predicate continue waiting for state updates until true
     * @return A future whose get() will resolve to the cluster state that matches the supplied predicate
     */
    private PlainActionFuture<ClusterState> waitForClusterState(Predicate<ClusterState> predicate) {
        var future = new PlainActionFuture<ClusterState>();
        var listener = new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                logger.info("cluster state updated: version {}", state.version());
                future.onResponse(state);
            }

            @Override
            public void onClusterServiceClose() {
                future.onFailure(null);
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                future.onFailure(new TimeoutException(timeout.toString()));
            }
        };

        ClusterStateObserver.waitForState(clusterService(), new ThreadContext(Settings.EMPTY), listener, predicate, null, logger);

        return future;
    }
}
