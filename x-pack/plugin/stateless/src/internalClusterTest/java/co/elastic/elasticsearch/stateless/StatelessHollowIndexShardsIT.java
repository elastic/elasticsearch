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

import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.HollowIndexEngine;
import co.elastic.elasticsearch.stateless.engine.HollowShardsMetrics;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;
import co.elastic.elasticsearch.stateless.recovery.TransportRegisterCommitForRecoveryAction;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS;
import static co.elastic.elasticsearch.stateless.engine.IndexEngineTestUtils.flushHollow;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class StatelessHollowIndexShardsIT extends AbstractStatelessIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        plugins.add(CustomIngestTestPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        plugins.add(MapperExtrasPlugin.class);
        plugins.add(MockRepository.Plugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            // TODO: ES-11519 Remove the explicit setting of the default values for TTLs here
            .put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMinutes(15))
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueDays(3))
            .put(DATA_STREAM_LIFECYCLE_POLL_INTERVAL, TimeValue.timeValueSeconds(1))
            .put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1")
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0);
    }

    public void testHollowIndexShardsEnabledSetting() {
        boolean hollowShardsEnabled = randomBoolean();
        final var indexingNode = startMasterAndIndexNode(
            Settings.builder().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), hollowShardsEnabled).build()
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexingNode);
        assertThat(hollowShardsService.isFeatureEnabled(), equalTo(hollowShardsEnabled));
    }

    public void testIsHollowableSystemIndex() {
        final var settings = Settings.builder()
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueNanos(1))
            .put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueNanos(1));
        final var indexingNode = startMasterAndIndexNode(settings.build());

        final String indexName = SYSTEM_INDEX_NAME;
        createSystemIndex(indexSettings(1, 0).build());
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 10));
        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexingNode);
        final var indexShard = findIndexShard(indexName);
        assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(false));
    }

    public void testIsHollowableRegularIndex() throws Exception {
        boolean lowTtl = randomBoolean();
        final var settings = Settings.builder();
        if (lowTtl) {
            settings.put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        if (randomBoolean()) {
            settings.put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        final var indexingNode = startMasterAndIndexNode(settings.build());

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 10));
        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexingNode);
        final var indexShard = findIndexShard(indexName);
        if (lowTtl) {
            assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(true)));
        } else {
            assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(false));
        }
    }

    public void testIsHollowableDataStreamWriteIndex() throws Exception {
        boolean lowTtl = randomBoolean();
        final var settings = Settings.builder();
        if (lowTtl) {
            settings.put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        if (randomBoolean()) {
            settings.put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        final var indexingNode = startMasterAndIndexNode(settings.build());
        startSearchNode();
        ensureStableCluster(2);

        createDataStreamWithMultipleBackingIndices("my-data-stream", false);
        final var writeIndex = getBackingIndices("my-data-stream", false).getLast();
        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexingNode);
        final var indexShard = findIndexShard(writeIndex.getName());
        if (lowTtl) {
            assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(true)));
        } else {
            assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(false));
        }
    }

    public void testIsHollowableFailureStoreWriteIndex() throws Exception {
        boolean lowTtl = randomBoolean();
        final var settings = Settings.builder();
        if (lowTtl) {
            settings.put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        if (randomBoolean()) {
            settings.put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        final var indexingNode = internalCluster().startNode(
            settingsForRoles(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.INGEST_ROLE).put(
                settings.build()
            )
        );
        startSearchNode();
        ensureStableCluster(2);

        createBasicPipeline("fail");
        createDataStreamWithMultipleBackingIndices("my-data-stream", true);
        indexDocsToDataStreamAndWaitForMultipleBackingIndices("my-data-stream", "fail");
        final var failureStoreWriteIndex = getBackingIndices("my-data-stream", true).getLast();

        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexingNode);
        final var indexShard = findIndexShard(failureStoreWriteIndex.getName());
        if (lowTtl) {
            assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(true)));
        } else {
            assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(false));
        }
    }

    public void testIsHollowableDataStreamNonWriteIndex() throws Exception {
        boolean lowTtl = randomBoolean();
        final var settings = Settings.builder();
        if (lowTtl) {
            settings.put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        if (randomBoolean()) {
            settings.put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        final var indexingNode = startMasterAndIndexNode(settings.build());
        startSearchNode();
        ensureStableCluster(2);

        createDataStreamWithMultipleBackingIndices("my-data-stream", false);
        final var nonWriteIndex = getBackingIndices("my-data-stream", false).getFirst();
        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexingNode);
        final var indexShard = findIndexShard(nonWriteIndex.getName());
        if (lowTtl) {
            assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(true)));
        } else {
            assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(false));
        }
    }

    public void testIsHollowableFailureStoreNonWriteIndex() throws Exception {
        boolean lowTtl = randomBoolean();
        final var settings = Settings.builder();
        if (lowTtl) {
            settings.put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        if (randomBoolean()) {
            settings.put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        final var indexingNode = internalCluster().startNode(
            settingsForRoles(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.INGEST_ROLE).put(
                settings.build()
            )
        );
        startSearchNode();
        ensureStableCluster(2);

        createBasicPipeline("fail");
        createDataStreamWithMultipleBackingIndices("my-data-stream", true);
        indexDocsToDataStreamAndWaitForMultipleBackingIndices("my-data-stream", "fail");
        final var failureStoreNonWriteIndex = getBackingIndices("my-data-stream", true).getFirst();

        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexingNode);
        final var indexShard = findIndexShard(failureStoreNonWriteIndex.getName());
        if (lowTtl) {
            assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(true)));
        } else {
            assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(false));
        }
    }

    public void testFlushHollowAndCompoundCommit() throws Exception {
        startMasterAndIndexNode();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        final var indexShard = findIndexShard(indexName);
        final var shardId = indexShard.shardId();
        final var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        final var statelessCommitService = indexEngine.getStatelessCommitService();

        indexDocs(indexName, randomIntBetween(1, 10));
        assertFalse(indexEngine.isLastCommitHollow());
        flush(indexName);
        assertFalse(indexEngine.isLastCommitHollow());
        assertFalse(statelessCommitService.getLatestUploadedBcc(shardId).lastCompoundCommit().hollow());

        indexDocs(indexName, randomIntBetween(1, 10));
        final PlainActionFuture<Engine.FlushResult> future = new PlainActionFuture<>();
        flushHollow(indexEngine, future);
        if (randomBoolean()) {
            indicesAdmin().prepareFlush(indexName).setForce(true).get(TimeValue.timeValueSeconds(10)); // competing flush
        }
        safeGet(future);
        assertTrue(indexEngine.isLastCommitHollow());
        assertTrue(statelessCommitService.getLatestUploadedBcc(shardId).lastCompoundCommit().hollow());
        assertFalse(indexEngine.refreshNeeded());

        var indexRequest = client().prepareIndex();
        indexRequest.setIndex(indexName);
        indexRequest.setSource("field", randomUnicodeOfCodepointLengthBetween(1, 25));
        Exception exception = expectThrows(Exception.class, () -> indexRequest.execute().get());
        Throwable cause = ExceptionsHelper.unwrapCause(exception.getCause());
        assertThat(cause, instanceOf(IllegalStateException.class));
        assertThat(cause.getMessage(), containsString("cannot ingest"));
    }

    private record HollowShardsInfo(
        int numberOfShards,
        Index index,
        String indexName,
        String indexNodeA,
        String indexNodeB,
        Settings indexNodeSettings
    ) {}

    private HollowShardsInfo startNodesAndHollowShards() throws Exception {
        startMasterOnlyNode();
        var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        String indexNodeA = startIndexNode(indexNodeSettings);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 0).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        indexDocs(indexName, randomIntBetween(16, 64));
        flush(indexName);
        var statelessCommitServiceA = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);
        var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
            assertFalse(indexEngine.isLastCommitHollow());
            assertFalse(statelessCommitServiceA.getLatestUploadedBcc(indexShard.shardId()).lastCompoundCommit().hollow());
            assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));
        }

        String indexNodeB = startIndexNode(indexNodeSettings);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        ensureGreen(indexName);

        var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, indexNodeB);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(HollowIndexEngine.class));
            hollowShardsServiceB.ensureHollowShard(indexShard.shardId(), true);
        }
        assertThat(
            getTotalLongUpDownCounterValue(HollowShardsMetrics.HOLLOW_SHARDS_TOTAL, getTelemetryPlugin(indexNodeB)),
            equalTo((long) numberOfShards)
        );
        return new HollowShardsInfo(numberOfShards, index, indexName, indexNodeA, indexNodeB, indexNodeSettings);
    }

    public void testRecoverHollowShard() throws Exception {
        var clusterInfo = startNodesAndHollowShards();

        logger.info("--> stopping node A");
        internalCluster().stopNode(clusterInfo.indexNodeA);
        ensureGreen(clusterInfo.indexName);

        logger.info("--> restarting node B");
        internalCluster().restartNode(clusterInfo.indexNodeB);
        ensureGreen(clusterInfo.indexName);

        for (int i = 0; i < clusterInfo.numberOfShards; i++) {
            final var indexShard = findIndexShard(clusterInfo.index, i);
            final var engine = indexShard.getEngineOrNull();
            assertThat(engine, instanceOf(HollowIndexEngine.class));
            internalCluster().getInstance(HollowShardsService.class, clusterInfo.indexNodeB).ensureHollowShard(indexShard.shardId(), true);
        }
    }

    public void testRecoverHollowShardsAsIndexShardsWithDisabledHollowing() throws Exception {
        var clusterInfo = startNodesAndHollowShards();

        // If we restart the node with the hollow shard feature flag disabled, shards should be initialized with an index engine
        internalCluster().stopNode(clusterInfo.indexNodeB);
        ensureStableCluster(2);
        String indexNodeC = startIndexNode(
            Settings.builder().put(clusterInfo.indexNodeSettings).put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), false).build()
        );
        ensureGreen(clusterInfo.indexName);
        var hollowShardsServiceC = internalCluster().getInstance(HollowShardsService.class, indexNodeC);
        for (int i = 0; i < clusterInfo.numberOfShards(); i++) {
            var indexShard = findIndexShard(clusterInfo.index, i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
            hollowShardsServiceC.ensureHollowShard(indexShard.shardId(), false);
        }
        assertThat(getTotalLongUpDownCounterValue(HollowShardsMetrics.HOLLOW_SHARDS_TOTAL, getTelemetryPlugin(indexNodeC)), equalTo(0L));
    }

    public void testIndexSettingsUpdateDoesNotUnhollow() throws Exception {
        var clusterInfo = startNodesAndHollowShards();

        var refreshInterval = randomTimeValue(5, 10, TimeUnit.SECONDS);
        updateIndexSettings(
            Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval),
            clusterInfo.indexName
        );
        ensureGreen(clusterInfo.indexName);
        var indexSettings = safeGet(indicesAdmin().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(clusterInfo.indexName)))
            .getIndexToSettings()
            .get(clusterInfo.indexName);
        assertThat(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.get(indexSettings), equalTo(refreshInterval));

        // If we updated an index setting, we shouldn't unhollow shards
        for (int i = 0; i < clusterInfo.numberOfShards(); i++) {
            assertThat(findIndexShard(clusterInfo.index(), i).getEngineOrNull(), instanceOf(HollowIndexEngine.class));
        }
        assertThat(
            getTotalLongUpDownCounterValue(HollowShardsMetrics.HOLLOW_SHARDS_TOTAL, getTelemetryPlugin(clusterInfo.indexNodeB())),
            equalTo((long) clusterInfo.numberOfShards())
        );
    }

    public void testRelocateHollowableShards() throws Exception {
        startMasterOnlyNode();
        final var indexNodeSettings = Settings.builder().put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1)).build();
        final var indexNodeA = startIndexNode(indexNodeSettings);
        final var indexNodeB = startIndexNode(indexNodeSettings);
        final var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        final var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, indexNodeB);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        indexDocs(indexName, between(20, 100));
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));
            hollowShardsServiceA.ensureHollowShard(indexShard.shardId(), false);
        }

        var telemetryPluginA = getTelemetryPlugin(indexNodeA);
        telemetryPluginA.collect();
        assertThat(getTotalLongCounterValue(HollowShardsMetrics.HOLLOW_SUCCESS_TOTAL, telemetryPluginA), equalTo(0L));
        assertThat(getTotalLongHistogramValue(HollowShardsMetrics.HOLLOW_TIME_MILLIS, telemetryPluginA), equalTo(0L));
        assertThat(getTotalLongUpDownCounterValue(HollowShardsMetrics.HOLLOW_SHARDS_TOTAL, telemetryPluginA), equalTo(0L));
        assertThat(getLastLongGaugeValue(HollowShardsMetrics.HOLLOWABLE_SHARDS_TOTAL, telemetryPluginA), equalTo((long) numberOfShards));

        logger.info("--> relocating {} hollowable shards from {} to {}", numberOfShards, indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        ensureGreen(indexName);
        logger.info("--> relocated");

        StatelessCommitService statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeB);
        for (int i = 0; i < numberOfShards; i++) {
            // After relocation, we produce a hollow blob and hollow shards switch to HollowIndexEngine
            var indexShard = findIndexShard(index, i);
            var engine = indexShard.getEngineOrNull();
            assertThat(engine, instanceOf(HollowIndexEngine.class));
            hollowShardsServiceB.ensureHollowShard(indexShard.shardId(), true);
            var commitAfterRelocation = statelessCommitService.getLatestUploadedBcc(indexShard.shardId()).lastCompoundCommit();
            assertTrue(commitAfterRelocation.hollow());
            assertThat(commitAfterRelocation.nodeEphemeralId(), is(emptyString()));
        }
        telemetryPluginA.collect();
        assertThat(getTotalLongCounterValue(HollowShardsMetrics.HOLLOW_SUCCESS_TOTAL, telemetryPluginA), equalTo((long) numberOfShards));
        assertThat(getTotalLongHistogramValue(HollowShardsMetrics.HOLLOW_TIME_MILLIS, telemetryPluginA), greaterThan(0L));
        assertThat(getTotalLongUpDownCounterValue(HollowShardsMetrics.HOLLOW_SHARDS_TOTAL, telemetryPluginA), equalTo(0L));
        assertThat(getLastLongGaugeValue(HollowShardsMetrics.HOLLOWABLE_SHARDS_TOTAL, telemetryPluginA), equalTo(0L));

        var telemetryPluginB = getTelemetryPlugin(indexNodeB);
        telemetryPluginB.collect();
        assertThat(
            getTotalLongUpDownCounterValue(HollowShardsMetrics.HOLLOW_SHARDS_TOTAL, telemetryPluginB),
            equalTo((long) numberOfShards)
        );
        assertThat(getLastLongGaugeValue(HollowShardsMetrics.HOLLOWABLE_SHARDS_TOTAL, telemetryPluginA), equalTo(0L));
    }

    public void testRelocateHollowShards() throws Exception {
        startMasterOnlyNode();
        final var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        var indexNodeA = startIndexNode(indexNodeSettings);
        var indexNodeB = startIndexNode(indexNodeSettings);
        final var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        final var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, indexNodeB);
        final var statelessCommitServiceA = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);
        final var statelessCommitServiceB = internalCluster().getInstance(StatelessCommitService.class, indexNodeB);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        indexDocs(indexName, between(20, 100));
        Map<ShardId, PrimaryTermAndGeneration> initialHollowPrimaryTermGenerations = new HashMap<>();
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
            assertFalse(indexEngine.isLastCommitHollow());
            assertFalse(statelessCommitServiceA.getLatestUploadedBcc(indexShard.shardId()).lastCompoundCommit().hollow());
            assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));
        }

        logger.info("--> relocating {} shards from {} to {}", numberOfShards, indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        ensureGreen(indexName);
        logger.info("--> relocated");

        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            var engine = indexShard.getEngineOrNull();
            assertThat(engine, instanceOf(HollowIndexEngine.class));
            hollowShardsServiceA.ensureHollowShard(indexShard.shardId(), false);
            hollowShardsServiceB.ensureHollowShard(indexShard.shardId(), true);

            initialHollowPrimaryTermGenerations.put(
                indexShard.shardId(),
                statelessCommitServiceB.getLatestUploadedBcc(indexShard.shardId()).lastCompoundCommit().primaryTermAndGeneration()
            );
        }
        var telemetryPluginA = getTelemetryPlugin(indexNodeA);
        assertThat(getTotalLongCounterValue(HollowShardsMetrics.HOLLOW_SUCCESS_TOTAL, telemetryPluginA), equalTo((long) numberOfShards));
        assertThat(getTotalLongUpDownCounterValue(HollowShardsMetrics.HOLLOW_SHARDS_TOTAL, telemetryPluginA), equalTo(0L));
        var telemetryPluginB = getTelemetryPlugin(indexNodeB);
        assertThat(
            getTotalLongUpDownCounterValue(HollowShardsMetrics.HOLLOW_SHARDS_TOTAL, telemetryPluginB),
            equalTo((long) numberOfShards)
        );

        // Try to relocate back hollow shards now initialized with `HollowIndexEngine`
        logger.info("--> relocating {} shards from {} to {}", numberOfShards, indexNodeB, indexNodeA);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeB), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeB))));
        ensureGreen(indexName);
        logger.info("--> relocated");

        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            var engine = indexShard.getEngineOrNull();
            assertThat(engine, instanceOf(HollowIndexEngine.class));
            hollowShardsServiceA.ensureHollowShard(indexShard.shardId(), true);
            hollowShardsServiceB.ensureHollowShard(indexShard.shardId(), false);

            // No extra flushes triggered on relocating hollow shards with `HollowIndexEngine`
            var commitAfterRelocationToNodeA = internalCluster().getInstance(StatelessCommitService.class, indexNodeA)
                .getLatestUploadedBcc(indexShard.shardId())
                .lastCompoundCommit();
            assertTrue(commitAfterRelocationToNodeA.hollow());
            assertThat(
                commitAfterRelocationToNodeA.primaryTermAndGeneration(),
                equalTo(initialHollowPrimaryTermGenerations.get(indexShard.shardId()))
            );
        }
        assertThat(getTotalLongCounterValue(HollowShardsMetrics.HOLLOW_SUCCESS_TOTAL, telemetryPluginA), equalTo((long) numberOfShards));
        assertThat(
            getTotalLongUpDownCounterValue(HollowShardsMetrics.HOLLOW_SHARDS_TOTAL, telemetryPluginA),
            equalTo((long) numberOfShards)
        );
        assertThat(getTotalLongUpDownCounterValue(HollowShardsMetrics.HOLLOW_SHARDS_TOTAL, telemetryPluginB), equalTo(0L));
    }

    public void testRegistrationOnBccWithLastHollowCommitAndDifferentPrimaryTerm() throws Exception {
        startMasterOnlyNode();
        final var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        var indexNodeA = startIndexNode(indexNodeSettings);
        var indexNodeB = startIndexNode(indexNodeSettings);
        final var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        final var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, indexNodeB);
        final var statelessCommitServiceA = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);

        // Make a commit that is not flushed yet, and wait until it is hollowable
        int numDocs = between(10, 30);
        indexDocs(indexName, numDocs);
        refresh(indexName);
        final var indexShard = findIndexShard(indexName);
        assertNotNull(statelessCommitServiceA.getCurrentVirtualBcc(indexShard.shardId()));
        var vbccGen = statelessCommitServiceA.getCurrentVirtualBcc(indexShard.shardId()).getPrimaryTermAndGeneration().generation();
        var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        assertFalse(indexEngine.isLastCommitHollow());
        assertFalse(statelessCommitServiceA.getLatestUploadedBcc(indexShard.shardId()).lastCompoundCommit().hollow());
        assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));

        // This test aims to create a BCC with multiple non-hollow commits and one last hollow commit.
        // Since a relocation does a first eager / optimistic flush before the second hollow flush, we first delay the first flush,
        // and during the delay we index more docs and refresh. Then we let the first flush go through, which unblocks the relocation
        // and finally flushes a hollow commit.
        var repository = ObjectStoreTestUtils.getObjectStoreMockRepository(getObjectStoreService(indexNodeA));
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setRandomIOExceptionPattern(".*stateless_commit_" + vbccGen + ".*");

        logger.info("--> relocating from {} to {}", indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);

        // Wait for first flush to be done
        assertBusy(() -> { assertNull(statelessCommitServiceA.getCurrentVirtualBcc(indexShard.shardId())); });

        // Input more CCs in VBCC
        var moreCommits = randomIntBetween(2, 5);
        logger.info("--> inputting more {} commits", moreCommits);
        for (int i = 0; i < moreCommits; i++) {
            indexDocs(indexName, numDocs);
            client().admin().indices().prepareRefresh(indexName).execute();
            int expectedCommits = i + 1;
            assertBusy(() -> {
                assertNotNull(statelessCommitServiceA.getCurrentVirtualBcc(indexShard.shardId()));
                assertThat(
                    statelessCommitServiceA.getCurrentVirtualBcc(indexShard.shardId()).getPendingCompoundCommits().size(),
                    equalTo(expectedCommits)
                );
            });
        }

        // Let relocation complete
        logger.info("--> let relocation complete");
        repository.setRandomControlIOExceptionRate(0.0);
        repository.setRandomDataFileIOExceptionRate(0.0);

        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        ensureGreen(indexName);
        logger.info("--> relocated");

        final var indexShardRelocated = findIndexShard(indexName);
        var engine = indexShardRelocated.getEngineOrNull();
        assertThat(engine, instanceOf(HollowIndexEngine.class));
        hollowShardsServiceA.ensureHollowShard(indexShardRelocated.shardId(), false);
        hollowShardsServiceB.ensureHollowShard(indexShardRelocated.shardId(), true);

        internalCluster().stopNode(indexNodeA);
        ensureGreen(indexName);

        logger.info("--> failing shard");
        indexShardRelocated.failShard("broken", new Exception("boom local shard"));
        ensureGreen(indexName);

        startSearchNode();
        logger.info("--> setting replica count");
        setReplicaCount(1, indexName);
        ensureGreen(indexName);
        hollowShardsServiceB.ensureHollowShard(indexShardRelocated.shardId(), true);
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs * (moreCommits + 1)));

        indexDocs(indexName, numDocs);
        refresh(indexName);
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs * (moreCommits + 2)));
    }

    public void testRefreshWithNewerOperationPrimaryTermThanOfHollowCommit() throws Exception {
        final var masterNode = startMasterOnlyNode();
        final var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        final var indexNodeA = startIndexNode(indexNodeSettings);
        final var indexNodeB = startIndexNode(indexNodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);

        hollowShards(indexName, 1, indexNodeA, indexNodeB);
        internalCluster().stopNode(indexNodeA);
        ensureGreen(indexName);
        final var shardId = findIndexShard(indexName).shardId();

        final long primaryTermBefore = getPrimaryTerms(client(), indexName)[0];
        final int primaryTermIncrements = randomIntBetween(1, 5);
        for (int i = 0; i < primaryTermIncrements; i++) {
            // Infrastructure to be able to wait until the primary term increases and the allocation ID changes
            final int finalI = i;
            final var allocationIdBefore = findIndexShard(indexName).routingEntry().allocationId().getId();
            ClusterService clusterService = internalCluster().clusterService(masterNode);
            CountDownLatch primaryShardReassigned = new CountDownLatch(1);
            clusterService.addListener(new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    try {
                        final var clusterState = event.state();

                        boolean primaryTermIncreased = false;
                        final var indexMetadata = clusterState.metadata().getProject(ProjectId.DEFAULT).index(indexName);
                        if (indexMetadata != null && indexMetadata.getNumberOfShards() > 0) {
                            final long primaryTerm = indexMetadata.primaryTerm(shardId.id());
                            primaryTermIncreased = primaryTerm > primaryTermBefore + finalI;
                        }

                        boolean allocationIdChanged = false;
                        final var routingTable = clusterState.routingTable(ProjectId.DEFAULT);
                        if (routingTable.hasIndex(indexName)) {
                            final var shardRoutingTable = routingTable.index(indexName).shard(shardId.id());
                            if (shardRoutingTable != null) {
                                final var primaryShardRouting = shardRoutingTable.primaryShard();
                                if (primaryShardRouting != null) {
                                    final var primaryAllocationId = primaryShardRouting.allocationId();
                                    allocationIdChanged = primaryAllocationId != null
                                        && primaryAllocationId.getId().equals(allocationIdBefore) == false;
                                }
                            }
                        }

                        if (primaryTermIncreased && allocationIdChanged) {
                            primaryShardReassigned.countDown();
                            clusterService.removeListener(this);
                        }
                    } catch (Exception e) {
                        logger.error("unexpected exception", e);
                        assert false : "unexpected exception : " + e;
                        clusterService.removeListener(this);
                    }
                }
            });

            // Increase primary term either by failing shard or restarting the index node
            if (randomBoolean()) {
                logger.debug("--> failing shard " + (i + 1) + "/" + primaryTermIncrements);
                findIndexShard(indexName).failShard("broken", new Exception("boom local shard"));
            } else {
                logger.debug("--> restarting " + indexNodeB + " " + (i + 1) + "/" + primaryTermIncrements);
                internalCluster().restartNode(indexNodeB);
            }

            // Wait for primary term increase
            safeAwait(primaryShardReassigned);
            ensureGreen(indexName);
        }

        startSearchNode();
        logger.debug("--> setting replica count");
        setReplicaCount(1, indexName);
        ensureGreen(indexName);

        logger.debug("--> refreshing");
        refresh(indexName);
    }

    public void testRelocateHollowableShardWithConnectionFailure() throws Exception {
        startMasterOnlyNode();
        var indexNodeSettings = Settings.builder().put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1)).build();
        var indexNodeA = startIndexNode(indexNodeSettings);
        var indexNodeB = startIndexNode(indexNodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        var numDocs = between(20, 100);
        indexDocs(indexName, numDocs);
        var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        final var indexShard = findIndexShard(index, 0);
        assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
        assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(true)));

        final boolean failStartElseHandoff = randomBoolean();
        var relocationFailedLatch = breakRelocation(
            indexNodeA,
            indexNodeB,
            failStartElseHandoff ? START_RELOCATION_ACTION_NAME : PRIMARY_CONTEXT_HANDOFF_ACTION_NAME
        );
        logger.info("--> relocating hollowable shard from {} to {}", indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        safeAwait(relocationFailedLatch);

        ensureGreen(indexName);
        assertNodeHasNoCurrentRecoveries(indexNodeB);
        assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeB)));

        final var engine = indexShard.getEngineOrNull();
        var telemetryPlugin = getTelemetryPlugin(indexNodeA);
        if (failStartElseHandoff) {
            // If a primary relocation fails before the hollow flush, e.g., due to node disconnection during the start message exchange, the
            // engine on the source node will be left to IndexEngine.
            assertThat(engine, instanceOf(IndexEngine.class));
            assertFalse(((IndexEngine) engine).isLastCommitHollow());
            hollowShardsService.ensureHollowShard(indexShard.shardId(), false);
            assertThat(getTotalLongCounterValue(HollowShardsMetrics.HOLLOW_SUCCESS_TOTAL, telemetryPlugin), equalTo(0L));
        } else {
            // If a primary relocation fails after the hollow flush, e.g., due to node disconnection during the handoff exchange, the
            // engine on the source node will have been reset to HollowIndexEngine and stay like that.
            assertThat(engine, instanceOf(HollowIndexEngine.class));
            hollowShardsService.ensureHollowShard(indexShard.shardId(), true);
            assertThat(getTotalLongCounterValue(HollowShardsMetrics.HOLLOW_SUCCESS_TOTAL, telemetryPlugin), equalTo(1L));
        }

        indexDocsAndRefresh(indexName, numDocs);
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs * 2));
    }

    public void testRelocateHollowableShardWithLingeringIngestionUponConnectionFailure() throws Exception {
        startMasterOnlyNode();
        var indexNodeSettings = Settings.builder()
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .build();
        var indexNodeA = startIndexNode(indexNodeSettings);
        var statelessCommitServiceA = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        var numDocs = between(20, 100);
        logger.debug("--> indexing {} docs", numDocs);
        indexDocs(indexName, numDocs);
        flush(indexName);
        var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        final var indexShard = findIndexShard(index, 0);
        assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
        assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(true)));

        // We would like to make the relocation flush stuck due to object store failures so we enable failures only for the new generation.
        // Later, while the flush keeps repeating the upload, we issue the ingestion that will linger until the relocation failure.
        long newGen = statelessCommitServiceA.getMaxGenerationToUploadForFlush(indexShard.shardId()) + 1;
        var repository = ObjectStoreTestUtils.getObjectStoreMockRepository(getObjectStoreService(indexNodeA));
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setRandomIOExceptionPattern(".*stateless_commit_" + newGen + ".*");

        var indexNodeB = startIndexNode(indexNodeSettings);
        ensureStableCluster(3);
        logger.debug("--> relocating hollowable shard from {} to {}", indexNodeA, indexNodeB);

        // Relocation is stuck waiting for the flushed commit to upload (which keeps retrying due to object store failures)
        client().execute(
            TransportClusterRerouteAction.TYPE,
            new ClusterRerouteRequest(TimeValue.timeValueSeconds(10), TimeValue.timeValueSeconds(10)).add(
                new MoveAllocationCommand(indexName, indexShard.shardId().id(), indexNodeA, indexNodeB)
            )
        );

        // Wait until the hollow flushed commit appears for upload
        assertBusy(() -> assertThat(statelessCommitServiceA.getMaxGenerationToUploadForFlush(indexShard.shardId()), equalTo(newGen)));

        // Index more docs, which will complete after the relocation failure and after unhollowing the shard
        logger.debug("--> indexing {} docs", numDocs);
        var bulkRequest = client(indexNodeA).prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        var bulkFuture = bulkRequest.execute();

        // Stop target node to disrupt relocation
        internalCluster().stopNode(indexNodeB);

        ensureGreen(indexName);
        repository.setRandomControlIOExceptionRate(0.0);
        repository.setRandomDataFileIOExceptionRate(0.0);
        assertNoFailures(bulkFuture.get());

        logger.debug("--> indexing {} docs", numDocs);
        indexDocs(indexName, numDocs);

        // The lingering ingestion was blocked on primary permits until the relocation failure, after which it was blocked on the ingestion
        // blocker which initiated unhollowing and reset the engine to IndexEngine, after which ingestion was processed.
        // So, verify that all ingested docs are searchable.
        logger.debug("--> starting a search node");
        startSearchNode();
        setReplicaCount(1, indexName);
        ensureGreen(indexName);
        logger.debug("--> indexing {} docs", numDocs);
        indexDocsAndRefresh(indexName, numDocs);
        assertHitCount(client().prepareSearch(indexName).setSize(0).setTrackTotalHits(true), numDocs * 4);

        // Restart node to ensure that translog recovery works
        logger.debug("--> restarting node " + indexNodeA);
        internalCluster().restartNode(indexNodeA);
        ensureGreen(indexName);
        long translogRecoveredOps = indicesAdmin().prepareRecoveries(indexName)
            .get()
            .shardRecoveryStates()
            .get(indexName)
            .stream()
            .mapToLong(e -> e.getTranslog().recoveredOperations())
            .sum();
        assertThat(translogRecoveredOps, equalTo((long) numDocs * 3));
        assertHitCount(client().prepareSearch(indexName).setSize(0).setTrackTotalHits(true), numDocs * 4);
    }

    public void testFlushesOnHollowShards() throws Exception {
        startMasterOnlyNode();
        final var indexNodes = startIndexNodes(
            2,
            Settings.builder()
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.ZERO)
                .build()
        );
        ensureStableCluster(3);

        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", indexNodes.getLast()).build());
        final var index = resolveIndex(indexName);
        ensureGreen(indexName);

        int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        var handoffFuture = new PlainActionFuture<StatelessCompoundCommit>();
        MockTransportService.getInstance(indexNodes.getFirst()).addSendBehavior((connection, requestId, action, request, options) -> {
            if (PRIMARY_CONTEXT_HANDOFF_ACTION_NAME.equals(action)) {
                ActionListener.completeWith(
                    handoffFuture,
                    () -> internalCluster().getInstance(StatelessCommitService.class, indexNodes.getFirst())
                        .getLatestUploadedBcc(new ShardId(index, 0))
                        .lastCompoundCommit()
                );
            }
            connection.sendRequest(requestId, action, request, options);
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodes.getFirst()));
        final var hollowCommit = safeGet(handoffFuture);
        final var hollowCommitFiles = Set.copyOf(hollowCommit.commitFiles().keySet());
        ensureGreen(indexName);

        var relocatedShard = findIndexShard(resolveIndex(indexName), 0);
        var relocatedEngine = getShardEngine(relocatedShard, HollowIndexEngine.class);
        assertThat(Set.of(relocatedShard.store().directory().listAll()), equalTo(hollowCommitFiles));
        assertThat(relocatedEngine.getLastCommittedSegmentInfos().files(true), equalTo(hollowCommitFiles));

        final var threads = new Thread[randomIntBetween(2, 5)];
        final var flushes = new AtomicLong(threads.length * randomLongBetween(2L, 5L));
        final var latch = new CountDownLatch(threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    while (flushes.decrementAndGet() > 0) {
                        var response = safeGet(indicesAdmin().prepareFlush(indexName).execute());
                        assertAllSuccessful(response);

                        var hollowShard = findIndexShard(resolveIndex(indexName), 0);
                        var hollowEngine = getShardEngine(hollowShard, HollowIndexEngine.class);
                        assertThat(Set.of(hollowShard.store().directory().listAll()), equalTo(hollowCommitFiles));
                        assertThat(hollowEngine.getLastCommittedSegmentInfos().files(true), equalTo(hollowCommitFiles));

                        safeSleep(randomLongBetween(100, 500));
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    latch.countDown();
                }
            });
            threads[i].start();
        }

        safeAwait(latch);

        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(flushes.get(), lessThanOrEqualTo(0L));
        assertThat(Set.of(relocatedShard.store().directory().listAll()), equalTo(hollowCommitFiles));
        assertThat(relocatedEngine.getLastCommittedSegmentInfos().files(true), equalTo(hollowCommitFiles));
    }

    public void testRefreshesOnHollowShards() throws Exception {
        startMasterOnlyNode();
        final var indexNodes = startIndexNodes(
            2,
            Settings.builder()
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.ZERO)
                .build()
        );
        final var searchNodes = startSearchNodes(2);
        ensureStableCluster(5);

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, randomIntBetween(1, 2)).put("index.routing.allocation.exclude._name", indexNodes.getLast()).build()
        );
        final var index = resolveIndex(indexName);
        ensureGreen(indexName);

        int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        var handoffFuture = new PlainActionFuture<StatelessCompoundCommit>();
        MockTransportService.getInstance(indexNodes.getFirst()).addSendBehavior((connection, requestId, action, request, options) -> {
            if (PRIMARY_CONTEXT_HANDOFF_ACTION_NAME.equals(action)) {
                ActionListener.completeWith(
                    handoffFuture,
                    () -> internalCluster().getInstance(StatelessCommitService.class, indexNodes.getFirst())
                        .getLatestUploadedBcc(new ShardId(index, 0))
                        .lastCompoundCommit()
                );
            }
            connection.sendRequest(requestId, action, request, options);
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodes.getFirst()));
        final var hollowCommit = safeGet(handoffFuture);
        final var hollowCommitFiles = Set.copyOf(hollowCommit.commitFiles().keySet());
        ensureGreen(indexName);

        var relocatedShard = findIndexShard(resolveIndex(indexName), 0);
        var relocatedEngine = getShardEngine(relocatedShard, HollowIndexEngine.class);
        assertThat(Set.of(relocatedShard.store().directory().listAll()), equalTo(hollowCommitFiles));
        assertThat(relocatedEngine.getLastCommittedSegmentInfos().files(true), equalTo(hollowCommitFiles));

        final var threads = new Thread[randomIntBetween(2, 5)];
        final var refreshes = new AtomicLong(threads.length * randomLongBetween(2L, 5L));
        final var latch = new CountDownLatch(threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    while (refreshes.decrementAndGet() > 0) {
                        var response = safeGet(indicesAdmin().prepareRefresh(indexName).execute());
                        assertAllSuccessful(response);

                        var hollowShard = findIndexShard(resolveIndex(indexName), 0);
                        var hollowEngine = getShardEngine(hollowShard, HollowIndexEngine.class);
                        assertThat(Set.of(hollowShard.store().directory().listAll()), equalTo(hollowCommitFiles));
                        assertThat(hollowEngine.getLastCommittedSegmentInfos().files(true), equalTo(hollowCommitFiles));

                        safeSleep(randomLongBetween(100, 500));
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    latch.countDown();
                }
            });
            threads[i].start();
        }

        safeAwait(latch);

        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(refreshes.get(), lessThanOrEqualTo(0L));
        assertThat(Set.of(relocatedShard.store().directory().listAll()), equalTo(hollowCommitFiles));
        assertThat(relocatedEngine.getLastCommittedSegmentInfos().files(true), equalTo(hollowCommitFiles));
    }

    private CountDownLatch breakRelocation(String nodeA, String nodeB, String recoveryActionToBlock) {
        var relocationFailed = new CountDownLatch(1);
        MockTransportService.getInstance(nodeA).addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel, task) -> {
            relocationFailed.countDown();
            channel.sendResponse(new ElasticsearchException("Unable to perform " + recoveryActionToBlock + " on node " + nodeA));
        });
        MockTransportService.getInstance(nodeB).addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel, task) -> {
            relocationFailed.countDown();
            channel.sendResponse(new ElasticsearchException("Unable to perform " + recoveryActionToBlock + " on node " + nodeB));
        });
        return relocationFailed;
    }

    private void stopBreakingRelocation(String nodeA, String nodeB) {
        MockTransportService.getInstance(nodeA).clearAllRules();
        MockTransportService.getInstance(nodeB).clearAllRules();
    }

    public void testUnhollowOnIndexing() throws Exception {
        unhollowOnIngestion(IngestionType.Index);
    }

    public void testUnhollowOnUpdates() throws Exception {
        unhollowOnIngestion(IngestionType.Update);
    }

    public void testUnhollowOnBulkUpdates() throws Exception {
        unhollowOnIngestion(IngestionType.BulkUpdate);
    }

    private enum IngestionType {
        Index,
        Update,
        BulkUpdate
    }

    private void unhollowOnIngestion(IngestionType ingestionType) throws Exception {
        startMasterOnlyNode();
        var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // Disable upload triggered by number of commits and size
            .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        var indexNodeA = startIndexNode(indexNodeSettings);
        var indexNodeB = startIndexNode(indexNodeSettings);
        ensureStableCluster(3);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        final var docsIdsGenerator = new AtomicLong();
        final Supplier<String> docIdSupplier = () -> Long.toHexString(docsIdsGenerator.getAndIncrement());
        var bulkResponse = indexDocs(indexName, between(64, 128), docIdSupplier); // need enough docs for ingesting into all shards
        var docsIds = Arrays.stream(bulkResponse.getItems()).map(BulkItemResponse::getId).collect(Collectors.toCollection(HashSet::new));

        flush(indexName);
        bulkResponse = indexDocs(indexName, between(64, 128), docIdSupplier); // need enough docs for ingesting into all shards
        Arrays.stream(bulkResponse.getItems()).forEach(item -> docsIds.add(item.getId()));
        var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));
            hollowShardsServiceA.ensureHollowShard(indexShard.shardId(), false);
        }

        logger.info("--> relocating {} hollowable shards from {} to {}", numberOfShards, indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        ensureGreen(indexName);
        logger.info("--> relocated");

        // Hollow shards on relocation
        var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, indexNodeB);
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeB);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(HollowIndexEngine.class));
            hollowShardsServiceB.ensureHollowShard(indexShard.shardId(), true);
        }
        var telemetryPluginA = getTelemetryPlugin(indexNodeA);
        assertThat(getTotalLongCounterValue(HollowShardsMetrics.HOLLOW_SUCCESS_TOTAL, telemetryPluginA), equalTo((long) numberOfShards));
        var telemetryPluginB = getTelemetryPlugin(indexNodeB);
        assertThat(getTotalLongCounterValue(HollowShardsMetrics.UNHOLLOW_SUCCESS_TOTAL, telemetryPluginB), equalTo(0L));
        assertThat(getTotalLongHistogramValue(HollowShardsMetrics.UNHOLLOW_TIME_MILLIS, telemetryPluginB), equalTo(0L));

        // Try to inject documents from background threads, the shards should unhollow on first ingestion
        int ingestingThreads = randomIntBetween(1, 8);
        var ingestExecutor = Executors.newFixedThreadPool(ingestingThreads);
        try {
            List<Long> generationsBeforeUnhollow = IntStream.range(0, numberOfShards)
                .mapToObj(i -> statelessCommitService.getLatestUploadedBcc(new ShardId(index, i)).lastCompoundCommit().generation())
                .toList();
            var ingestLatch = new CountDownLatch(ingestingThreads);
            for (int i = 0; i < ingestingThreads; i++) {
                Runnable ingestRunnable = switch (ingestionType) {
                    // Index docs
                    case Index -> () -> {
                        try {
                            indexDocs(indexName, randomIntBetween(64, 128)); // need enough ops to ensure unhollowing all shards
                        } finally {
                            ingestLatch.countDown();
                        }
                    };
                    // Update doc or Upsert new doc
                    case Update -> () -> {
                        try {
                            for (int j = 0; j < Math.min(docsIds.size(), 128); j++) { // need enough updates to be sure to hollow every
                                                                                      // shard
                                final var upsertOrUpdate = randomBoolean();
                                var docId = upsertOrUpdate ? docIdSupplier.get() : randomFrom(docsIds);
                                var response = client().prepareUpdate(indexName, docId)
                                    .setDoc(frequently() ? "field" : "field_" + docId, randomUnicodeOfLength(10))
                                    .setDocAsUpsert(upsertOrUpdate)
                                    .get();
                                assertThat(
                                    response.getResult(),
                                    equalTo(upsertOrUpdate ? DocWriteResponse.Result.CREATED : DocWriteResponse.Result.UPDATED)
                                );
                                assertThat(response.getId(), equalTo(docId));
                            }
                        } finally {
                            ingestLatch.countDown();
                        }
                    };
                    // Bulk update docs
                    case BulkUpdate -> () -> {
                        try {
                            var client = client();
                            var bulkUpdates = client.prepareBulk();
                            for (int j = 0; j < Math.min(docsIds.size(), 128); j++) { // need enough updates to be sure to hollow every
                                                                                      // shard
                                var docId = randomFrom(docsIds);
                                bulkUpdates.add(client.prepareUpdate(indexName, docId).setDoc("field", randomUnicodeOfLength(10)));
                            }
                            assertNoFailures(bulkUpdates.get());
                        } finally {
                            ingestLatch.countDown();
                        }
                    };
                    default -> throw new AssertionError("Unexpected value");
                };
                ingestExecutor.submit(ingestRunnable);
            }
            safeAwait(ingestLatch, TimeValue.THIRTY_SECONDS);
            for (int i = 0; i < numberOfShards; i++) {
                // Should unhollow only once
                assertThat(
                    statelessCommitService.getLatestUploadedBcc(new ShardId(index, i)).lastCompoundCommit().generation(),
                    equalTo(generationsBeforeUnhollow.get(i) + 1)
                );
            }
        } finally {
            terminate(ingestExecutor);
        }

        // Check that shards switched to the index engine and flushed a blob with a new translog node ID
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            var engine = indexShard.getEngineOrNull();
            var indexEngine = asInstanceOf(IndexEngine.class, engine);
            assertFalse(indexEngine.isLastCommitHollow());
            checkLastCommitIsNotHollow(indexNodeB, indexShard.shardId());
        }
        assertThat(getTotalLongCounterValue(HollowShardsMetrics.UNHOLLOW_SUCCESS_TOTAL, telemetryPluginB), equalTo((long) numberOfShards));
        assertThat(getTotalLongHistogramValue(HollowShardsMetrics.UNHOLLOW_TIME_MILLIS, telemetryPluginB), greaterThan(0L));

        // We can continue inject new documents
        int docs = randomIntBetween(16, 64);
        var response = indexDocs(indexName, docs);

        // Updating also works
        List<String> docIds = Arrays.stream(response.getItems()).map(BulkItemResponse::getId).limit(16).toList();
        for (String id : randomSubsetOf(docIds)) {
            var updateResponse = safeGet(
                client().prepareUpdate(indexName, id)
                    .setDoc(Requests.INDEX_CONTENT_TYPE, "field", randomUnicodeOfCodepointLengthBetween(1, 25))
                    .execute()
            );
            assertThat(updateResponse.status(), equalTo(RestStatus.OK));
            assertThat(updateResponse.getResult(), equalTo(DocWriteResponse.Result.UPDATED));
            assertThat(updateResponse.getVersion(), equalTo(2L));
        }

        // And deleting
        for (String id : randomSubsetOf(docIds)) {
            var deleteResponse = safeGet(client().prepareDelete(indexName, id).execute());
            assertThat(deleteResponse.status(), equalTo(RestStatus.OK));
            assertThat(deleteResponse.getResult(), equalTo(DocWriteResponse.Result.DELETED));
        }
    }

    public void testRecoverUnhollowedShardFromTranslog() throws Exception {
        startMasterOnlyNode();
        var indexNodeSettings = Settings.builder()
            // Disable upload triggered by number of commits and size
            .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        var indexNodeA = startIndexNode(indexNodeSettings);
        var indexNodeB = startIndexNode(indexNodeSettings);
        ensureStableCluster(3);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        int docs1 = between(16, 128);
        indexDocs(indexName, docs1);
        flush(indexName);
        int docs2 = between(16, 128);
        indexDocs(indexName, docs2);
        var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));
            hollowShardsServiceA.ensureHollowShard(indexShard.shardId(), false);
        }

        // Hollow shards by relocating them
        logger.info("--> relocating {} hollowable shards from {} to {}", numberOfShards, indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        ensureGreen(indexName);
        logger.info("--> relocated");
        var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, indexNodeB);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(HollowIndexEngine.class));
            hollowShardsServiceB.ensureHollowShard(indexShard.shardId(), true);
        }

        // Trigger unhollowing
        int docs3 = randomIntBetween(128, 256);
        indexDocs(indexName, docs3);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
            assertFalse(((IndexEngine) indexShard.getEngineOrNull()).isLastCommitHollow());
        }

        var statelessCommitServiceB = internalCluster().getInstance(StatelessCommitService.class, indexNodeB);
        List<Long> generationsBeforeRestart = IntStream.range(0, numberOfShards)
            .mapToObj(i -> statelessCommitServiceB.getLatestUploadedBcc(new ShardId(index, i)).lastCompoundCommit().generation())
            .toList();

        int docs4 = 0;
        if (randomBoolean()) {
            docs4 = randomIntBetween(64, 128);
            indexDocs(indexName, docs4);
        }

        // We should recover non flushed data from the translog
        logger.info("--> restarting node " + indexNodeB);
        internalCluster().restartNode(indexNodeB);
        ensureStableCluster(3);
        ensureGreen(indexName);
        assertNodeHasNoCurrentRecoveries(indexNodeB);
        assertThat(
            indicesAdmin().prepareStats(indexName).setDocs(true).get().getTotal().getDocs().getCount(),
            equalTo((long) docs1 + docs2 + docs3 + docs4)
        );
        long translogRecoveredOps = indicesAdmin().prepareRecoveries(indexName)
            .get()
            .shardRecoveryStates()
            .get(indexName)
            .stream()
            .mapToLong(e -> e.getTranslog().recoveredOperations())
            .sum();
        assertThat(translogRecoveredOps, equalTo((long) docs3 + docs4));

        var newStatelessCommitServiceB = internalCluster().getInstance(StatelessCommitService.class, indexNodeB);
        for (int i = 0; i < numberOfShards; i++) {
            var lastCompoundCommit = newStatelessCommitServiceB.getLatestUploadedBcc(new ShardId(index, i)).lastCompoundCommit();
            assertFalse(lastCompoundCommit.hollow());
            long generationBeforeRestart = generationsBeforeRestart.get(i);
            assertThat(lastCompoundCommit.generation(), either(is(generationBeforeRestart)).or(is(generationBeforeRestart + 1)));
        }
    }

    public void testCompletionStatsOnUnhollowedShard() throws Exception {
        final var indexNodeSettings = Settings.builder().put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1)).build();
        final var indexNodeA = startMasterAndIndexNode(indexNodeSettings);
        final var indexNodeB = startMasterAndIndexNode(indexNodeSettings);
        final var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        final var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, indexNodeB);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        var numDocs = randomIntBetween(20, 100);
        indexDocs(indexName, numDocs);
        var indexShard = findIndexShard(index, 0);
        assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));
        hollowShardsServiceA.ensureHollowShard(indexShard.shardId(), false);

        logger.debug("--> relocating hollowable shard from {} to {}", indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        ensureGreen(indexName);
        logger.debug("--> relocated");

        // Initiate unhollow on the background
        logger.debug("--> indexing {} docs", numDocs);
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        var bulkFuture = bulkRequest.execute();

        // Now get completion stats while unhollowing
        var statsResponse = indicesAdmin().prepareStats(indexName).setIndices(indexName).setCompletion(true).get(TEST_REQUEST_TIMEOUT);
        CompletionStats completionStats = statsResponse.getIndex(indexName).getPrimaries().completion;
        assertThat(completionStats, notNullValue());

        ensureGreen(indexName);
        assertNoFailures(bulkFuture.get());
    }

    public void testHollowShardFailsIfSearchShardRegistersNewerCommit() throws Exception {
        var nodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
            .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "5s")
            .build();
        String masterNode = startMasterOnlyNode(nodeSettings);
        String searchNode = startSearchNode(nodeSettings);
        String indexNodeA = startIndexNode(nodeSettings);

        // In the following, create a hollow shard by relocating from node A to node B.
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        int numDocs = randomIntBetween(16, 64);
        indexDocs(indexName, numDocs);
        flush(indexName);
        var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        var commitServiceA = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);
        final var indexShardA = findIndexShard(indexName);
        assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShardA), equalTo(true)));

        String indexNodeB = startIndexNode(nodeSettings);
        var commitServiceB = internalCluster().getInstance(StatelessCommitService.class, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        ensureGreen(indexName);

        var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, indexNodeB);
        final var indexShardB = findIndexShard(indexName);
        hollowShardsServiceB.ensureHollowShard(indexShardB.shardId(), true);

        final var termGenHollow = commitServiceB.getLatestUploadedBcc(indexShardB.shardId())
            .lastCompoundCommit()
            .primaryTermAndGeneration();

        // Ingest to node B directly so it initiates unhollowing to flush a new unhollow generation to object store.
        // But do not let the unhollow commit be uploaded until node B is isolated.
        // The bulk request will not be acknowledged since the translog upload will be stalled as the node is isolated.
        var repositoryB = ObjectStoreTestUtils.getObjectStoreMockRepository(getObjectStoreService(indexNodeB));
        repositoryB.setRandomControlIOExceptionRate(1.0);
        repositoryB.setRandomDataFileIOExceptionRate(1.0);
        repositoryB.setRandomIOExceptionPattern(".*stateless_commit_" + (termGenHollow.generation() + 1) + ".*");
        var bulkRequest = client(indexNodeB).prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        var bulkFuture = bulkRequest.execute();

        // Isolate node B
        Set<String> isolatedSide = Collections.singleton(indexNodeB);
        Set<String> restOfClusterSide = Set.of(masterNode, indexNodeA, searchNode);
        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(isolatedSide, restOfClusterSide),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        // Wait until node B is removed from the cluster
        ensureStableCluster(3, masterNode);

        // Allow allocation on index node A
        assertAcked(
            client(indexNodeA).admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", (String) null))
                .get()
        );

        // Wait until index shard is allocated on node A
        ensureGreenViaMasterNode(masterNode, indexName, true);

        // Let the unhollowing complete and wait until the dirty documents are ingested on the isolated node B
        repositoryB.setRandomControlIOExceptionRate(0.0);
        repositoryB.setRandomDataFileIOExceptionRate(0.0);
        assertBusy(() -> { assertThat(indexShardB.docStats().getCount(), equalTo((long) numDocs * 2)); });

        // Flush as well to ensure that the dirty unacknowledged documents are uploaded to the object store
        client(indexNodeB).admin().indices().prepareFlush(indexName).setForce(true).execute();

        // Wait until the unhollow generation and the generation with the new documents appear on the object store
        assertBusy(() -> {
            assertThat(
                commitServiceB.getLatestUploadedBcc(indexShardB.shardId()).lastCompoundCommit().primaryTermAndGeneration().generation(),
                greaterThanOrEqualTo(termGenHollow.generation() + 2)
            );
        });

        final var termGenUnhollow = commitServiceB.getLatestUploadedBcc(indexShardB.shardId())
            .lastCompoundCommit()
            .primaryTermAndGeneration();

        // Create some random delay between the shard failure message and returning the RecoveryCommitTooNewException exception to the
        // search shard. Just so we cover both cases where one of these happens first.
        MockTransportService.getInstance(indexNodeA)
            .addRequestHandlingBehavior(
                TransportRegisterCommitForRecoveryAction.NAME,
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public void sendResponse(TransportResponse response) {
                        safeSleep(randomInt(100));
                        channel.sendResponse(response);
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        safeSleep(randomInt(100));
                        channel.sendResponse(exception);
                    }

                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }
                }, task)
            );
        MockTransportService.getInstance(indexNodeA).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(ShardStateAction.SHARD_FAILED_ACTION_NAME)) {
                safeSleep(randomInt(100));
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // Start the search shard
        hollowShardsServiceA.ensureHollowShard(indexShardA.shardId(), true);
        assertAcked(
            client(indexNodeA).admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
                .get()
        );

        // Wait for the index and search shard to be finally green
        ensureGreenViaMasterNode(masterNode, indexName, true);

        // The shard on node A should have been failed, since the search shard tried to register a newer commit, and prompted the
        // shard on node A to reload the latest unhollow commit from the blob store, flushing a new commit in the latest primary term.
        hollowShardsServiceA.ensureHollowShard(indexShardA.shardId(), false);
        assertThat(
            commitServiceA.getLatestUploadedBcc(indexShardA.shardId()).lastCompoundCommit().primaryTermAndGeneration().primaryTerm(),
            greaterThan(termGenUnhollow.primaryTerm())
        );
        assertThat(findIndexShard(indexName).getOperationPrimaryTerm(), greaterThan(termGenUnhollow.primaryTerm()));

        // The new commit should include the dirty reads of the unacknowledged docs
        assertThat(findIndexShard(indexName).docStats().getCount(), equalTo((long) numDocs * 2));
        // The bulk request should not have been acknowledged, since node B is isolated
        assertFalse(bulkFuture.isDone());
    }

    // A mix of threads that do indexing, updates, gets, force merges, and one thread moving shards so they can be hollowed.
    // Boolean flags give the possibility to randomly fail a relocation due to a relocation failure or object store failure.
    private void doTestStress(boolean failRelocations, boolean failObjectStore) throws Exception {
        final var masterNode = startMasterOnlyNode();
        int numOfShards = randomIntBetween(6, 8);
        var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.ZERO)
            .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 5) // so that relocations are fast in case of replaying translog
            .build();
        List<String> indexNodes = startIndexNodes(randomIntBetween(2, 4), indexNodeSettings);
        String searchNode = startSearchNode();

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numOfShards, 1).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        int docs0 = randomIntBetween(16, 128);
        indexDocs(indexName, docs0);
        flush(indexName);

        var threads = new ArrayList<Thread>();
        var stopThreads = new AtomicBoolean(false);

        // The following thread is what drives the end of the whole experiment, until it moves 10 hollowable shards.
        // It iterates shards to find hollowable shards. If none is found, it repeats.
        // It is important we give enough windows of opportunities for the thread to find hollowable shards sometimes, so
        // should not hammer the shards all the time with indexing operations or merges.
        CountDownLatch hollowRelocationsLatch = new CountDownLatch(10);
        threads.add(new Thread(() -> {
            while (stopThreads.get() == false) {
                for (int i = 0; i < numOfShards; i++) {
                    var indexShard = findIndexShard(index, i);
                    if (indexShard.getEngineOrNull() instanceof IndexEngine indexEngine && indexEngine.isLastCommitHollow() == false) {
                        var shardId = indexShard.shardId();
                        var hollowShardsService = internalCluster().getInstance(
                            HollowShardsService.class,
                            getNodeName(indexShard.routingEntry().currentNodeId())
                        );
                        if (hollowShardsService.isHollowableIndexShard(indexShard) == false) {
                            continue;
                        }

                        String sourceNode = getNodeName(indexShard.routingEntry().currentNodeId());
                        String targetNode = randomValueOtherThan(sourceNode, () -> randomFrom(indexNodes));
                        logger.info("Hollowing shard {} by relocating it from {} to {}", shardId, sourceNode, targetNode);

                        CountDownLatch relocationFailedLatch = null;
                        boolean failingObjectStore = false;
                        try {
                            // Randomly fail relocations to test how deal with failed shards with IndexEngine and a hollow commit
                            if (failRelocations && randomIntBetween(0, 10) <= 2) {
                                logger.info("--> Failing relocation between nodes {} and {}", sourceNode, targetNode);
                                relocationFailedLatch = breakRelocation(
                                    sourceNode,
                                    targetNode,
                                    randomBoolean() ? START_RELOCATION_ACTION_NAME : PRIMARY_CONTEXT_HANDOFF_ACTION_NAME
                                );
                            }

                            if (failObjectStore && relocationFailedLatch == null && randomIntBetween(0, 10) <= 2) {
                                logger.info("--> Failing object store on {}", sourceNode);
                                failingObjectStore = failObjectStore(sourceNode);
                            }

                            logger.info("--> Relocating {} hollowable shards from {} to {}", shardId, sourceNode, targetNode);
                            var timeout = TimeValue.timeValueSeconds(10);
                            ClusterRerouteResponse response = client().execute(
                                TransportClusterRerouteAction.TYPE,
                                new ClusterRerouteRequest(timeout, timeout).add(
                                    new MoveAllocationCommand(indexName, shardId.id(), sourceNode, targetNode)
                                )
                            ).get(timeout.getSeconds(), TimeUnit.SECONDS);

                            if (relocationFailedLatch != null) {
                                logger.info("--> Failed relocation between nodes {} and {}", sourceNode, targetNode);
                                safeAwait(relocationFailedLatch);
                            } else if (failingObjectStore == false) {
                                assertAcked(response);
                                // Note that the shard may still need time to fully relocate, that's why we ensure green without
                                // any ongoing relocations before next move.
                                hollowRelocationsLatch.countDown();
                                logger.info(
                                    "Shard [{}] move command processed (#{} remaining)",
                                    shardId,
                                    hollowRelocationsLatch.getCount()
                                );
                                break;
                            }
                        } catch (Exception e) {
                            if (relocationFailedLatch == null) {
                                // Shard is already relocating
                                logger.warn("Relocation failed without injected failures", e);
                            }
                        } finally {
                            if (relocationFailedLatch != null) {
                                stopBreakingRelocation(sourceNode, targetNode);
                            }
                            if (failingObjectStore) {
                                stopFailingObjectStore(sourceNode);
                            }
                            safeSleep(randomLongBetween(0, 100));
                            ensureGreenViaMasterNode(masterNode, indexName, false);
                        }
                    }
                }
            }
            logger.info("exiting");
        }, "RelocDriver"));

        Queue<String> insertedDocs = new ConcurrentLinkedQueue<>();
        int numIndexingThreads = randomIntBetween(2, 4);
        var insertedDocsCount = new AtomicLong(docs0);
        for (int i = 0; i < numIndexingThreads; i++) {
            threads.add(new Thread(() -> {
                while (stopThreads.get() == false) {
                    // Sleep a bit and do not insert too many docs, so that we give windows of opportunities w/o ingestion for
                    // shards to become hollowable.
                    safeSleep(randomLongBetween(10, 100));
                    int docs = numOfShards / numIndexingThreads;
                    var bulkResponse = indexDocs(indexName, docs);
                    logger.info("Inserted {} docs", docs);
                    insertedDocsCount.addAndGet(Arrays.stream(bulkResponse.getItems()).filter(r -> r.isFailed() == false).count());
                    insertedDocs.addAll(
                        randomSubsetOf(
                            Arrays.stream(bulkResponse.getItems()).filter(r -> r.isFailed() == false).map(e -> e.getId()).toList()
                        )
                    );
                    switch (randomInt(2)) {
                        case 0:
                            try {
                                client().admin().indices().prepareFlush(indexName).get();
                            } catch (Exception e) {
                                // ignore
                            }
                            break;
                        case 1:
                            try {
                                client().admin().indices().prepareRefresh(indexName).get();
                            } catch (Exception e) {
                                // ignore
                            }
                            break;
                        case 2:
                            // do nothing
                            break;
                        default:
                            throw new AssertionError();
                    }
                }
                logger.info("exiting");
            }, "Ingest" + i));
        }

        threads.add(new Thread(() -> {
            while (stopThreads.get() == false) {
                safeSleep(randomLongBetween(1000, 10000));
                logger.info("Requesting force merge of index {}", indexName);
                try {
                    client().admin().indices().prepareForceMerge(indexName).get();
                } catch (Exception e) {
                    // ignore
                }
            }
            logger.info("exiting");
        }, "ForceMerge"));

        threads.add(new Thread(() -> {
            while (stopThreads.get() == false) {
                safeSleep(randomLongBetween(1000, 2000));
                var refreshInterval = randomTimeValue(5, 10, TimeUnit.SECONDS);
                logger.info("Updating refresh interval on the index {} to {}", indexName, refreshInterval);
                updateIndexSettings(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval),
                    indexName
                );
                ensureGreen(indexName);
            }
            logger.info("exiting");
        }, "IndexSettingsUpdate"));

        int numReadingThreads = randomIntBetween(2, 4);
        for (int i = 0; i < numReadingThreads; i++) {
            threads.add(new Thread(() -> {
                while (stopThreads.get() == false) {
                    safeSleep(randomLongBetween(0, 100));
                    if (insertedDocs.isEmpty()) {
                        continue;
                    }
                    List<String> docIds = randomSubsetOf(Math.min(8, insertedDocs.size()), insertedDocs);
                    try {
                        if (randomBoolean()) {
                            var multiGetItemResponse = safeGet(client().prepareMultiGet().addIds(indexName, docIds).execute());
                            for (MultiGetItemResponse itemResponse : multiGetItemResponse) {
                                if (itemResponse.isFailed()) {
                                    assertThat(
                                        "Expected no failure but got: "
                                            + org.elasticsearch.common.Strings.toString(itemResponse.getFailure()),
                                        itemResponse.getFailure(),
                                        nullValue()
                                    );
                                }
                                assertTrue(itemResponse.getResponse().isExists());
                            }
                        } else {
                            for (String docId : docIds) {
                                assertTrue(safeGet(client().prepareGet(indexName, docId).execute()).isExists());
                            }
                        }
                    } catch (Exception e) {
                        throw new AssertionError("Unable to get docs in real-time", e);
                    }
                }
                logger.info("exiting");
            }, "Read" + i));
        }

        threads.add(new Thread(() -> {
            while (stopThreads.get() == false) {
                safeSleep(randomLongBetween(0, 100));
                if (insertedDocs.isEmpty()) {
                    continue;
                }
                List<String> docIds = randomSubsetOf(Math.min(insertedDocs.size(), 64), insertedDocs);
                if (docIds.isEmpty()) {
                    continue;
                }
                var client = client();
                var bulkRequestBuilder = client.prepareBulk();
                for (String docId : docIds) {
                    bulkRequestBuilder.add(
                        client.prepareUpdate(indexName, docId).setDoc("field", randomUnicodeOfCodepointLengthBetween(1, 25))
                    );
                }
                assertNoFailures(bulkRequestBuilder.get());
            }
            logger.info("exiting");
        }, "Update"));

        for (Thread thread : threads) {
            thread.start();
        }

        logger.info("Waiting for hollowRelocationsLatch");
        safeAwait(hollowRelocationsLatch, TimeValue.timeValueMinutes(10));
        logger.info("Waited for hollowRelocationsLatch");

        stopThreads.set(true);
        for (Thread thread : threads) {
            thread.join();
        }

        logger.info("Waiting for ensureGreen");
        ensureGreenViaMasterNode(masterNode, indexName, true);
        logger.info("Waited for ensureGreen");

        // Unhollow to calculate the amount of inserted docs
        int docs1 = randomIntBetween(256, 512);
        indexDocs(indexName, docs1);
        insertedDocsCount.addAndGet(docs1);
        flush(indexName);
        for (int i = 0; i < numOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
            IndexEngine indexEngine = (IndexEngine) indexShard.getEngineOrNull();
            assertFalse(indexEngine.isLastCommitHollow());
        }

        // Verify that all ingested docs are searchable
        refresh(indexName);
        assertHitCount(client().prepareSearch(indexName).setSize(0).setTrackTotalHits(true), insertedDocsCount.get());
    }

    public void testStressWithoutFailures() throws Exception {
        doTestStress(false, false);
    }

    public void testStressWithRelocationFailures() throws Exception {
        doTestStress(true, false);
    }

    // TODO: Stabilize the test with injected object store failures
    // (currently they sometimes make the test randomly hang for 1m+)
    // Inject delays when the IndexWriter is created by introducing sleeps into the blob store reads,
    // as currently we test only really tiny shards.
    @AwaitsFix(bugUrl = "https://elasticco.atlassian.net/browse/ES-10705")
    public void testStressWithRelocationOrObjectStoreFailures() throws Exception {
        doTestStress(true, true);
    }

    public void testRealTimeGet() throws Exception {
        startMasterOnlyNode();
        int numOfShards = randomIntBetween(2, 4);
        int numOfReplicas = 1;
        var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        var indexNodeA = startIndexNode(indexNodeSettings);
        var indexNodeB = startIndexNode(indexNodeSettings);
        startSearchNodes(numOfReplicas);

        var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(numOfShards, numOfReplicas).put("index.routing.allocation.exclude._name", indexNodeB)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                .build()
        );
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        indexDocs(indexName, randomIntBetween(16, 32));
        flush(indexName);

        var bulkResponse = indexDocs(indexName, randomIntBetween(256, 512));
        List<String> docIds = Arrays.stream(bulkResponse.getItems()).map(BulkItemResponse::getId).toList();

        var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        for (int i = 0; i < numOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));
            hollowShardsServiceA.ensureHollowShard(indexShard.shardId(), false);
        }
        logger.info("--> relocating {} hollowable shards from {} to {}", numOfShards, indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        ensureGreen(indexName);
        logger.info("--> relocated");
        var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, indexNodeB);
        for (int i = 0; i < numOfShards; i++) {
            hollowShardsServiceB.ensureHollowShard(new ShardId(index, i), true);
        }

        var id = randomFrom(docIds);
        // A real-time get should see the doc
        assertThat(client().prepareGet(indexName, id).get().isExists(), is(true));

        // Refreshes are always visible
        assertThat(client().prepareGet(indexName, id).setRefresh(true).setRealtime(randomBoolean()).get().isExists(), is(true));

        // A non-real time get shouldn't read from the translog
        assertThat(client().prepareGet(indexName, id).setRealtime(false).get().isExists(), is(true));

        var ids = Set.copyOf(randomNonEmptySubsetOf(docIds));
        var multiGetResponse = client().prepareMultiGet().addIds(indexName, ids).get();
        assertTrue(Stream.of(multiGetResponse.getResponses()).noneMatch(MultiGetItemResponse::isFailed));
        assertTrue(Stream.of(multiGetResponse.getResponses()).map(e -> e.getResponse()).allMatch(GetResponse::isExists));
        assertThat(Stream.of(multiGetResponse.getResponses()).map(e -> e.getResponse().getId()).collect(Collectors.toSet()), equalTo(ids));

        // Refreshes are always visible
        var multiGetResponseRefresh = client().prepareMultiGet().addIds(indexName, ids).setRefresh(true).setRealtime(randomBoolean()).get();
        assertTrue(Stream.of(multiGetResponseRefresh.getResponses()).noneMatch(MultiGetItemResponse::isFailed));
        assertTrue(Stream.of(multiGetResponseRefresh.getResponses()).map(e -> e.getResponse()).allMatch(GetResponse::isExists));

        // A non-real time mget shouldn't cause any translog reads or refreshes
        var nonRealTimeMultiGetResponse = client().prepareMultiGet().addIds(indexName, ids).setRealtime(false).get();
        assertTrue(Stream.of(nonRealTimeMultiGetResponse.getResponses()).noneMatch(MultiGetItemResponse::isFailed));
        assertTrue(Stream.of(nonRealTimeMultiGetResponse.getResponses()).map(e -> e.getResponse()).allMatch(GetResponse::isExists));
    }

    private void ensureGreenViaMasterNode(String masterNode, String index, boolean waitForEvents) {
        var healthRequest = client(masterNode).admin()
            .cluster()
            .prepareHealth(TEST_REQUEST_TIMEOUT, index)
            .setWaitForStatus(ClusterHealthStatus.GREEN)
            .setWaitForNoInitializingShards(true)
            .setWaitForNoRelocatingShards(true);
        if (waitForEvents) {
            healthRequest = healthRequest.setWaitForEvents(Priority.LANGUID);
        }
        final var future = healthRequest.execute();
        long start = System.currentTimeMillis();
        try {
            assertBusy(() -> {
                if (System.currentTimeMillis() - start > 10 * 1000) {
                    logger.warn(
                        "cluster state:\n{}",
                        client(masterNode).admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState()
                    );
                    logger.warn("cluster pending tasks:\n{}", getClusterPendingTasks());
                    logger.warn(
                        "recoveries:\n{}",
                        client(masterNode).admin()
                            .indices()
                            .recoveries(new RecoveryRequest(index))
                            .get()
                            .shardRecoveryStates()
                            .entrySet()
                            .stream()
                            .map(
                                e -> e.getKey()
                                    + ":"
                                    + e.getValue()
                                        .stream()
                                        .map(rs -> rs.getShardId() + " - " + rs.getTimer().time())
                                        .collect(Collectors.joining(","))
                            )
                            .toList()
                    );
                }
                assertThat(future.isDone(), equalTo(true));
            }, 1, TimeUnit.MINUTES);
            assertThat(future.get().getStatus(), equalTo(ClusterHealthStatus.GREEN));
        } catch (Exception e) {
            assert false : "unexpected exception: " + e;
            throw new RuntimeException(e);
        }
    }

    private static void stopFailingObjectStore(String node) {
        var repository = ObjectStoreTestUtils.getObjectStoreMockRepository(getObjectStoreService(node));
        repository.setRandomControlIOExceptionRate(0.0);
        repository.setRandomDataFileIOExceptionRate(0.0);
    }

    private static boolean failObjectStore(String node) {
        var repository = ObjectStoreTestUtils.getObjectStoreMockRepository(getObjectStoreService(node));
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(1);
        if (randomBoolean()) {
            repository.setRandomIOExceptionPattern(".*stateless_commit_.*");
        } else if (randomBoolean()) {
            repository.setRandomIOExceptionPattern(".*translog.*");
        }
        return true;
    }

    private static void checkLastCommitIsNotHollow(String node, ShardId shardId) {
        var lastCompoundCommit = internalCluster().getInstance(StatelessCommitService.class, node)
            .getLatestUploadedBcc(shardId)
            .lastCompoundCommit();
        assertFalse(lastCompoundCommit.hollow());
        assertThat(lastCompoundCommit.nodeEphemeralId(), equalTo(getEphemeralId(node)));
    }

    private static String getEphemeralId(String node) {
        return internalCluster().getInstance(ClusterService.class, node).localNode().getEphemeralId();
    }

    private static String getNodeName(String id) {
        return internalCluster().getInstance(ClusterService.class).state().nodes().get(id).getName();
    }

    protected void createDataStreamWithMultipleBackingIndices(String dataStream, boolean failureStore) throws Exception {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request("id1");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStream + "*"))
                .template(
                    Template.builder()
                        .dataStreamOptions(
                            failureStore
                                ? new DataStreamOptions.Template(DataStreamFailureStore.builder().enabled(true).buildTemplate())
                                : null
                        )
                        .lifecycle(DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(1)))
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();

        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStream
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocsToDataStreamAndWaitForMultipleBackingIndices(dataStream, null);
        ensureGreen();
    }

    protected List<Index> getBackingIndices(String dataStream, boolean failureStore) {
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStream }
        );
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStream));
        if (failureStore) {
            return getDataStreamResponse.getDataStreams().get(0).getDataStream().getFailureIndices();
        } else {
            return getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
        }
    }

    protected void indexDocsToDataStreamAndWaitForMultipleBackingIndices(String dataStream, String pipeline) throws Exception {
        final var backingIndices = new HashSet<String>();
        assertBusy(() -> {
            BulkRequest bulkRequest = new BulkRequest();
            final int numDocs = randomIntBetween(1, 5);
            for (int i = 0; i < numDocs; i++) {
                String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
                var indexRequest = new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON);
                if (pipeline != null) {
                    indexRequest.setPipeline(pipeline);
                }
                bulkRequest.add(indexRequest);
            }
            BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
            assertThat(bulkResponse.getItems().length, equalTo(numDocs));
            String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + dataStream;
            String failBackingIndexPrefix = DataStream.FAILURE_STORE_PREFIX + dataStream;
            for (BulkItemResponse itemResponse : bulkResponse) {
                assertThat(itemResponse.getFailureMessage(), nullValue());
                assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
                final var backingIndex = itemResponse.getIndex();
                backingIndices.add(backingIndex);
                assertThat(backingIndex, either(startsWith(backingIndexPrefix)).or(startsWith(failBackingIndexPrefix)));
            }

            // Index docs until the data stream has at least two backing indices
            assertThat(backingIndices.size(), greaterThan(1));
        });
        indicesAdmin().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    private void createBasicPipeline(String processorType) {
        createPipeline(Strings.format("\"%s\": {}", processorType));
    }

    private void createPipeline(String processor) {
        String pipeline = "pipeline-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        putJsonPipeline(pipeline, Strings.format("{\"processors\": [{%s}]}", processor));
    }

    public static class CustomIngestTestPlugin extends IngestTestPlugin {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> processors = new HashMap<>();
            processors.put(
                "fail",
                (processorFactories, tag, description, config, projectId) -> new TestProcessor(
                    tag,
                    "fail",
                    description,
                    new RuntimeException()
                )
            );
            return processors;
        }
    }

}
