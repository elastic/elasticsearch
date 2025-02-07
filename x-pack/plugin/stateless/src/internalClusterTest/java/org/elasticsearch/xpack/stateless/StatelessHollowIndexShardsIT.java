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
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.ResettableValue;
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
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS;
import static co.elastic.elasticsearch.stateless.engine.IndexEngineTestUtils.flushHollow;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL;
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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class StatelessHollowIndexShardsIT extends AbstractStatelessIntegTestCase {

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
            .put(DATA_STREAM_LIFECYCLE_POLL_INTERVAL, TimeValue.timeValueSeconds(1))
            .put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1")
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        releaseIngestionBlockers();
        super.tearDown();
    }

    // TODO may not be necessary after ES-10253 if cleanup operations are noop for hollow shards
    private static void releaseIngestionBlockers() {
        var shards = resolveIndices().entrySet()
            .stream()
            .flatMap(e -> IntStream.range(0, e.getValue()).mapToObj(i -> findIndexShard(e.getKey(), i)))
            .collect(Collectors.toUnmodifiableSet());
        internalCluster().getInstances(HollowShardsService.class).forEach(hollowShardsService -> {
            shards.forEach(shard -> hollowShardsService.removeHollowShard(shard));
        });
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

    public void testRecoverHollowShard() throws Exception {
        startMasterOnlyNode();
        final var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        String indexNodeA = startIndexNode(indexNodeSettings);
        final var statelessCommitServiceA = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);
        final var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 0).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        indexDocs(indexName, randomIntBetween(16, 64));
        flush(indexName);

        List<Integer> shardIds = IntStream.range(0, numberOfShards).boxed().toList();
        for (var shardId : shardIds) {
            var indexShard = findIndexShard(index, shardId);
            var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
            assertFalse(indexEngine.isLastCommitHollow());
            assertFalse(statelessCommitServiceA.getLatestUploadedBcc(indexShard.shardId()).lastCompoundCommit().hollow());
            assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));
        }

        String indexNodeB = startIndexNode(indexNodeSettings);
        logger.info("--> relocating {} hollowable shards from {} to {}", numberOfShards, indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        ensureGreen(indexName);
        logger.info("--> relocated");

        for (var shardId : shardIds) {
            var indexShard = findIndexShard(index, shardId);
            var engine = indexShard.getEngineOrNull();
            assertThat(engine, instanceOf(HollowIndexEngine.class));
            internalCluster().getInstance(HollowShardsService.class, indexNodeB).ensureHollowShard(indexShard.shardId(), true);
        }

        logger.info("--> stopping node A");
        internalCluster().stopNode(indexNodeA);
        ensureGreen(indexName);

        logger.info("--> restarting node B");
        internalCluster().restartNode(indexNodeB);
        ensureGreen(indexName);

        for (int i = 0; i < numberOfShards; i++) {
            final var indexShard = findIndexShard(index, i);
            final var engine = indexShard.getEngineOrNull();
            assertThat(engine, instanceOf(HollowIndexEngine.class));
            internalCluster().getInstance(HollowShardsService.class, indexNodeB).ensureHollowShard(indexShard.shardId(), true);
        }
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
        if (failStartElseHandoff) {
            // If a primary relocation fails before the hollow flush, e.g., due to node disconnection during the start message exchange, the
            // engine on the source node will be left to IndexEngine.
            assertThat(engine, instanceOf(IndexEngine.class));
            assertFalse(((IndexEngine) engine).isLastCommitHollow());
            hollowShardsService.ensureHollowShard(indexShard.shardId(), false);
        } else {
            // If a primary relocation fails after the hollow flush, e.g., due to node disconnection during the handoff exchange, the
            // engine on the source node will have been reset to HollowIndexEngine and stay like that.
            assertThat(engine, instanceOf(HollowIndexEngine.class));
            hollowShardsService.ensureHollowShard(indexShard.shardId(), true);
        }

        indexDocsAndRefresh(indexName, numDocs);
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs * 2));
    }

    public void testRelocateHollowableShardWithLingeringIngestionUponConnectionFailure() throws Exception {
        startMasterOnlyNode();
        var indexNodeSettings = Settings.builder().put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1)).build();
        var indexNodeA = startIndexNode(indexNodeSettings);
        var statelessCommitServiceA = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        var numDocs = between(20, 100);
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
        logger.info("--> relocating hollowable shard from {} to {}", indexNodeA, indexNodeB);

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

        // The lingering ingestion was blocked on primary permits until the relocation failure, after which it was blocked on the ingestion
        // blocker which initiated unhollowing and reset the engine to IndexEngine, after which ingestion was processed.
        // So, verify that all ingested docs are searchable.
        logger.info("--> starting a search node");
        startSearchNode();
        setReplicaCount(1, indexName);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, numDocs);
        assertHitCount(client().prepareSearch(indexName).setSize(0).setTrackTotalHits(true), numDocs * 3);
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

    public void testUnhollowOnIngestion() throws Exception {
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

        indexDocs(indexName, between(16, 128));
        flush(indexName);
        indexDocs(indexName, between(16, 128));
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

        // Try to inject documents from background threads, the shards should unhollow on first ingestion
        int indexDocsThreads = randomIntBetween(1, 8);
        var indexDocsExecutor = Executors.newFixedThreadPool(indexDocsThreads);
        try {
            List<Long> generationsBeforeUnhollow = IntStream.range(0, numberOfShards)
                .mapToObj(i -> statelessCommitService.getLatestUploadedBcc(new ShardId(index, i)).lastCompoundCommit().generation())
                .toList();
            var indexedDocsLatch = new CountDownLatch(indexDocsThreads);
            for (int i = 0; i < indexDocsThreads; i++) {
                indexDocsExecutor.submit(() -> {
                    indexDocs(indexName, randomIntBetween(16, 64));
                    indexedDocsLatch.countDown();
                });
            }
            safeAwait(indexedDocsLatch);
            for (int i = 0; i < numberOfShards; i++) {
                // Should unhollow only once
                assertThat(
                    statelessCommitService.getLatestUploadedBcc(new ShardId(index, i)).lastCompoundCommit().generation(),
                    equalTo(generationsBeforeUnhollow.get(i) + 1)
                );
            }
        } finally {
            terminate(indexDocsExecutor);
        }

        // Check that shards switched to the index engine and flushed a blob with a new translog node ID
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            var engine = indexShard.getEngineOrNull();
            var indexEngine = asInstanceOf(IndexEngine.class, engine);
            assertFalse(indexEngine.isLastCommitHollow());
            checkLastCommitIsNotHollow(indexNodeB, indexShard.shardId());
        }

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

    public void testStress() throws Exception {
        startMasterOnlyNode();
        int numOfShards = randomIntBetween(4, 8);
        var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        List<String> indexNodes = startIndexNodes(randomIntBetween(2, 4), indexNodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numOfShards, 0).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        int docs0 = randomIntBetween(16, 128);
        indexDocs(indexName, docs0);
        flush(indexName);

        CountDownLatch hollowRelocationsLatch = new CountDownLatch(10);

        var threads = new ArrayList<Thread>();
        var stopHollowing = new AtomicBoolean(false);
        threads.add(new Thread(() -> {
            while (stopHollowing.get() == false) {
                safeSleep(randomLongBetween(0, 100));
                ensureGreenViaMasterNode(indexName);
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
                            if (randomIntBetween(0, 10) <= 2) {
                                logger.info("--> Failing relocation between nodes {} and {}", sourceNode, targetNode);
                                relocationFailedLatch = breakRelocation(
                                    sourceNode,
                                    targetNode,
                                    randomBoolean() ? START_RELOCATION_ACTION_NAME : PRIMARY_CONTEXT_HANDOFF_ACTION_NAME
                                );
                            }

                            // TODO https://elasticco.atlassian.net/browse/ES-10705
                            // Inject delays when the IndexWriter is created by introducing sleeps into the blob store reads,
                            // as currently we test only really tiny shards.
                            if (relocationFailedLatch == null && randomIntBetween(0, 10) <= 2) {
                                logger.info("--> Failing object store on {}", sourceNode);
                                // TODO https://elasticco.atlassian.net/browse/ES-10705
                                // Stabilize the test with injected object store failures
                                // (currently they sometimes make the test randomly hang for 1m+)
                                // failingObjectStore = failObjectStore(sourceNode);
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
                                logger.info("Shard [{}] has been relocated (#{} remaining)", shardId, hollowRelocationsLatch.getCount());
                                hollowRelocationsLatch.countDown();
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
                        }
                    }
                }
            }
        }));

        Queue<String> insertedDocs = new ConcurrentLinkedQueue<>();

        var stopIndexing = new AtomicBoolean(false);
        int numIndexingThreads = randomIntBetween(2, 4);
        var insertedDocsCount = new AtomicLong(docs0);
        for (int i = 0; i < numIndexingThreads; i++) {
            threads.add(new Thread(() -> {
                while (stopIndexing.get() == false) {
                    safeSleep(randomLongBetween(0, 100));
                    int docs = randomIntBetween(16, 128);
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
                            flush(indexName);
                            break;
                        case 1:
                            refresh(indexName);
                            break;
                        case 2:
                            // do nothing
                            break;
                        default:
                            throw new AssertionError();
                    }
                }
            }));
        }

        var stopUpdating = new AtomicBoolean(false);
        // TODO Support updates in https://elasticco.atlassian.net/browse/ES-10708
        /*threads.add(new Thread(() -> {
            while (stopUpdating.get() == false) {
                safeSleep(randomLongBetween(0, 100));
                lock.lock();
                try {
                    if (insertedDocs.isEmpty()) {
                        continue;
                    }
                    List<String> docIds = randomSubsetOf(Math.min(insertedDocs.size(), 64), insertedDocs);
                    if (docIds.isEmpty()) {
                        continue;
                    }
                    var bulkRequestBuilder = client().prepareBulk();
                    for (String docId : docIds) {
                        bulkRequestBuilder.add(
                            new UpdateRequest(indexName, docId).doc("field", randomUnicodeOfCodepointLengthBetween(1, 25))
                        );
                    }
                    assertNoFailures(bulkRequestBuilder.get());
                    logger.info("Updated {} docs", docIds.size());
                } finally {
                    lock.unlock();
                }
            }
        }));*/

        for (Thread thread : threads) {
            thread.start();
        }

        logger.info("Waiting for hollowRelocationsLatch");
        safeAwait(hollowRelocationsLatch, TimeValue.timeValueMinutes(3));
        logger.info("Waited for hollowRelocationsLatch");

        stopHollowing.set(true);
        stopIndexing.set(true);
        stopUpdating.set(true);
        for (Thread thread : threads) {
            thread.join();
        }

        logger.info("Waiting for ensureGreen");
        ensureGreen(indexName);
        logger.info("Waited for ensureGreen");

        // Unhollow to calculate the amount of inserted docs
        int docs1 = randomIntBetween(256, 512);
        indexDocs(indexName, docs1);
        insertedDocsCount.addAndGet(docs1);
        flush(indexName);
        for (int i = 0; i < numOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
            assertFalse(((IndexEngine) indexShard.getEngineOrNull()).isLastCommitHollow());
        }
        assertThat(
            indicesAdmin().prepareStats(indexName).setDocs(true).get().getTotal().getDocs().getCount(),
            equalTo(insertedDocsCount.get())
        );

        // Verify that all ingested docs are searchable
        logger.info("--> starting a search node");
        startSearchNode();
        updateIndexSettings(Settings.builder().put("index.number_of_replicas", 1), indexName);
        ensureGreen(indexName);
        assertHitCount(client().prepareSearch(indexName).setSize(0).setTrackTotalHits(true), insertedDocsCount.get());
    }

    private static void ensureGreenViaMasterNode(String index) {
        assertThat(
            client(internalCluster().getMasterName()).admin()
                .cluster()
                .health(
                    new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, index).waitForStatus(ClusterHealthStatus.GREEN)
                        .waitForNoInitializingShards(true)
                        .waitForNoRelocatingShards(true)
                        .waitForEvents(Priority.LANGUID)
                )
                .actionGet(TEST_REQUEST_TIMEOUT)
                .getStatus(),
            equalTo(ClusterHealthStatus.GREEN)
        );
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
                                ? new DataStreamOptions.Template(
                                    ResettableValue.create(new DataStreamFailureStore.Template(ResettableValue.create(true)))
                                )
                                : null
                        )
                        .lifecycle(DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueDays(1)).build())
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
                (processorFactories, tag, description, config) -> new TestProcessor(tag, "fail", description, new RuntimeException())
            );
            return processors;
        }
    }

}
