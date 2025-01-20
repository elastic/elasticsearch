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
import co.elastic.elasticsearch.stateless.engine.HollowIndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.ResettableValue;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsServiceTestUtils.isIngestionBlocked;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
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
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(DATA_STREAM_LIFECYCLE_POLL_INTERVAL, TimeValue.timeValueSeconds(1))
            .put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
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
            // .map(IndexShard::shardId)
            .collect(Collectors.toUnmodifiableSet());
        internalCluster().getInstances(HollowShardsService.class).forEach(hollowShardsService -> {
            shards.forEach(shard -> hollowShardsService.uninstallIngestionBlocker(shard));
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
        indexEngine.flushHollow(future);
        if (randomBoolean()) {
            indicesAdmin().prepareFlush(indexName).setForce(true).get(TimeValue.timeValueSeconds(10)); // competing flush
        }
        safeGet(future);
        assertTrue(indexEngine.isLastCommitHollow());
        assertTrue(statelessCommitService.getLatestUploadedBcc(shardId).lastCompoundCommit().hollow());

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
        String indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());

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
            assertFalse(indexEngine.getStatelessCommitService().getLatestUploadedBcc(indexShard.shardId()).lastCompoundCommit().hollow());
        }

        indexDocs(indexName, randomIntBetween(16, 64));
        // Manually mark subset of shards as hollow
        List<Integer> hollowShardIds = randomSubsetOf(shardIds);
        for (var shardId : hollowShardIds) {
            var indexShard = findIndexShard(index, shardId);
            var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
            var sync = new PlainActionFuture<Engine.FlushResult>();
            indexEngine.flushHollow(sync);
            safeGet(sync);
            assertTrue(indexEngine.isLastCommitHollow());
            assertTrue(indexEngine.getStatelessCommitService().getLatestUploadedBcc(indexShard.shardId()).lastCompoundCommit().hollow());
        }

        logger.warn("--> restarting node {}", indexNode);
        internalCluster().restartNode(indexNode);
        logger.warn("--> restarted");
        ensureGreen(indexName);

        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexNode);

        for (int i = 0; i < numberOfShards; i++) {
            final var indexShard = findIndexShard(index, i);
            final var engine = indexShard.getEngineOrNull();
            if (hollowShardIds.contains(i)) {
                // After recovery, hollow shards switch to HollowIndexEngine and the ingestion blocker should be installed
                assertThat(engine, instanceOf(HollowIndexEngine.class));
                assertTrue(isIngestionBlocked(hollowShardsService, indexShard.shardId()));
            } else {
                assertThat(engine, instanceOf(IndexEngine.class));
            }
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
            assertFalse(isIngestionBlocked(hollowShardsServiceA, indexShard.shardId()));
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
            assertTrue(isIngestionBlocked(hollowShardsServiceB, indexShard.shardId()));
            var commitAfterRelocation = statelessCommitService.getLatestUploadedBcc(indexShard.shardId()).lastCompoundCommit();
            assertTrue(commitAfterRelocation.hollow());
            assertThat(commitAfterRelocation.nodeEphemeralId(), is(emptyString()));
        }
    }

    public void testRelocateHollowShards() throws Exception {
        startMasterOnlyNode();
        var indexNodeA = startIndexNode();
        var indexNodeB = startIndexNode();
        final var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        final var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, indexNodeB);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        indexDocs(indexName, between(20, 100));
        // Randomly mark a subset shards as hollow, relocate the rest as normal index shards
        List<Integer> hollowShardIds = IntStream.range(0, numberOfShards).boxed().toList();
        Map<ShardId, PrimaryTermAndGeneration> initialHollowPrimaryTermGenerations = new HashMap<>();
        for (var shardId : hollowShardIds) {
            var indexShard = findIndexShard(index, shardId);
            var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
            var sync = new PlainActionFuture<Engine.FlushResult>();
            indexEngine.flushHollow(sync);
            safeGet(sync);
            assertTrue(indexEngine.isLastCommitHollow());

            var statelessCompoundCommit = indexEngine.getStatelessCommitService()
                .getLatestUploadedBcc(indexShard.shardId())
                .lastCompoundCommit();
            assertTrue(statelessCompoundCommit.hollow());
            initialHollowPrimaryTermGenerations.put(indexShard.shardId(), statelessCompoundCommit.primaryTermAndGeneration());
        }

        logger.info("--> relocating {} shards from {} to {}", numberOfShards, indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        ensureGreen(indexName);
        logger.info("--> relocated");

        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            var engine = indexShard.getEngineOrNull();
            if (hollowShardIds.contains(i)) {
                assertThat(engine, instanceOf(HollowIndexEngine.class));
                assertFalse(isIngestionBlocked(hollowShardsServiceA, indexShard.shardId()));
                assertTrue(isIngestionBlocked(hollowShardsServiceB, indexShard.shardId()));

                // Check that relocating a hollow shard we don't trigger an extra hollow flush
                var commitAfterRelocationToNodeB = internalCluster().getInstance(StatelessCommitService.class, indexNodeB)
                    .getLatestUploadedBcc(indexShard.shardId())
                    .lastCompoundCommit();
                assertTrue(commitAfterRelocationToNodeB.hollow());
                assertThat(
                    commitAfterRelocationToNodeB.primaryTermAndGeneration(),
                    equalTo(initialHollowPrimaryTermGenerations.get(indexShard.shardId()))
                );
            } else {
                assertThat(engine, instanceOf(IndexEngine.class));
                assertFalse(isIngestionBlocked(hollowShardsServiceA, indexShard.shardId()));
                assertFalse(isIngestionBlocked(hollowShardsServiceB, indexShard.shardId()));
            }
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
            if (hollowShardIds.contains(i)) {
                assertThat(engine, instanceOf(HollowIndexEngine.class));
                assertTrue(isIngestionBlocked(hollowShardsServiceA, indexShard.shardId()));
                assertFalse(isIngestionBlocked(hollowShardsServiceB, indexShard.shardId()));

                // No extra flushes triggered on relocating hollow shards with `HollowIndexEngine`
                var commitAfterRelocationToNodeA = internalCluster().getInstance(StatelessCommitService.class, indexNodeA)
                    .getLatestUploadedBcc(indexShard.shardId())
                    .lastCompoundCommit();
                assertTrue(commitAfterRelocationToNodeA.hollow());
                assertThat(
                    commitAfterRelocationToNodeA.primaryTermAndGeneration(),
                    equalTo(initialHollowPrimaryTermGenerations.get(indexShard.shardId()))
                );
            } else {
                assertThat(engine, instanceOf(IndexEngine.class));
                assertFalse(isIngestionBlocked(hollowShardsServiceA, indexShard.shardId()));
                assertFalse(isIngestionBlocked(hollowShardsServiceB, indexShard.shardId()));
            }
        }
    }

    @AwaitsFix(bugUrl = "https://elasticco.atlassian.net/browse/ES-10573")
    public void testRelocateHollowableShardsWithFailure() throws Exception {
        startMasterOnlyNode();
        var indexNodeSettings = Settings.builder().put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1)).build();
        var indexNodeA = startIndexNode(indexNodeSettings);
        var indexNodeB = startIndexNode(indexNodeSettings);

        var indexName = randomIdentifier();
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        indexDocs(indexName, between(20, 100));
        var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
            assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(true)));
        }

        var relocationFailedLatch = breakRelocation(
            indexNodeA,
            indexNodeB,
            randomBoolean() ? START_RELOCATION_ACTION_NAME : PRIMARY_CONTEXT_HANDOFF_ACTION_NAME
        );
        logger.info("--> relocating {} hollowable shards from {} to {}", numberOfShards, indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        safeAwait(relocationFailedLatch);

        ensureGreen(indexName);
        assertNodeHasNoCurrentRecoveries(indexNodeB);
        assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeB)));

        // If relocation fails, we produce a flush hollow commit, but don't swap the shard's engine to HollowIndexEngine
        // TODO ES-10573 should fix this behaviour
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            var engine = indexShard.getEngineOrNull();
            assertThat(engine, instanceOf(IndexEngine.class));
        }
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
