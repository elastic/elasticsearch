/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsTests;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexAliasesService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.action.admin.indices.rollover.TransportRolloverAction.evaluateConditions;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TransportRolloverActionTests extends ESTestCase {

    public void testDocStatsSelectionFromPrimariesOnly() {
        long docsInPrimaryShards = 100;
        long docsInShards = 200;

        final Condition<?> condition = createTestCondition();
        String indexName = randomAlphaOfLengthBetween(5, 7);
        evaluateConditions(Sets.newHashSet(condition), createMetaData(indexName),
                createIndicesStatResponse(indexName, docsInShards, docsInPrimaryShards));
        final ArgumentCaptor<Condition.Stats> argument = ArgumentCaptor.forClass(Condition.Stats.class);
        verify(condition).evaluate(argument.capture());

        assertEquals(docsInPrimaryShards, argument.getValue().numDocs);
    }

    public void testEvaluateConditions() {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(2));
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomIntBetween(10, 100), ByteSizeUnit.MB));

        long matchMaxDocs = randomIntBetween(100, 1000);
        long notMatchMaxDocs = randomIntBetween(0, 99);
        ByteSizeValue notMatchMaxSize = new ByteSizeValue(randomIntBetween(0, 9), ByteSizeUnit.MB);
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        final IndexMetaData metaData = IndexMetaData.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
        final Set<Condition<?>> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition, maxSizeCondition);
        Map<String, Boolean> results = evaluateConditions(conditions,
            new DocsStats(matchMaxDocs, 0L, ByteSizeUnit.MB.toBytes(120)), metaData);
        assertThat(results.size(), equalTo(3));
        for (Boolean matched : results.values()) {
            assertThat(matched, equalTo(true));
        }

        results = evaluateConditions(conditions, new DocsStats(notMatchMaxDocs, 0, notMatchMaxSize.getBytes()), metaData);
        assertThat(results.size(), equalTo(3));
        for (Map.Entry<String, Boolean> entry : results.entrySet()) {
            if (entry.getKey().equals(maxAgeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(true));
            } else if (entry.getKey().equals(maxDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else {
                fail("unknown condition result found " + entry.getKey());
            }
        }
    }

    public void testEvaluateWithoutDocStats() {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(randomNonNegativeLong());
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(randomIntBetween(1, 3)));
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong()));

        Set<Condition<?>> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition, maxSizeCondition);
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 1000))
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomInt(10))
            .build();

        final IndexMetaData metaData = IndexMetaData.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(5, 10)).getMillis())
            .settings(settings)
            .build();
        Map<String, Boolean> results = evaluateConditions(conditions, null, metaData);
        assertThat(results.size(), equalTo(3));

        for (Map.Entry<String, Boolean> entry : results.entrySet()) {
            if (entry.getKey().equals(maxAgeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(true));
            } else if (entry.getKey().equals(maxDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else {
                fail("unknown condition result found " + entry.getKey());
            }
        }
    }

    public void testEvaluateWithoutMetaData() {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(2));
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomIntBetween(10, 100), ByteSizeUnit.MB));

        long matchMaxDocs = randomIntBetween(100, 1000);
        final Set<Condition<?>> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition, maxSizeCondition);
        Map<String, Boolean> results = evaluateConditions(conditions,
            new DocsStats(matchMaxDocs, 0L, ByteSizeUnit.MB.toBytes(120)), null);
        assertThat(results.size(), equalTo(3));
        results.forEach((k, v) -> assertFalse(v));

        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 1000))
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomInt(10))
            .build();

        final IndexMetaData metaData = IndexMetaData.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(5, 10)).getMillis())
            .settings(settings)
            .build();
        IndicesStatsResponse indicesStats = randomIndicesStatsResponse(new IndexMetaData[]{metaData});
        Map<String, Boolean> results2 = evaluateConditions(conditions, null, indicesStats);
        assertThat(results2.size(), equalTo(3));
        results2.forEach((k, v) -> assertFalse(v));
    }

    public void testRolloverAliasActions() {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(sourceAlias, targetIndex);

        List<AliasAction> actions = TransportRolloverAction.rolloverAliasToNewIndex(sourceIndex, targetIndex, rolloverRequest, false);
        assertThat(actions, hasSize(2));
        boolean foundAdd = false;
        boolean foundRemove = false;
        for (AliasAction action : actions) {
            if (action.getIndex().equals(targetIndex)) {
                assertEquals(sourceAlias, ((AliasAction.Add) action).getAlias());
                foundAdd = true;
            } else if (action.getIndex().equals(sourceIndex)) {
                assertEquals(sourceAlias, ((AliasAction.Remove) action).getAlias());
                foundRemove = true;
            } else {
                throw new AssertionError("Unknown index [" + action.getIndex() + "]");
            }
        }
        assertTrue(foundAdd);
        assertTrue(foundRemove);
    }

    public void testRolloverAliasActionsWithExplicitWriteIndex() {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(sourceAlias, targetIndex);
        List<AliasAction> actions = TransportRolloverAction.rolloverAliasToNewIndex(sourceIndex, targetIndex, rolloverRequest, true);

        assertThat(actions, hasSize(2));
        boolean foundAddWrite = false;
        boolean foundRemoveWrite = false;
        for (AliasAction action : actions) {
            AliasAction.Add addAction = (AliasAction.Add) action;
            if (action.getIndex().equals(targetIndex)) {
                assertEquals(sourceAlias, addAction.getAlias());
                assertTrue(addAction.writeIndex());
                foundAddWrite = true;
            } else if (action.getIndex().equals(sourceIndex)) {
                assertEquals(sourceAlias, addAction.getAlias());
                assertFalse(addAction.writeIndex());
                foundRemoveWrite = true;
            } else {
                throw new AssertionError("Unknown index [" + action.getIndex() + "]");
            }
        }
        assertTrue(foundAddWrite);
        assertTrue(foundRemoveWrite);
    }

    public void testValidation() {
        String index1 = randomAlphaOfLength(10);
        String aliasWithWriteIndex = randomAlphaOfLength(10);
        String index2 = randomAlphaOfLength(10);
        String aliasWithNoWriteIndex = randomAlphaOfLength(10);
        Boolean firstIsWriteIndex = randomFrom(false, null);
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        MetaData.Builder metaDataBuilder = MetaData.builder()
            .put(IndexMetaData.builder(index1)
                .settings(settings)
                .putAlias(AliasMetaData.builder(aliasWithWriteIndex))
                .putAlias(AliasMetaData.builder(aliasWithNoWriteIndex).writeIndex(firstIsWriteIndex))
            );
        IndexMetaData.Builder indexTwoBuilder = IndexMetaData.builder(index2).settings(settings);
        if (firstIsWriteIndex == null) {
            indexTwoBuilder.putAlias(AliasMetaData.builder(aliasWithNoWriteIndex).writeIndex(randomFrom(false, null)));
        }
        metaDataBuilder.put(indexTwoBuilder);
        MetaData metaData = metaDataBuilder.build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.validate(metaData, new RolloverRequest(aliasWithNoWriteIndex,
                randomAlphaOfLength(10))));
        assertThat(exception.getMessage(), equalTo("source alias [" + aliasWithNoWriteIndex + "] does not point to a write index"));
        exception = expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.validate(metaData, new RolloverRequest(randomFrom(index1, index2),
                randomAlphaOfLength(10))));
        assertThat(exception.getMessage(), equalTo("source alias is a concrete index"));
        exception = expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.validate(metaData, new RolloverRequest(randomAlphaOfLength(5),
                randomAlphaOfLength(10)))
        );
        assertThat(exception.getMessage(), equalTo("source alias does not exist"));
        TransportRolloverAction.validate(metaData, new RolloverRequest(aliasWithWriteIndex, randomAlphaOfLength(10)));
    }

    public void testGenerateRolloverIndexName() {
        String invalidIndexName = randomAlphaOfLength(10) + "A";
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
        expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.generateRolloverIndexName(invalidIndexName, indexNameExpressionResolver));
        int num = randomIntBetween(0, 100);
        final String indexPrefix = randomAlphaOfLength(10);
        String indexEndingInNumbers = indexPrefix + "-" + num;
        assertThat(TransportRolloverAction.generateRolloverIndexName(indexEndingInNumbers, indexNameExpressionResolver),
            equalTo(indexPrefix + "-" + String.format(Locale.ROOT, "%06d", num + 1)));
        assertThat(TransportRolloverAction.generateRolloverIndexName("index-name-1", indexNameExpressionResolver),
            equalTo("index-name-000002"));
        assertThat(TransportRolloverAction.generateRolloverIndexName("index-name-2", indexNameExpressionResolver),
            equalTo("index-name-000003"));
        assertEquals( "<index-name-{now/d}-000002>", TransportRolloverAction.generateRolloverIndexName("<index-name-{now/d}-1>",
            indexNameExpressionResolver));
    }

    public void testCreateIndexRequest() {
        String alias = randomAlphaOfLength(10);
        String rolloverIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(alias, randomAlphaOfLength(10));
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        rolloverRequest.getCreateIndexRequest().waitForActiveShards(activeShardCount);
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        rolloverRequest.getCreateIndexRequest().settings(settings);
        final CreateIndexClusterStateUpdateRequest createIndexRequest =
            TransportRolloverAction.prepareCreateIndexRequest(rolloverIndex, rolloverIndex, rolloverRequest);
        assertThat(createIndexRequest.settings(), equalTo(settings));
        assertThat(createIndexRequest.index(), equalTo(rolloverIndex));
        assertThat(createIndexRequest.cause(), equalTo("rollover_index"));
    }

    public void testRejectDuplicateAlias() {
        final IndexTemplateMetaData template = IndexTemplateMetaData.builder("test-template")
            .patterns(Arrays.asList("foo-*", "bar-*"))
            .putAlias(AliasMetaData.builder("foo-write")).putAlias(AliasMetaData.builder("bar-write").writeIndex(randomBoolean()))
            .build();
        final MetaData metaData = MetaData.builder().put(createMetaData(randomAlphaOfLengthBetween(5, 7)), false).put(template).build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> TransportRolloverAction.checkNoDuplicatedAliasInIndexTemplate(metaData, indexName, aliasName));
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testConditionEvaluationWhenAliasToWriteAndReadIndicesConsidersOnlyPrimariesFromWriteIndex() {
        final TransportService mockTransportService = mock(TransportService.class);
        final ClusterService mockClusterService = mock(ClusterService.class);
        final DiscoveryNode mockNode = mock(DiscoveryNode.class);
        when(mockNode.getId()).thenReturn("mocknode");
        when(mockClusterService.localNode()).thenReturn(mockNode);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        final MetaDataCreateIndexService mockCreateIndexService = mock(MetaDataCreateIndexService.class);
        final IndexNameExpressionResolver mockIndexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(mockIndexNameExpressionResolver.resolveDateMathExpression(any())).thenReturn("logs-index-000003");
        final ActionFilters mockActionFilters = mock(ActionFilters.class);
        final MetaDataIndexAliasesService mdIndexAliasesService = mock(MetaDataIndexAliasesService.class);

        final Client mockClient = mock(Client.class);

        final Map<String, IndexStats> indexStats = new HashMap<>();
        int total = randomIntBetween(500, 1000);
        indexStats.put("logs-index-000001", createIndexStats(200L, total));
        indexStats.put("logs-index-000002", createIndexStats(300L, total));
        final IndicesStatsResponse statsResponse = createAliasToMultipleIndicesStatsResponse(indexStats);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            ActionListener<IndicesStatsResponse> listener = (ActionListener<IndicesStatsResponse>) args[2];
            listener.onResponse(statsResponse);
            return null;
        }).when(mockClient).execute(eq(IndicesStatsAction.INSTANCE), any(ActionRequest.class), any(ActionListener.class));

        assert statsResponse.getPrimaries().getDocs().getCount() == 500L;
        assert statsResponse.getTotal().getDocs().getCount() == (total + total);

        final IndexMetaData.Builder indexMetaData = IndexMetaData.builder("logs-index-000001")
                .putAlias(AliasMetaData.builder("logs-alias").writeIndex(false).build()).settings(settings(Version.CURRENT))
                .numberOfShards(1).numberOfReplicas(1);
        final IndexMetaData.Builder indexMetaData2 = IndexMetaData.builder("logs-index-000002")
                .putAlias(AliasMetaData.builder("logs-alias").writeIndex(true).build()).settings(settings(Version.CURRENT))
                .numberOfShards(1).numberOfReplicas(1);
        final ClusterState stateBefore = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder().put(indexMetaData).put(indexMetaData2)).build();

        final TransportRolloverAction transportRolloverAction = new TransportRolloverAction(mockTransportService, mockClusterService,
                mockThreadPool, mockCreateIndexService, mockActionFilters, mockIndexNameExpressionResolver, mdIndexAliasesService,
                mockClient);

        // For given alias, verify that condition evaluation fails when the condition doc count is greater than the primaries doc count
        // (primaries from only write index is considered)
        PlainActionFuture<RolloverResponse> future = new PlainActionFuture<>();
        RolloverRequest rolloverRequest = new RolloverRequest("logs-alias", "logs-index-000003");
        rolloverRequest.addMaxIndexDocsCondition(500L);
        rolloverRequest.dryRun(true);
        transportRolloverAction.masterOperation(mock(Task.class), rolloverRequest, stateBefore, future);

        RolloverResponse response = future.actionGet();
        assertThat(response.getOldIndex(), equalTo("logs-index-000002"));
        assertThat(response.getNewIndex(), equalTo("logs-index-000003"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(1));
        assertThat(response.getConditionStatus().get("[max_docs: 500]"), is(false));

        // For given alias, verify that the condition evaluation is successful when condition doc count is less than the primaries doc count
        // (primaries from only write index is considered)
        future = new PlainActionFuture<>();
        rolloverRequest = new RolloverRequest("logs-alias", "logs-index-000003");
        rolloverRequest.addMaxIndexDocsCondition(300L);
        rolloverRequest.dryRun(true);
        transportRolloverAction.masterOperation(mock(Task.class), rolloverRequest, stateBefore, future);

        response = future.actionGet();
        assertThat(response.getOldIndex(), equalTo("logs-index-000002"));
        assertThat(response.getNewIndex(), equalTo("logs-index-000003"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(1));
        assertThat(response.getConditionStatus().get("[max_docs: 300]"), is(true));
    }

    private IndicesStatsResponse createIndicesStatResponse(String indexName, long totalDocs, long primariesDocs) {
        final CommonStats primaryStats = mock(CommonStats.class);
        when(primaryStats.getDocs()).thenReturn(new DocsStats(primariesDocs, 0, between(1, 10000)));

        final CommonStats totalStats = mock(CommonStats.class);
        when(totalStats.getDocs()).thenReturn(new DocsStats(totalDocs, 0, between(1, 10000)));

        final IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        when(response.getPrimaries()).thenReturn(primaryStats);
        when(response.getTotal()).thenReturn(totalStats);
        final IndexStats indexStats = mock(IndexStats.class);
        when(response.getIndex(indexName)).thenReturn(indexStats);
        when(indexStats.getPrimaries()).thenReturn(primaryStats);
        when(indexStats.getTotal()).thenReturn(totalStats);
        return response;
    }

    private IndicesStatsResponse createAliasToMultipleIndicesStatsResponse(Map<String, IndexStats> indexStats) {
        final IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        final CommonStats primariesStats = new CommonStats();
        final CommonStats totalStats = new CommonStats();
        for (String indexName : indexStats.keySet()) {
            when(response.getIndex(indexName)).thenReturn(indexStats.get(indexName));
            primariesStats.add(indexStats.get(indexName).getPrimaries());
            totalStats.add(indexStats.get(indexName).getTotal());
        }

        when(response.getPrimaries()).thenReturn(primariesStats);
        when(response.getTotal()).thenReturn(totalStats);
        return response;
    }

    private IndexStats createIndexStats(long primaries, long total) {
        final CommonStats primariesCommonStats = mock(CommonStats.class);
        when(primariesCommonStats.getDocs()).thenReturn(new DocsStats(primaries, 0, between(1, 10000)));

        final CommonStats totalCommonStats = mock(CommonStats.class);
        when(totalCommonStats.getDocs()).thenReturn(new DocsStats(total, 0, between(1, 10000)));

        IndexStats indexStats = mock(IndexStats.class);
        when(indexStats.getPrimaries()).thenReturn(primariesCommonStats);
        when(indexStats.getTotal()).thenReturn(totalCommonStats);
        return indexStats;
    }

    private static IndexMetaData createMetaData(String indexName) {
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        return IndexMetaData.builder(indexName)
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
    }

    private static Condition<?> createTestCondition() {
        final Condition<?> condition = mock(Condition.class);
        when(condition.evaluate(any())).thenReturn(new Condition.Result(condition, true));
        return condition;
    }

    public static IndicesStatsResponse randomIndicesStatsResponse(final IndexMetaData[] indices) {
        List<ShardStats> shardStats = new ArrayList<>();
        for (final IndexMetaData index : indices) {
            int numShards = randomIntBetween(1, 3);
            int primaryIdx = randomIntBetween(-1, numShards - 1); // -1 means there is no primary shard.
            for (int i = 0; i < numShards; i++) {
                ShardId shardId = new ShardId(index.getIndex(), i);
                boolean primary = (i == primaryIdx);
                Path path = createTempDir().resolve("indices").resolve(index.getIndexUUID()).resolve(String.valueOf(i));
                ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, primary,
                    primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
                );
                shardRouting = shardRouting.initialize("node-0", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                shardRouting = shardRouting.moveToStarted();
                CommonStats stats = new CommonStats();
                stats.fieldData = new FieldDataStats();
                stats.queryCache = new QueryCacheStats();
                stats.docs = new DocsStats();
                stats.store = new StoreStats();
                stats.indexing = new IndexingStats();
                stats.search = new SearchStats();
                stats.segments = new SegmentsStats();
                stats.merge = new MergeStats();
                stats.refresh = new RefreshStats();
                stats.completion = new CompletionStats();
                stats.requestCache = new RequestCacheStats();
                stats.get = new GetStats();
                stats.flush = new FlushStats();
                stats.warmer = new WarmerStats();
                shardStats.add(new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, null, null));
            }
        }
        return IndicesStatsTests.newIndicesStatsResponse(
            shardStats.toArray(new ShardStats[shardStats.size()]), shardStats.size(), shardStats.size(), 0, emptyList()
        );
    }
}
