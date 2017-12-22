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
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.action.admin.indices.rollover.TransportRolloverAction.evaluateConditions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TransportRolloverActionTests extends ESTestCase {

    public void testDocStatsSelectionFromPrimariesOnly() throws Exception {
        long docsInPrimaryShards = 100;
        long docsInShards = 200;

        final Condition condition = createTestCondition();
        evaluateConditions(Sets.newHashSet(condition), createMetaData(), createIndicesStatResponse(docsInShards, docsInPrimaryShards));
        final ArgumentCaptor<Condition.Stats> argument = ArgumentCaptor.forClass(Condition.Stats.class);
        verify(condition).evaluate(argument.capture());

        assertEquals(docsInPrimaryShards, argument.getValue().numDocs);
    }

    public void testEvaluateConditions() throws Exception {
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
        final Set<Condition> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition, maxSizeCondition);
        Set<Condition.Result> results = evaluateConditions(conditions,
            new DocsStats(matchMaxDocs, 0L, ByteSizeUnit.MB.toBytes(120)), metaData);
        assertThat(results.size(), equalTo(3));
        for (Condition.Result result : results) {
            assertThat(result.matched, equalTo(true));
        }

        results = evaluateConditions(conditions, new DocsStats(notMatchMaxDocs, 0, notMatchMaxSize.getBytes()), metaData);
        assertThat(results.size(), equalTo(3));
        for (Condition.Result result : results) {
            if (result.condition instanceof MaxAgeCondition) {
                assertThat(result.matched, equalTo(true));
            } else if (result.condition instanceof MaxDocsCondition) {
                assertThat(result.matched, equalTo(false));
            } else if (result.condition instanceof MaxSizeCondition) {
                assertThat(result.matched, equalTo(false));
            } else {
                fail("unknown condition result found " + result.condition);
            }
        }
    }

    public void testEvaluateWithoutDocStats() throws Exception {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(randomNonNegativeLong());
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(randomIntBetween(1, 3)));
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong()));

        Set<Condition> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition, maxSizeCondition);
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
        Set<Condition.Result> results = evaluateConditions(conditions, null, metaData);
        assertThat(results.size(), equalTo(3));

        for (Condition.Result result : results) {
            if (result.condition instanceof MaxAgeCondition) {
                assertThat(result.matched, equalTo(true));
            } else if (result.condition instanceof MaxDocsCondition) {
                assertThat(result.matched, equalTo(false));
            } else if (result.condition instanceof MaxSizeCondition) {
                assertThat(result.matched, equalTo(false));
            } else {
                fail("unknown condition result found " + result.condition);
            }
        }
    }

    public void testCreateUpdateAliasRequest() throws Exception {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(sourceAlias, targetIndex);
        final IndicesAliasesClusterStateUpdateRequest updateRequest =
            TransportRolloverAction.prepareRolloverAliasesUpdateRequest(sourceIndex, targetIndex, rolloverRequest);

        List<AliasAction> actions = updateRequest.actions();
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
                throw new AssertionError("Unknow index [" + action.getIndex() + "]");
            }
        }
        assertTrue(foundAdd);
        assertTrue(foundRemove);
    }

    public void testValidation() throws Exception {
        String index1 = randomAlphaOfLength(10);
        String alias = randomAlphaOfLength(10);
        String index2 = randomAlphaOfLength(10);
        String aliasWithMultipleIndices = randomAlphaOfLength(10);
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        final MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder(index1)
                .settings(settings)
                .putAlias(AliasMetaData.builder(alias))
                .putAlias(AliasMetaData.builder(aliasWithMultipleIndices))
            )
            .put(IndexMetaData.builder(index2)
                .settings(settings)
                .putAlias(AliasMetaData.builder(aliasWithMultipleIndices))
            ).build();

        expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.validate(metaData, new RolloverRequest(aliasWithMultipleIndices,
                randomAlphaOfLength(10))));
        expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.validate(metaData, new RolloverRequest(randomFrom(index1, index2),
                randomAlphaOfLength(10))));
        expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.validate(metaData, new RolloverRequest(randomAlphaOfLength(5),
                randomAlphaOfLength(10)))
        );
        TransportRolloverAction.validate(metaData, new RolloverRequest(alias, randomAlphaOfLength(10)));
    }

    public void testGenerateRolloverIndexName() throws Exception {
        String invalidIndexName = randomAlphaOfLength(10) + "A";
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);
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

    public void testCreateIndexRequest() throws Exception {
        String alias = randomAlphaOfLength(10);
        String rolloverIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(alias, randomAlphaOfLength(10));
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        rolloverRequest.setWaitForActiveShards(activeShardCount);
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

    private IndicesStatsResponse createIndicesStatResponse(long totalDocs, long primaryDocs) {
        final CommonStats primaryStats = mock(CommonStats.class);
        when(primaryStats.getDocs()).thenReturn(new DocsStats(primaryDocs, 0, between(1, 10000)));

        final CommonStats totalStats = mock(CommonStats.class);
        when(totalStats.getDocs()).thenReturn(new DocsStats(totalDocs, 0, between(1, 10000)));

        final IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        when(response.getPrimaries()).thenReturn(primaryStats);
        when(response.getTotal()).thenReturn(totalStats);

        return response;
    }

    private IndexMetaData createMetaData() {
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        return IndexMetaData.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
    }

    private Condition createTestCondition() {
        final Condition condition = mock(Condition.class);
        when(condition.evaluate(any())).thenReturn(new Condition.Result(condition, true));
        return condition;
    }
}
