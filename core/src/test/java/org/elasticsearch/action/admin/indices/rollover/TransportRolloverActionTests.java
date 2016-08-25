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
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.action.admin.indices.rollover.TransportRolloverAction.evaluateConditions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class TransportRolloverActionTests extends ESTestCase {

    public void testEvaluateConditions() throws Exception {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(2));
        long matchMaxDocs = randomIntBetween(100, 1000);
        long notMatchMaxDocs = randomIntBetween(0, 99);
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        final IndexMetaData metaData = IndexMetaData.builder(randomAsciiOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
        final HashSet<Condition> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition);
        Set<Condition.Result> results = evaluateConditions(conditions, new DocsStats(matchMaxDocs, 0L), metaData);
        assertThat(results.size(), equalTo(2));
        for (Condition.Result result : results) {
            assertThat(result.matched, equalTo(true));
        }
        results = evaluateConditions(conditions, new DocsStats(notMatchMaxDocs, 0), metaData);
        assertThat(results.size(), equalTo(2));
        for (Condition.Result result : results) {
            if (result.condition instanceof MaxAgeCondition) {
                assertThat(result.matched, equalTo(true));
            } else if (result.condition instanceof MaxDocsCondition) {
                assertThat(result.matched, equalTo(false));
            } else {
                fail("unknown condition result found " + result.condition);
            }
        }
        results = evaluateConditions(conditions, null, metaData);
        assertThat(results.size(), equalTo(2));
        for (Condition.Result result : results) {
            if (result.condition instanceof MaxAgeCondition) {
                assertThat(result.matched, equalTo(true));
            } else if (result.condition instanceof MaxDocsCondition) {
                assertThat(result.matched, equalTo(false));
            } else {
                fail("unknown condition result found " + result.condition);
            }
        }
    }

    public void testCreateUpdateAliasRequest() throws Exception {
        String sourceAlias = randomAsciiOfLength(10);
        String sourceIndex = randomAsciiOfLength(10);
        String targetIndex = randomAsciiOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(sourceAlias, targetIndex);
        final IndicesAliasesClusterStateUpdateRequest updateRequest =
            TransportRolloverAction.prepareRolloverAliasesUpdateRequest(sourceIndex, targetIndex, rolloverRequest);

        final AliasAction[] actions = updateRequest.actions();
        assertThat(actions.length, equalTo(2));
        boolean foundAdd = false;
        boolean foundRemove = false;
        for (AliasAction action : actions) {
            if (action.actionType() == AliasAction.Type.ADD) {
                foundAdd = true;
                assertThat(action.index(), equalTo(targetIndex));
                assertThat(action.alias(), equalTo(sourceAlias));
            } else if (action.actionType() == AliasAction.Type.REMOVE) {
                foundRemove = true;
                assertThat(action.index(), equalTo(sourceIndex));
                assertThat(action.alias(), equalTo(sourceAlias));
            }
        }
        assertTrue(foundAdd);
        assertTrue(foundRemove);
    }

    public void testValidation() throws Exception {
        String index1 = randomAsciiOfLength(10);
        String alias = randomAsciiOfLength(10);
        String index2 = randomAsciiOfLength(10);
        String aliasWithMultipleIndices = randomAsciiOfLength(10);
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
                randomAsciiOfLength(10))));
        expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.validate(metaData, new RolloverRequest(randomFrom(index1, index2),
                randomAsciiOfLength(10))));
        expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.validate(metaData, new RolloverRequest(randomAsciiOfLength(5),
                randomAsciiOfLength(10)))
        );
        TransportRolloverAction.validate(metaData, new RolloverRequest(alias, randomAsciiOfLength(10)));
    }

    public void testGenerateRolloverIndexName() throws Exception {
        String invalidIndexName = randomAsciiOfLength(10) + "A";
        expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.generateRolloverIndexName(invalidIndexName));
        int num = randomIntBetween(0, 100);
        final String indexPrefix = randomAsciiOfLength(10);
        String indexEndingInNumbers = indexPrefix + "-" + num;
        assertThat(TransportRolloverAction.generateRolloverIndexName(indexEndingInNumbers),
            equalTo(indexPrefix + "-" + String.format(Locale.ROOT, "%06d", num + 1)));
        assertThat(TransportRolloverAction.generateRolloverIndexName("index-name-1"), equalTo("index-name-000002"));
        assertThat(TransportRolloverAction.generateRolloverIndexName("index-name-2"), equalTo("index-name-000003"));
    }

    public void testCreateIndexRequest() throws Exception {
        String alias = randomAsciiOfLength(10);
        String rolloverIndex = randomAsciiOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(alias, randomAsciiOfLength(10));
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
            TransportRolloverAction.prepareCreateIndexRequest(rolloverIndex, rolloverRequest);
        assertThat(createIndexRequest.settings(), equalTo(settings));
        assertThat(createIndexRequest.index(), equalTo(rolloverIndex));
        assertThat(createIndexRequest.cause(), equalTo("rollover_index"));
    }
}
