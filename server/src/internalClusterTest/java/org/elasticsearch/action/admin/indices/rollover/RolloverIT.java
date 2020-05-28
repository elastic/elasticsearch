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

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class RolloverIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }


    public void testRolloverOnEmptyIndex() throws Exception {
        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-1").addAlias(testAlias).get());
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test_index-1"));
        assertThat(response.getNewIndex(), equalTo("test_index-000002"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-1");
        if (explicitWriteIndex) {
            assertTrue(oldIndex.getAliases().containsKey("test_alias"));
            assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        }
        final IndexMetadata newIndex = state.metadata().index("test_index-000002");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
    }

    public void testRollover() throws Exception {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        assertAcked(prepareCreate("test_index-2").addAlias(new Alias("test_alias")).get());
        indexDoc("test_index-2", "1", "field", "value");
        flush("test_index-2");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-2");
        assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getAlias(), equalTo("test_alias"));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getMetConditions(), is(empty()));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L))));
    }

    public void testRolloverWithExplicitWriteIndex() throws Exception {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        assertAcked(prepareCreate("test_index-2").addAlias(new Alias("test_alias").writeIndex(true)).get());
        indexDoc("test_index-2", "1", "field", "value");
        flush("test_index-2");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-2");
        assertTrue(oldIndex.getAliases().containsKey("test_alias"));
        assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertTrue(newIndex.getAliases().get("test_alias").writeIndex());
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getAlias(), equalTo("test_alias"));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getMetConditions(), is(empty()));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L))));
    }

    public void testRolloverWithNoWriteIndex() {
        Boolean firstIsWriteIndex = randomFrom(false, null);
        assertAcked(prepareCreate("index1").addAlias(new Alias("alias").writeIndex(firstIsWriteIndex)).get());
        if (firstIsWriteIndex == null) {
            assertAcked(prepareCreate("index2").addAlias(new Alias("alias").writeIndex(randomFrom(false, null))).get());
        }
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> client().admin().indices().prepareRolloverIndex("alias").dryRun(randomBoolean()).get());
        assertThat(exception.getMessage(), equalTo("rollover target [alias] does not point to a write index"));
    }

    public void testRolloverWithIndexSettings() throws Exception {
        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-2").addAlias(testAlias).get());
        indexDoc("test_index-2", "1", "field", "value");
        flush("test_index-2");
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias")
            .settings(settings).alias(new Alias("extra_alias")).get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-2");
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertThat(newIndex.getNumberOfShards(), equalTo(1));
        assertThat(newIndex.getNumberOfReplicas(), equalTo(0));
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertTrue(newIndex.getAliases().containsKey("extra_alias"));
        if (explicitWriteIndex) {
            assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
            assertTrue(newIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        }
    }

    public void testRolloverDryRun() throws Exception {
        assertAcked(prepareCreate("test_index-1").addAlias(new Alias("test_alias")).get());
        indexDoc("test_index-1", "1", "field", "value");
        flush("test_index-1");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").dryRun(true).get();
        assertThat(response.getOldIndex(), equalTo("test_index-1"));
        assertThat(response.getNewIndex(), equalTo("test_index-000002"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-1");
        assertTrue(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetadata newIndex = state.metadata().index("test_index-000002");
        assertNull(newIndex);
    }

    public void testRolloverConditionsNotMet() throws Exception {
        boolean explicitWriteIndex = randomBoolean();
        Alias testAlias = new Alias("test_alias");
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-0").addAlias(testAlias).get());
        indexDoc("test_index-0", "1", "field", "value");
        flush("test_index-0");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias")
            .addMaxIndexSizeCondition(new ByteSizeValue(10, ByteSizeUnit.MB))
            .addMaxIndexAgeCondition(TimeValue.timeValueHours(4)).get();
        assertThat(response.getOldIndex(), equalTo("test_index-0"));
        assertThat(response.getNewIndex(), equalTo("test_index-000001"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(2));
        assertThat(response.getConditionStatus().values(), everyItem(is(false)));
        Set<String> conditions = response.getConditionStatus().keySet();
        assertThat(conditions, containsInAnyOrder(
            new MaxSizeCondition(new ByteSizeValue(10, ByteSizeUnit.MB)).toString(),
            new MaxAgeCondition(TimeValue.timeValueHours(4)).toString()));

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-0");
        assertTrue(oldIndex.getAliases().containsKey("test_alias"));
        if (explicitWriteIndex) {
            assertTrue(oldIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertNull(oldIndex.getAliases().get("test_alias").writeIndex());
        }
        final IndexMetadata newIndex = state.metadata().index("test_index-000001");
        assertNull(newIndex);
    }

    public void testRolloverWithNewIndexName() throws Exception {
        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index").addAlias(testAlias).get());
        indexDoc("test_index", "1", "field", "value");
        flush("test_index");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias")
            .setNewIndexName("test_new_index").get();
        assertThat(response.getOldIndex(), equalTo("test_index"));
        assertThat(response.getNewIndex(), equalTo("test_new_index"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index");
        final IndexMetadata newIndex = state.metadata().index("test_new_index");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        if (explicitWriteIndex) {
            assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
            assertTrue(newIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        }
    }

    public void testRolloverOnExistingIndex() throws Exception {
        assertAcked(prepareCreate("test_index-0").addAlias(new Alias("test_alias")).get());
        indexDoc("test_index-0", "1", "field", "value");
        assertAcked(prepareCreate("test_index-000001").get());
        indexDoc("test_index-000001", "1", "field", "value");
        flush("test_index-0", "test_index-000001");
        try {
            client().admin().indices().prepareRolloverIndex("test_alias").get();
            fail("expected failure due to existing rollover index");
        } catch (ResourceAlreadyExistsException e) {
            assertThat(e.getIndex().getName(), equalTo("test_index-000001"));
        }
    }

    public void testRolloverWithDateMath() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        assumeTrue("only works on the same day", now.plusMinutes(5).getDayOfYear() == now.getDayOfYear());
        String index = "test-" + DateFormatter.forPattern("yyyy.MM.dd").format(now) + "-1";
        String dateMathExp = "<test-{now/d}-1>";
        assertAcked(prepareCreate(dateMathExp).addAlias(new Alias("test_alias")).get());
        ensureGreen(index);
        // now we modify the provided name such that we can test that the pattern is carried on
        client().admin().indices().prepareClose(index).get();
        client().admin().indices().prepareUpdateSettings(index).setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME,
            "<test-{now/M{yyyy.MM}}-1>")).get();

        client().admin().indices().prepareOpen(index).get();
        ensureGreen(index);
        RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo(index));
        assertThat(response.getNewIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000002"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));

        response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000002"));
        assertThat(response.getNewIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));

        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(response.getOldIndex(),
            response.getNewIndex()).get();
        assertEquals("<test-{now/M{yyyy.MM}}-000002>", getSettingsResponse.getSetting(response.getOldIndex(),
            IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals("<test-{now/M{yyyy.MM}}-000003>", getSettingsResponse.getSetting(response.getNewIndex(),
            IndexMetadata.SETTING_INDEX_PROVIDED_NAME));

        response = client().admin().indices().prepareRolloverIndex("test_alias").setNewIndexName("<test-{now/d}-000004>").get();
        assertThat(response.getOldIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000003"));
        assertThat(response.getNewIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM.dd").format(now) + "-000004"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
    }

    public void testRolloverMaxSize() throws Exception {
        assertAcked(prepareCreate("test-1").addAlias(new Alias("test_alias")).get());
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            indexDoc("test-1", Integer.toString(i), "field", "foo-" + i);
        }
        flush("test-1");
        refresh("test_alias");

        // A large max_size
        {
            final RolloverResponse response = client().admin().indices()
                .prepareRolloverIndex("test_alias")
                .addMaxIndexSizeCondition(new ByteSizeValue(randomIntBetween(100, 50 * 1024), ByteSizeUnit.MB))
                .get();
            assertThat(response.getOldIndex(), equalTo("test-1"));
            assertThat(response.getNewIndex(), equalTo("test-000002"));
            assertThat("No rollover with a large max_size condition", response.isRolledOver(), equalTo(false));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-1");
            assertThat(oldIndex.getRolloverInfos().size(), equalTo(0));
        }

        // A small max_size
        {
            ByteSizeValue maxSizeValue = new ByteSizeValue(randomIntBetween(1, 20), ByteSizeUnit.BYTES);
            long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
            final RolloverResponse response = client().admin().indices()
                .prepareRolloverIndex("test_alias")
                .addMaxIndexSizeCondition(maxSizeValue)
                .get();
            assertThat(response.getOldIndex(), equalTo("test-1"));
            assertThat(response.getNewIndex(), equalTo("test-000002"));
            assertThat("Should rollover with a small max_size condition", response.isRolledOver(), equalTo(true));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-1");
            List<Condition<?>> metConditions = oldIndex.getRolloverInfos().get("test_alias").getMetConditions();
            assertThat(metConditions.size(), equalTo(1));
            assertThat(metConditions.get(0).toString(), equalTo(new MaxSizeCondition(maxSizeValue).toString()));
            assertThat(oldIndex.getRolloverInfos().get("test_alias").getTime(),
                is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L))));
        }

        // An empty index
        {
            final RolloverResponse response = client().admin().indices()
                .prepareRolloverIndex("test_alias")
                .addMaxIndexSizeCondition(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES))
                .get();
            assertThat(response.getOldIndex(), equalTo("test-000002"));
            assertThat(response.getNewIndex(), equalTo("test-000003"));
            assertThat("No rollover with an empty index", response.isRolledOver(), equalTo(false));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-000002");
            assertThat(oldIndex.getRolloverInfos().size(), equalTo(0));
        }
    }

    public void testRejectIfAliasFoundInTemplate() throws Exception {
        client().admin().indices().preparePutTemplate("logs")
            .setPatterns(Collections.singletonList("logs-*")).addAlias(new Alias("logs-write")).get();
        assertAcked(client().admin().indices().prepareCreate("logs-000001").get());
        ensureYellow("logs-write");
        final IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
            () -> client().admin().indices().prepareRolloverIndex("logs-write").addMaxIndexSizeCondition(new ByteSizeValue(1)).get());
        assertThat(error.getMessage(), equalTo(
            "Rollover alias [logs-write] can point to multiple indices, found duplicated alias [[logs-write]] in index template [logs]"));
    }

    public void testRolloverWithClosedIndexInAlias() throws Exception {
        final String aliasName = "alias";
        final String openNonwriteIndex = "open-index-nonwrite";
        final String closedIndex = "closed-index-nonwrite";
        final String writeIndexPrefix = "write-index-";
        assertAcked(prepareCreate(openNonwriteIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(closedIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(writeIndexPrefix + "000001").addAlias(new Alias(aliasName).writeIndex(true)).get());

        index(closedIndex, null, "{\"foo\": \"bar\"}");
        index(aliasName, null, "{\"foo\": \"bar\"}");
        index(aliasName, null, "{\"foo\": \"bar\"}");
        refresh(aliasName);

        assertAcked(client().admin().indices().prepareClose(closedIndex).get());

        RolloverResponse rolloverResponse = client().admin().indices().prepareRolloverIndex(aliasName)
            .addMaxIndexDocsCondition(1)
            .get();
        assertTrue(rolloverResponse.isRolledOver());
        assertEquals(writeIndexPrefix + "000001", rolloverResponse.getOldIndex());
        assertEquals(writeIndexPrefix + "000002", rolloverResponse.getNewIndex());
    }

    public void testRolloverWithClosedWriteIndex() throws Exception {
        final String aliasName = "alias";
        final String openNonwriteIndex = "open-index-nonwrite";
        final String closedIndex = "closed-index-nonwrite";
        final String writeIndexPrefix = "write-index-";
        assertAcked(prepareCreate(openNonwriteIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(closedIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(writeIndexPrefix + "000001").addAlias(new Alias(aliasName).writeIndex(true)).get());

        index(closedIndex, null, "{\"foo\": \"bar\"}");
        index(aliasName, null, "{\"foo\": \"bar\"}");
        index(aliasName, null, "{\"foo\": \"bar\"}");
        refresh(aliasName);

        assertAcked(client().admin().indices().prepareClose(closedIndex).get());
        assertAcked(client().admin().indices().prepareClose(writeIndexPrefix + "000001").get());
        ensureGreen(aliasName);

        RolloverResponse rolloverResponse = client().admin().indices().prepareRolloverIndex(aliasName)
            .addMaxIndexDocsCondition(1)
            .get();
        assertTrue(rolloverResponse.isRolledOver());
        assertEquals(writeIndexPrefix + "000001", rolloverResponse.getOldIndex());
        assertEquals(writeIndexPrefix + "000002", rolloverResponse.getNewIndex());
    }

    public void testRolloverWithHiddenAliasesAndExplicitWriteIndex() {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        final String indexNamePrefix = "test_index_hidden-";
        final String firstIndexName = indexNamePrefix + "000001";
        final String secondIndexName = indexNamePrefix + "000002";

        final String aliasName = "test_alias";
        assertAcked(prepareCreate(firstIndexName).addAlias(new Alias(aliasName).writeIndex(true).isHidden(true)).get());
        indexDoc(aliasName, "1", "field", "value");
        refresh();
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex(aliasName).get();
        assertThat(response.getOldIndex(), equalTo(firstIndexName));
        assertThat(response.getNewIndex(), equalTo(secondIndexName));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index(firstIndexName);
        assertTrue(oldIndex.getAliases().containsKey(aliasName));
        assertTrue(oldIndex.getAliases().get(aliasName).isHidden());
        assertFalse(oldIndex.getAliases().get(aliasName).writeIndex());
        final IndexMetadata newIndex = state.metadata().index(secondIndexName);
        assertTrue(newIndex.getAliases().containsKey(aliasName));
        assertTrue(newIndex.getAliases().get(aliasName).isHidden());
        assertTrue(newIndex.getAliases().get(aliasName).writeIndex());
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getAlias(), equalTo(aliasName));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getMetConditions(), is(empty()));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L))));
    }

    public void testRolloverWithHiddenAliasesAndImplicitWriteIndex() {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        final String indexNamePrefix = "test_index_hidden-";
        final String firstIndexName = indexNamePrefix + "000001";
        final String secondIndexName = indexNamePrefix + "000002";

        final String aliasName = "test_alias";
        assertAcked(prepareCreate(firstIndexName).addAlias(new Alias(aliasName).isHidden(true)).get());
        indexDoc(aliasName, "1", "field", "value");
        refresh();
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex(aliasName).get();
        assertThat(response.getOldIndex(), equalTo(firstIndexName));
        assertThat(response.getNewIndex(), equalTo(secondIndexName));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index(firstIndexName);
        assertFalse(oldIndex.getAliases().containsKey(aliasName));
        final IndexMetadata newIndex = state.metadata().index(secondIndexName);
        assertTrue(newIndex.getAliases().containsKey(aliasName));
        assertTrue(newIndex.getAliases().get(aliasName).isHidden());
        assertThat(newIndex.getAliases().get(aliasName).writeIndex(), nullValue());
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getAlias(), equalTo(aliasName));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getMetConditions(), is(empty()));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L))));
    }
}
