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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class RolloverIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }


    public void testRolloverOnEmptyIndex() throws Exception {
        assertAcked(prepareCreate("test_index-1").addAlias(new Alias("test_alias")).get());
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test_index-1"));
        assertThat(response.getNewIndex(), equalTo("test_index-000002"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetaData oldIndex = state.metaData().index("test_index-1");
        assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetaData newIndex = state.metaData().index("test_index-000002");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
    }

    public void testRollover() throws Exception {
        assertAcked(prepareCreate("test_index-2").addAlias(new Alias("test_alias")).get());
        index("test_index-2", "type1", "1", "field", "value");
        flush("test_index-2");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetaData oldIndex = state.metaData().index("test_index-2");
        assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetaData newIndex = state.metaData().index("test_index-000003");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
    }

    public void testRolloverWithIndexSettings() throws Exception {
        assertAcked(prepareCreate("test_index-2").addAlias(new Alias("test_alias")).get());
        index("test_index-2", "type1", "1", "field", "value");
        flush("test_index-2");
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias")
            .settings(settings).alias(new Alias("extra_alias")).get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetaData oldIndex = state.metaData().index("test_index-2");
        assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetaData newIndex = state.metaData().index("test_index-000003");
        assertThat(newIndex.getNumberOfShards(), equalTo(1));
        assertThat(newIndex.getNumberOfReplicas(), equalTo(0));
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertTrue(newIndex.getAliases().containsKey("extra_alias"));
    }

    public void testRolloverDryRun() throws Exception {
        assertAcked(prepareCreate("test_index-1").addAlias(new Alias("test_alias")).get());
        index("test_index-1", "type1", "1", "field", "value");
        flush("test_index-1");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").dryRun(true).get();
        assertThat(response.getOldIndex(), equalTo("test_index-1"));
        assertThat(response.getNewIndex(), equalTo("test_index-000002"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetaData oldIndex = state.metaData().index("test_index-1");
        assertTrue(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetaData newIndex = state.metaData().index("test_index-000002");
        assertNull(newIndex);
    }

    public void testRolloverConditionsNotMet() throws Exception {
        assertAcked(prepareCreate("test_index-0").addAlias(new Alias("test_alias")).get());
        index("test_index-0", "type1", "1", "field", "value");
        flush("test_index-0");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias")
            .addMaxIndexSizeCondition(new ByteSizeValue(10, ByteSizeUnit.MB))
            .addMaxIndexAgeCondition(TimeValue.timeValueHours(4)).get();
        assertThat(response.getOldIndex(), equalTo("test_index-0"));
        assertThat(response.getNewIndex(), equalTo("test_index-000001"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(2));


        assertThat(response.getConditionStatus(), everyItem(hasProperty("value", is(false))));
        Set<String> conditions = response.getConditionStatus().stream()
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        assertThat(conditions, containsInAnyOrder(
            new MaxSizeCondition(new ByteSizeValue(10, ByteSizeUnit.MB)).toString(),
            new MaxAgeCondition(TimeValue.timeValueHours(4)).toString()));

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetaData oldIndex = state.metaData().index("test_index-0");
        assertTrue(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetaData newIndex = state.metaData().index("test_index-000001");
        assertNull(newIndex);
    }

    public void testRolloverWithNewIndexName() throws Exception {
        assertAcked(prepareCreate("test_index").addAlias(new Alias("test_alias")).get());
        index("test_index", "type1", "1", "field", "value");
        flush("test_index");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias")
            .setNewIndexName("test_new_index").get();
        assertThat(response.getOldIndex(), equalTo("test_index"));
        assertThat(response.getNewIndex(), equalTo("test_new_index"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetaData oldIndex = state.metaData().index("test_index");
        assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetaData newIndex = state.metaData().index("test_new_index");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
    }

    public void testRolloverOnExistingIndex() throws Exception {
        assertAcked(prepareCreate("test_index-0").addAlias(new Alias("test_alias")).get());
        index("test_index-0", "type1", "1", "field", "value");
        assertAcked(prepareCreate("test_index-000001").get());
        index("test_index-000001", "type1", "1", "field", "value");
        flush("test_index-0", "test_index-000001");
        try {
            client().admin().indices().prepareRolloverIndex("test_alias").get();
            fail("expected failure due to existing rollover index");
        } catch (ResourceAlreadyExistsException e) {
            assertThat(e.getIndex().getName(), equalTo("test_index-000001"));
        }
    }

    public void testRolloverWithDateMath() {
        DateTime now = new DateTime(DateTimeZone.UTC);
        String index = "test-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now) + "-1";
        String dateMathExp = "<test-{now/d}-1>";
        assertAcked(prepareCreate(dateMathExp).addAlias(new Alias("test_alias")).get());
        ensureGreen(index);
        // now we modify the provided name such that we can test that the pattern is carried on
        client().admin().indices().prepareClose(index).get();
        client().admin().indices().prepareUpdateSettings(index).setSettings(Settings.builder()
            .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME,
            "<test-{now/M{YYYY.MM}}-1>")).get();

        client().admin().indices().prepareOpen(index).get();
        ensureGreen(index);
        RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo(index));
        assertThat(response.getNewIndex(), equalTo("test-" + DateTimeFormat.forPattern("YYYY.MM").print(now) + "-000002"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));

        response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test-" + DateTimeFormat.forPattern("YYYY.MM").print(now) + "-000002"));
        assertThat(response.getNewIndex(), equalTo("test-" + DateTimeFormat.forPattern("YYYY.MM").print(now) + "-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));

        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(response.getOldIndex(),
            response.getNewIndex()).get();
        assertEquals("<test-{now/M{YYYY.MM}}-000002>", getSettingsResponse.getSetting(response.getOldIndex(),
            IndexMetaData.SETTING_INDEX_PROVIDED_NAME));
        assertEquals("<test-{now/M{YYYY.MM}}-000003>", getSettingsResponse.getSetting(response.getNewIndex(),
            IndexMetaData.SETTING_INDEX_PROVIDED_NAME));

        response = client().admin().indices().prepareRolloverIndex("test_alias").setNewIndexName("<test-{now/d}-000004>").get();
        assertThat(response.getOldIndex(), equalTo("test-" + DateTimeFormat.forPattern("YYYY.MM").print(now) + "-000003"));
        assertThat(response.getNewIndex(), equalTo("test-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now) + "-000004"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
    }

    public void testRolloverMaxSize() throws Exception {
        assertAcked(prepareCreate("test-1").addAlias(new Alias("test_alias")).get());
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            index("test-1", "doc", Integer.toString(i), "field", "foo-" + i);
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
        }

        // A small max_size
        {
            final RolloverResponse response = client().admin().indices()
                .prepareRolloverIndex("test_alias")
                .addMaxIndexSizeCondition(new ByteSizeValue(randomIntBetween(1, 20), ByteSizeUnit.BYTES))
                .get();
            assertThat(response.getOldIndex(), equalTo("test-1"));
            assertThat(response.getNewIndex(), equalTo("test-000002"));
            assertThat("Should rollover with a small max_size condition", response.isRolledOver(), equalTo(true));
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
        }
    }
}
