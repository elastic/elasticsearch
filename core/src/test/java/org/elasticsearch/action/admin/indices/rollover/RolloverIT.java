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

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.CollectionAssertions.hasKey;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class RolloverIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testRolloverOnEmptyIndex() throws Exception {
        assertAcked(prepareCreate("test_index-1").addAlias(new Alias("test_alias")).get());
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.responses().get(0).getOldIndex(), equalTo("test_index-1"));
        assertThat(response.responses().get(0).getNewIndex(), equalTo("test_index-000002"));
        assertThat(response.responses().get(0).isDryRun(), equalTo(false));
        assertThat(response.responses().get(0).isRolledOver(), equalTo(true));
        assertThat(response.responses().get(0).getConditionStatus().size(), equalTo(0));
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
        assertThat(response.responses().get(0).getOldIndex(), equalTo("test_index-2"));
        assertThat(response.responses().get(0).getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.responses().get(0).isDryRun(), equalTo(false));
        assertThat(response.responses().get(0).isRolledOver(), equalTo(true));
        assertThat(response.responses().get(0).getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetaData oldIndex = state.metaData().index("test_index-2");
        assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetaData newIndex = state.metaData().index("test_index-000003");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
    }

    public void testRolloverMultipleAliasesFromWildCard() throws Exception {
        assertAcked(prepareCreate("test1_index-1").addAlias(new Alias("test1_alias")).get());
        assertAcked(prepareCreate("test2_index-1").addAlias(new Alias("test2_alias")).get());

        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test*").get();
        final List<RolloverResponse.SingleAliasRolloverResponse> responses = response.responses();
        // preserve order for matching
        responses.sort(Comparator.comparing(RolloverResponse.SingleAliasRolloverResponse::getAlias));

        assertThat(responses.get(0).getOldIndex(), equalTo("test1_index-1"));
        assertThat(responses.get(0).getNewIndex(), equalTo("test1_index-000002"));
        assertThat(responses.get(0).isDryRun(), equalTo(false));
        assertThat(responses.get(0).isRolledOver(), equalTo(true));
        assertThat(responses.get(0).getConditionStatus().size(), equalTo(0));

        assertThat(responses.get(1).getOldIndex(), equalTo("test2_index-1"));
        assertThat(responses.get(1).getNewIndex(), equalTo("test2_index-000002"));
        assertThat(responses.get(1).isDryRun(), equalTo(false));
        assertThat(responses.get(1).isRolledOver(), equalTo(true));
        assertThat(responses.get(1).getConditionStatus().size(), equalTo(0));

        final MetaData metaData = client().admin().cluster().prepareState().get().getState().metaData();
        assertThat(metaData.index("test1_index-1").getAliases(), not(hasKey("test1_alias")));
        assertThat(metaData.index("test1_index-000002").getAliases(), hasKey("test1_alias"));
        assertThat(metaData.index("test2_index-1").getAliases(), not(hasKey("test2_alias")));
        assertThat(metaData.index("test2_index-000002").getAliases(), hasKey("test2_alias"));
    }

    public void testRolloverMultipleAliasesFromList() throws Exception {
        assertAcked(prepareCreate("test1_index-1").addAlias(new Alias("test1_alias")).get());
        assertAcked(prepareCreate("test2_index-1").addAlias(new Alias("test2_alias")).get());

        client().admin().indices().prepareRolloverIndex("test1_alias", "test2_alias", "missing_alias_will_be_ignored").get();

        final MetaData metaData = client().admin().cluster().prepareState().get().getState().metaData();
        assertThat(metaData.index("test1_index-000002").getAliases(), hasKey("test1_alias"));
        assertThat(metaData.index("test2_index-000002").getAliases(), hasKey("test2_alias"));
    }

    // todo assert both old and new response for sinle

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
        assertThat(response.responses().get(0).getOldIndex(), equalTo("test_index-2"));
        assertThat(response.responses().get(0).getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.responses().get(0).isDryRun(), equalTo(false));
        assertThat(response.responses().get(0).isRolledOver(), equalTo(true));
        assertThat(response.responses().get(0).getConditionStatus().size(), equalTo(0));
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
        assertThat(response.responses().get(0).getOldIndex(), equalTo("test_index-1"));
        assertThat(response.responses().get(0).getNewIndex(), equalTo("test_index-000002"));
        assertThat(response.responses().get(0).isDryRun(), equalTo(true));
        assertThat(response.responses().get(0).isRolledOver(), equalTo(false));
        assertThat(response.responses().get(0).getConditionStatus().size(), equalTo(0));
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
            .addMaxIndexAgeCondition(TimeValue.timeValueHours(4)).get();
        assertThat(response.responses().get(0).getOldIndex(), equalTo("test_index-0"));
        assertThat(response.responses().get(0).getNewIndex(), equalTo("test_index-000001"));
        assertThat(response.responses().get(0).isDryRun(), equalTo(false));
        assertThat(response.responses().get(0).isRolledOver(), equalTo(false));
        assertThat(response.responses().get(0).getConditionStatus().size(), equalTo(1));
        final Map.Entry<String, Boolean> conditionEntry = response.responses().get(0).getConditionStatus().iterator().next();
        assertThat(conditionEntry.getKey(), equalTo(new MaxAgeCondition(TimeValue.timeValueHours(4)).toString()));
        assertThat(conditionEntry.getValue(), equalTo(false));
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
        assertThat(response.responses().get(0).getOldIndex(), equalTo("test_index"));
        assertThat(response.responses().get(0).getNewIndex(), equalTo("test_new_index"));
        assertThat(response.responses().get(0).isDryRun(), equalTo(false));
        assertThat(response.responses().get(0).isRolledOver(), equalTo(true));
        assertThat(response.responses().get(0).getConditionStatus().size(), equalTo(0));
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
        assertThat(response.responses().get(0).getOldIndex(), equalTo(index));
        assertThat(response.responses().get(0).getNewIndex(),
            equalTo("test-" + DateTimeFormat.forPattern("YYYY.MM").print(now) + "-000002"));
        assertThat(response.responses().get(0).isDryRun(), equalTo(false));
        assertThat(response.responses().get(0).isRolledOver(), equalTo(true));
        assertThat(response.responses().get(0).getConditionStatus().size(), equalTo(0));

        response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.responses().get(0).getOldIndex(),
            equalTo("test-" + DateTimeFormat.forPattern("YYYY.MM").print(now) + "-000002"));
        assertThat(response.responses().get(0).getNewIndex(),
            equalTo("test-" + DateTimeFormat.forPattern("YYYY.MM").print(now) + "-000003"));
        assertThat(response.responses().get(0).isDryRun(), equalTo(false));
        assertThat(response.responses().get(0).isRolledOver(), equalTo(true));
        assertThat(response.responses().get(0).getConditionStatus().size(), equalTo(0));

        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(response.responses().get(0).getOldIndex(),
            response.responses().get(0).getNewIndex()).get();
        assertEquals("<test-{now/M{YYYY.MM}}-000002>", getSettingsResponse.getSetting(response.responses().get(0).getOldIndex(),
            IndexMetaData.SETTING_INDEX_PROVIDED_NAME));
        assertEquals("<test-{now/M{YYYY.MM}}-000003>", getSettingsResponse.getSetting(response.responses().get(0).getNewIndex(),
            IndexMetaData.SETTING_INDEX_PROVIDED_NAME));

        response = client().admin().indices().prepareRolloverIndex("test_alias").setNewIndexName("<test-{now/d}-000004>").get();
        assertThat(response.responses().get(0).getOldIndex(),
            equalTo("test-" + DateTimeFormat.forPattern("YYYY.MM").print(now) + "-000003"));
        assertThat(response.responses().get(0).getNewIndex(),
            equalTo("test-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now) + "-000004"));
        assertThat(response.responses().get(0).isDryRun(), equalTo(false));
        assertThat(response.responses().get(0).isRolledOver(), equalTo(true));
        assertThat(response.responses().get(0).getConditionStatus().size(), equalTo(0));
    }
}
