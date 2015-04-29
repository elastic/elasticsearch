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

package org.elasticsearch.action.admin.indices.get;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.warmer.IndexWarmersMetaData.Entry;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.*;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class GetIndexTests extends ElasticsearchIntegrationTest {

    private static final String[] allFeatures = { "_alias", "_aliases", "_mapping", "_mappings", "_settings", "_warmer", "_warmers" };

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").addAlias(new Alias("alias_idx")).addMapping("type1", "{\"type1\":{}}")
                .setSettings(ImmutableSettings.builder().put("number_of_shards", 1)).get());
        ensureSearchable("idx");
        assertAcked(client().admin().indices().preparePutWarmer("warmer1").setSearchRequest(client().prepareSearch("idx")).get());
        createIndex("empty_idx");
        ensureSearchable("idx", "empty_idx");
    }

    @Test
    public void testSimple() {
        GetIndexResponse response = client().admin().indices().prepareGetIndex().addIndices("idx").get();
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertAliases(response, "idx");
        assertMappings(response, "idx");
        assertSettings(response, "idx");
        assertWarmers(response, "idx");
    }

    @Test(expected=IndexMissingException.class)
    public void testSimpleUnknownIndex() {
        client().admin().indices().prepareGetIndex().addIndices("missing_idx").get();
    }

    @Test
    public void testEmpty() {
        GetIndexResponse response = client().admin().indices().prepareGetIndex().addIndices("empty_idx").get();
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("empty_idx"));
        assertEmptyAliases(response);
        assertEmptyOrOnlyDefaultMappings(response, "empty_idx");
        assertNonEmptySettings(response, "empty_idx");
        assertEmptyWarmers(response);
    }

    @Test
    public void testSimpleMapping() {
        GetIndexResponse response = runWithRandomFeatureMethod(client().admin().indices().prepareGetIndex().addIndices("idx"),
                Feature.MAPPINGS);
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertMappings(response, "idx");
        assertEmptyAliases(response);
        assertEmptySettings(response);
        assertEmptyWarmers(response);
    }

    @Test
    public void testSimpleAlias() {
        GetIndexResponse response = runWithRandomFeatureMethod(client().admin().indices().prepareGetIndex().addIndices("idx"),
                Feature.ALIASES);
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertAliases(response, "idx");
        assertEmptyMappings(response);
        assertEmptySettings(response);
        assertEmptyWarmers(response);
    }

    @Test
    public void testSimpleSettings() {
        GetIndexResponse response = runWithRandomFeatureMethod(client().admin().indices().prepareGetIndex().addIndices("idx"),
                Feature.SETTINGS);
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertSettings(response, "idx");
        assertEmptyAliases(response);
        assertEmptyMappings(response);
        assertEmptyWarmers(response);
    }

    @Test
    public void testSimpleWarmer() {
        GetIndexResponse response = runWithRandomFeatureMethod(client().admin().indices().prepareGetIndex().addIndices("idx"),
                Feature.WARMERS);
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertWarmers(response, "idx");
        assertEmptyAliases(response);
        assertEmptyMappings(response);
        assertEmptySettings(response);
    }

    @Test
    public void testSimpleMixedFeatures() {
        int numFeatures = randomIntBetween(1, Feature.values().length);
        List<Feature> features = new ArrayList<Feature>(numFeatures);
        for (int i = 0; i < numFeatures; i++) {
            features.add(randomFrom(Feature.values()));
        }
        GetIndexResponse response = runWithRandomFeatureMethod(client().admin().indices().prepareGetIndex().addIndices("idx"),
                features.toArray(new Feature[features.size()]));
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        if (features.contains(Feature.ALIASES)) {
            assertAliases(response, "idx");
        } else {
            assertEmptyAliases(response);
        }
        if (features.contains(Feature.MAPPINGS)) {
            assertMappings(response, "idx");
        } else {
            assertEmptyMappings(response);
        }
        if (features.contains(Feature.SETTINGS)) {
            assertSettings(response, "idx");
        } else {
            assertEmptySettings(response);
        }
        if (features.contains(Feature.WARMERS)) {
            assertWarmers(response, "idx");
        } else {
            assertEmptyWarmers(response);
        }
    }

    @Test
    public void testEmptyMixedFeatures() {
        int numFeatures = randomIntBetween(1, Feature.values().length);
        List<Feature> features = new ArrayList<Feature>(numFeatures);
        for (int i = 0; i < numFeatures; i++) {
            features.add(randomFrom(Feature.values()));
        }
        GetIndexResponse response = runWithRandomFeatureMethod(client().admin().indices().prepareGetIndex().addIndices("empty_idx"),
                features.toArray(new Feature[features.size()]));
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("empty_idx"));
        assertEmptyAliases(response);
        if (features.contains(Feature.MAPPINGS)) {
            assertEmptyOrOnlyDefaultMappings(response, "empty_idx");
        } else {
            assertEmptyMappings(response);
        }
        if (features.contains(Feature.SETTINGS)) {
            assertNonEmptySettings(response, "empty_idx");
        } else {
            assertEmptySettings(response);
        }
        assertEmptyWarmers(response);
    }

    @Test
    public void testGetIndexWithBlocks() {
        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("idx", block);
                GetIndexResponse response = client().admin().indices().prepareGetIndex().addIndices("idx")
                        .addFeatures(Feature.MAPPINGS, Feature.ALIASES).get();
                String[] indices = response.indices();
                assertThat(indices, notNullValue());
                assertThat(indices.length, equalTo(1));
                assertThat(indices[0], equalTo("idx"));
                assertMappings(response, "idx");
                assertAliases(response, "idx");
            } finally {
                disableIndexBlock("idx", block);
            }
        }

        try {
            enableIndexBlock("idx", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareGetIndex().addIndices("idx").addFeatures(Feature.MAPPINGS, Feature.ALIASES), INDEX_METADATA_BLOCK);
        } finally {
            disableIndexBlock("idx", SETTING_BLOCKS_METADATA);
        }
    }

    private GetIndexResponse runWithRandomFeatureMethod(GetIndexRequestBuilder requestBuilder, Feature... features) {
        if (randomBoolean()) {
            return requestBuilder.addFeatures(features).get();
        } else {
            return requestBuilder.setFeatures(features).get();
        }
    }

    private void assertWarmers(GetIndexResponse response, String indexName) {
        ImmutableOpenMap<String, ImmutableList<Entry>> warmers = response.warmers();
        assertThat(warmers, notNullValue());
        assertThat(warmers.size(), equalTo(1));
        ImmutableList<Entry> indexWarmers = warmers.get(indexName);
        assertThat(indexWarmers, notNullValue());
        assertThat(indexWarmers.size(), equalTo(1));
        Entry warmer = indexWarmers.get(0);
        assertThat(warmer, notNullValue());
        assertThat(warmer.name(), equalTo("warmer1"));
    }

    private void assertSettings(GetIndexResponse response, String indexName) {
        ImmutableOpenMap<String, Settings> settings = response.settings();
        assertThat(settings, notNullValue());
        assertThat(settings.size(), equalTo(1));
        Settings indexSettings = settings.get(indexName);
        assertThat(indexSettings, notNullValue());
        assertThat(indexSettings.get("index.number_of_shards"), equalTo("1"));
    }

    private void assertNonEmptySettings(GetIndexResponse response, String indexName) {
        ImmutableOpenMap<String, Settings> settings = response.settings();
        assertThat(settings, notNullValue());
        assertThat(settings.size(), equalTo(1));
        Settings indexSettings = settings.get(indexName);
        assertThat(indexSettings, notNullValue());
    }

    private void assertMappings(GetIndexResponse response, String indexName) {
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = response.mappings();
        assertThat(mappings, notNullValue());
        assertThat(mappings.size(), equalTo(1));
        ImmutableOpenMap<String, MappingMetaData> indexMappings = mappings.get(indexName);
        assertThat(indexMappings, notNullValue());
        assertThat(indexMappings.size(), anyOf(equalTo(1), equalTo(2)));
        if (indexMappings.size() == 2) {
            MappingMetaData mapping = indexMappings.get("_default_");
            assertThat(mapping, notNullValue());
        }
        MappingMetaData mapping = indexMappings.get("type1");
        assertThat(mapping, notNullValue());
        assertThat(mapping.type(), equalTo("type1"));
    }

    private void assertEmptyOrOnlyDefaultMappings(GetIndexResponse response, String indexName) {
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = response.mappings();
        assertThat(mappings, notNullValue());
        assertThat(mappings.size(), equalTo(1));
        ImmutableOpenMap<String, MappingMetaData> indexMappings = mappings.get(indexName);
        assertThat(indexMappings, notNullValue());
        assertThat(indexMappings.size(), anyOf(equalTo(0), equalTo(1)));
        if (indexMappings.size() == 1) {
            MappingMetaData mapping = indexMappings.get("_default_");
            assertThat(mapping, notNullValue());
        }
    }

    private void assertAliases(GetIndexResponse response, String indexName) {
        ImmutableOpenMap<String, ImmutableList<AliasMetaData>> aliases = response.aliases();
        assertThat(aliases, notNullValue());
        assertThat(aliases.size(), equalTo(1));
        ImmutableList<AliasMetaData> indexAliases = aliases.get(indexName);
        assertThat(indexAliases, notNullValue());
        assertThat(indexAliases.size(), equalTo(1));
        AliasMetaData alias = indexAliases.get(0);
        assertThat(alias, notNullValue());
        assertThat(alias.alias(), equalTo("alias_idx"));
    }

    private void assertEmptyWarmers(GetIndexResponse response) {
        assertThat(response.warmers(), notNullValue());
        assertThat(response.warmers().isEmpty(), equalTo(true));
    }

    private void assertEmptySettings(GetIndexResponse response) {
        assertThat(response.settings(), notNullValue());
        assertThat(response.settings().isEmpty(), equalTo(true));
    }

    private void assertEmptyMappings(GetIndexResponse response) {
        assertThat(response.mappings(), notNullValue());
        assertThat(response.mappings().isEmpty(), equalTo(true));
    }

    private void assertEmptyAliases(GetIndexResponse response) {
        assertThat(response.aliases(), notNullValue());
        assertThat(response.aliases().isEmpty(), equalTo(true));
    }
}
