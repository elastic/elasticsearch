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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_METADATA_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class GetIndexIT extends ESIntegTestCase {
    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").addAlias(new Alias("alias_idx")).addMapping("type1", "{\"type1\":{}}", XContentType.JSON)
                .setSettings(Settings.builder().put("number_of_shards", 1)).get());
        ensureSearchable("idx");
        createIndex("empty_idx");
        ensureSearchable("idx", "empty_idx");
    }

    public void testSimple() {
        GetIndexResponse response = client().admin().indices().prepareGetIndex().addIndices("idx").get();
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertAliases(response, "idx");
        assertMappings(response, "idx");
        assertSettings(response, "idx");
    }

    public void testSimpleUnknownIndex() {
        try {
            client().admin().indices().prepareGetIndex().addIndices("missing_idx").get();
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testEmpty() {
        GetIndexResponse response = client().admin().indices().prepareGetIndex().addIndices("empty_idx").get();
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("empty_idx"));
        assertEmptyAliases(response);
        assertEmptyOrOnlyDefaultMappings(response, "empty_idx");
        assertNonEmptySettings(response, "empty_idx");
    }

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
    }

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
    }

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
    }

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
    }

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
    }

    public void testGetIndexWithBlocks() {
        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY, SETTING_READ_ONLY_ALLOW_DELETE)) {
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
        ImmutableOpenMap<String, List<AliasMetaData>> aliases = response.aliases();
        assertThat(aliases, notNullValue());
        assertThat(aliases.size(), equalTo(1));
        List<AliasMetaData> indexAliases = aliases.get(indexName);
        assertThat(indexAliases, notNullValue());
        assertThat(indexAliases.size(), equalTo(1));
        AliasMetaData alias = indexAliases.get(0);
        assertThat(alias, notNullValue());
        assertThat(alias.alias(), equalTo("alias_idx"));
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
        for (final ObjectObjectCursor<String, List<AliasMetaData>> entry : response.getAliases()) {
            assertTrue(entry.value.isEmpty());
        }
    }
}
