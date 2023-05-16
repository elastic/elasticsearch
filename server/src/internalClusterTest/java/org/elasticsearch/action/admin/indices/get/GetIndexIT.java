/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.get;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_METADATA_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class GetIndexIT extends ESIntegTestCase {
    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").addAlias(new Alias("alias_idx")).setSettings(Settings.builder().put("number_of_shards", 1)).get());
        ensureSearchable("idx");
        createIndex("empty_idx");
        ensureSearchable("idx", "empty_idx");
    }

    public void testSimple() {
        GetIndexResponse response = indicesAdmin().prepareGetIndex().addIndices("idx").get();
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
            indicesAdmin().prepareGetIndex().addIndices("missing_idx").get();
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index [missing_idx]"));
        }
    }

    public void testUnknownIndexWithAllowNoIndices() {
        GetIndexResponse response = indicesAdmin().prepareGetIndex()
            .addIndices("missing_idx")
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .get();
        assertThat(response.indices(), notNullValue());
        assertThat(response.indices().length, equalTo(0));
        assertThat(response.mappings(), notNullValue());
        assertThat(response.mappings().size(), equalTo(0));
    }

    public void testEmpty() {
        GetIndexResponse response = indicesAdmin().prepareGetIndex().addIndices("empty_idx").get();
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("empty_idx"));
        assertEmptyAliases(response);
        assertEmptyOrOnlyDefaultMappings(response, "empty_idx");
        assertNonEmptySettings(response, "empty_idx");
    }

    public void testSimpleMapping() {
        GetIndexResponse response = runWithRandomFeatureMethod(indicesAdmin().prepareGetIndex().addIndices("idx"), Feature.MAPPINGS);
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertMappings(response, "idx");
        assertEmptyAliases(response);
        assertEmptySettings(response);
    }

    public void testSimpleAlias() {
        GetIndexResponse response = runWithRandomFeatureMethod(indicesAdmin().prepareGetIndex().addIndices("idx"), Feature.ALIASES);
        String[] indices = response.indices();
        assertThat(indices, notNullValue());
        assertThat(indices.length, equalTo(1));
        assertThat(indices[0], equalTo("idx"));
        assertAliases(response, "idx");
        assertEmptyMappings(response);
        assertEmptySettings(response);
    }

    public void testSimpleSettings() {
        GetIndexResponse response = runWithRandomFeatureMethod(indicesAdmin().prepareGetIndex().addIndices("idx"), Feature.SETTINGS);
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
        List<Feature> features = new ArrayList<>(numFeatures);
        for (int i = 0; i < numFeatures; i++) {
            features.add(randomFrom(Feature.values()));
        }
        GetIndexResponse response = runWithRandomFeatureMethod(
            indicesAdmin().prepareGetIndex().addIndices("idx"),
            features.toArray(new Feature[features.size()])
        );
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
        List<Feature> features = new ArrayList<>(numFeatures);
        for (int i = 0; i < numFeatures; i++) {
            features.add(randomFrom(Feature.values()));
        }
        GetIndexResponse response = runWithRandomFeatureMethod(
            indicesAdmin().prepareGetIndex().addIndices("empty_idx"),
            features.toArray(new Feature[features.size()])
        );
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
                GetIndexResponse response = indicesAdmin().prepareGetIndex()
                    .addIndices("idx")
                    .addFeatures(Feature.MAPPINGS, Feature.ALIASES)
                    .get();
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
            assertBlocked(
                indicesAdmin().prepareGetIndex().addIndices("idx").addFeatures(Feature.MAPPINGS, Feature.ALIASES),
                INDEX_METADATA_BLOCK
            );
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
        Map<String, Settings> settings = response.settings();
        assertThat(settings, notNullValue());
        assertThat(settings.size(), equalTo(1));
        Settings indexSettings = settings.get(indexName);
        assertThat(indexSettings, notNullValue());
        assertThat(indexSettings.get("index.number_of_shards"), equalTo("1"));
    }

    private void assertNonEmptySettings(GetIndexResponse response, String indexName) {
        Map<String, Settings> settings = response.settings();
        assertThat(settings, notNullValue());
        assertThat(settings.size(), equalTo(1));
        Settings indexSettings = settings.get(indexName);
        assertThat(indexSettings, notNullValue());
    }

    private void assertMappings(GetIndexResponse response, String indexName) {
        Map<String, MappingMetadata> mappings = response.mappings();
        assertThat(mappings, notNullValue());
        assertThat(mappings.size(), equalTo(1));
        MappingMetadata indexMappings = mappings.get(indexName);
        assertThat(indexMappings, notNullValue());
    }

    private void assertEmptyOrOnlyDefaultMappings(GetIndexResponse response, String indexName) {
        Map<String, MappingMetadata> mappings = response.mappings();
        assertThat(mappings, notNullValue());
        assertThat(mappings.size(), equalTo(1));
        MappingMetadata indexMappings = mappings.get(indexName);
        assertEquals(indexMappings, MappingMetadata.EMPTY_MAPPINGS);
    }

    private void assertAliases(GetIndexResponse response, String indexName) {
        Map<String, List<AliasMetadata>> aliases = response.aliases();
        assertThat(aliases, notNullValue());
        assertThat(aliases.size(), equalTo(1));
        List<AliasMetadata> indexAliases = aliases.get(indexName);
        assertThat(indexAliases, notNullValue());
        assertThat(indexAliases.size(), equalTo(1));
        AliasMetadata alias = indexAliases.get(0);
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
        for (final Map.Entry<String, List<AliasMetadata>> entry : response.getAliases().entrySet()) {
            assertTrue(entry.getValue().isEmpty());
        }
    }
}
