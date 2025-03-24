/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class SemanticTextIndexVersionIT extends ESIntegTestCase {
    private static final IndexVersion SEMANTIC_TEXT_INTRODUCED_VERSION = IndexVersion.fromId(8512000);

    private Set<IndexVersion> availableVersions;
    private static final int MIN_NUMBER_OF_TESTS_TO_RUN = 10;

    @Before
    public void setup() throws Exception {
        Utils.storeSparseModel(client());
        availableVersions = IndexVersionUtils.allReleasedVersions().stream()
            .filter(indexVersion -> indexVersion.after(SEMANTIC_TEXT_INTRODUCED_VERSION))
            .collect(Collectors.toSet());

        logger.info("Available versions for testing: {}", availableVersions);
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(otherSettings).put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class);
    }

    /**
     * Generate settings for an index with a specific version.
     */
    private Settings getIndexSettingsWithVersion(IndexVersion version) {
        return Settings.builder().put(indexSettings()).put("index.version.created", version).build();
    }

    /**
     * Creates a subset of indices with different versions for testing.
     *
     * @return Map of created indices with their versions
     */
    protected Map<String, IndexVersion> createRandomVersionIndices() throws IOException {
        int versionsCount = Math.min(MIN_NUMBER_OF_TESTS_TO_RUN, availableVersions.size());
        List<IndexVersion> selectedVersions = randomSubsetOf(versionsCount, availableVersions);
        Map<String, IndexVersion> result = new HashMap<>();

        for (int i = 0; i < selectedVersions.size(); i++) {
            String indexName = "test_semantic" + "_" + i;
            IndexVersion version = selectedVersions.get(i);
            createIndex(indexName, getIndexSettingsWithVersion(version));
            result.put(indexName, version);
        }

        return result;
    }

    public void testSemanticText() throws Exception {
        Map<String, IndexVersion> indices = createRandomVersionIndices();
        for (String indexName : indices.keySet()) {
            IndexVersion version = indices.get(indexName);
            logger.info("Testing index [{}] with version [{}] [{}]", indexName, version, version.toReleaseVersion());

            // Test index creation
            assertTrue("Index " + indexName + " should exist", indexExists(indexName));
            assertEquals(
                "Index version should match",
                version.id(),
                client().admin()
                    .indices()
                    .prepareGetSettings(TimeValue.THIRTY_SECONDS, indexName)
                    .get()
                    .getIndexToSettings()
                    .get(indexName)
                    .getAsVersionId("index.version.created", IndexVersion::fromId)
                    .id()
            );

            // Test update mapping
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("semantic_field")
                .field("type", "semantic_text")
                .field("inference_id", TestSparseInferenceServiceExtension.TestInferenceService.NAME)
                .endObject()
                .endObject()
                .endObject();

            assertAcked(client().admin().indices().preparePutMapping(indexName).setSource(mapping).get());

            // Test data ingestion
            String[] text = new String[] { "inference test", "another inference test" };
            DocWriteResponse docWriteResponse = client().prepareIndex(indexName)
                .setSource(Map.of("semantic_field", text))
                .get();

            assertEquals("Document should be created", "created", docWriteResponse.getResult().toString().toLowerCase());

            // Ensure index is ready
            client().admin().indices().refresh(new RefreshRequest(indexName)).get();
            ensureGreen(indexName);

            // Semantic Search
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(new SemanticQueryBuilder("semantic_field", "inference"))
                .trackTotalHits(true);

            assertResponse(client().search(new SearchRequest(indexName).source(sourceBuilder)), response -> {
                assertHitCount(response, 1L);
            });

            //Semantic Search with highlighter only available from 8.18 and 9.0
            Settings settings = client().admin()
                .indices()
                .prepareGetSettings(TimeValue.THIRTY_SECONDS, indexName)
                .get()
                .getIndexToSettings()
                .get(indexName);

            if (InferenceMetadataFieldsMapper.isEnabled(settings)) {
                SearchSourceBuilder sourceHighlighterBuilder = new SearchSourceBuilder()
                    .query(new SemanticQueryBuilder("semantic_field", "inference"))
                    .highlighter(new HighlightBuilder().field("semantic_field"))
                    .trackTotalHits(true);

                assertResponse(client().search(new SearchRequest(indexName).source(sourceHighlighterBuilder)), response -> {
                    assertHighlight(response, 0, "semantic_field", 0, 2, equalTo("inference test"));
                    assertHighlight(response, 0, "semantic_field", 1, 2, equalTo("another inference test"));
                });
            }
        }
    }
}
