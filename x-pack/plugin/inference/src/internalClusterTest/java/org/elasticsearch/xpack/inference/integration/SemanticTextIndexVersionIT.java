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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class SemanticTextIndexVersionIT extends ESIntegTestCase {
    private static final int MAXIMUM_NUMBER_OF_VERSIONS_TO_TEST = 25;
    private static final String SPARSE_SEMANTIC_FIELD = "sparse_field";
    private static final String DENSE_SEMANTIC_FIELD = "dense_field";
    private List<IndexVersion> selectedVersions;

    @Before
    public void setup() throws Exception {
        ModelRegistry modelRegistry = internalCluster().getCurrentMasterNodeInstance(ModelRegistry.class);
        DenseVectorFieldMapper.ElementType elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());
        // dot product means that we need normalized vectors; it's not worth doing that in this test
        SimilarityMeasure similarity = randomValueOtherThan(
            SimilarityMeasure.DOT_PRODUCT,
            () -> randomFrom(DenseVectorFieldMapperTestUtils.getSupportedSimilarities(elementType))
        );
        int dimensions = DenseVectorFieldMapperTestUtils.randomCompatibleDimensions(elementType, 100);
        Utils.storeSparseModel(modelRegistry);
        Utils.storeDenseModel(modelRegistry, dimensions, similarity, elementType);

        Set<IndexVersion> availableVersions = IndexVersionUtils.allReleasedVersions()
            .stream()
            .filter(indexVersion -> indexVersion.onOrAfter(IndexVersions.SEMANTIC_TEXT_FIELD_TYPE))
            .collect(Collectors.toSet());

        selectedVersions = randomSubsetOf(Math.min(availableVersions.size(), MAXIMUM_NUMBER_OF_VERSIONS_TO_TEST), availableVersions);
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
        return List.of(LocalStateInferencePlugin.class, FakeMlPlugin.class);
    }

    /**
     * Generate settings for an index with a specific version.
     */
    private Settings getIndexSettingsWithVersion(IndexVersion version) {
        return Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
    }

    /**
     * This test creates an index, ingests data, and performs searches (including highlighting when applicable)
     * for a selected subset of index versions.
     */
    public void testSemanticText() throws Exception {
        for (IndexVersion version : selectedVersions) {
            String indexName = "test_semantic_" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(SPARSE_SEMANTIC_FIELD)
                .field("type", "semantic_text")
                .field("inference_id", TestSparseInferenceServiceExtension.TestInferenceService.NAME)
                .endObject()
                .startObject(DENSE_SEMANTIC_FIELD)
                .field("type", "semantic_text")
                .field("inference_id", TestDenseInferenceServiceExtension.TestInferenceService.NAME)
                .endObject()
                .endObject()
                .endObject();

            assertAcked(prepareCreate(indexName).setSettings(getIndexSettingsWithVersion(version)).setMapping(mapping).get());

            // Test index creation with expected version id
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
                    .getAsVersionId(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion::fromId)
                    .id()
            );

            // Test data ingestion
            String[] text = new String[] { "inference test", "another inference test" };
            Map<String, String[]> sourceMap = new HashMap<>();
            sourceMap.put(SPARSE_SEMANTIC_FIELD, text);
            sourceMap.put(DENSE_SEMANTIC_FIELD, text);
            DocWriteResponse docWriteResponse = client().prepareIndex(indexName).setSource(sourceMap).get();

            assertEquals("Document should be created", "created", docWriteResponse.getResult().toString().toLowerCase(Locale.ROOT));

            // Ensure index is ready
            client().admin().indices().refresh(new RefreshRequest(indexName)).get();
            ensureGreen(indexName);

            // Semantic search with sparse embedding
            SearchSourceBuilder sparseSourceBuilder = new SearchSourceBuilder().query(
                new SemanticQueryBuilder(SPARSE_SEMANTIC_FIELD, "inference")
            ).trackTotalHits(true);

            assertResponse(
                client().search(new SearchRequest(indexName).source(sparseSourceBuilder)),
                response -> { assertHitCount(response, 1L); }
            );

            // Highlighting semantic search with sparse embedding
            SearchSourceBuilder sparseSourceHighlighterBuilder = new SearchSourceBuilder().query(
                new SemanticQueryBuilder(SPARSE_SEMANTIC_FIELD, "inference")
            ).highlighter(new HighlightBuilder().field(SPARSE_SEMANTIC_FIELD)).trackTotalHits(true);

            assertResponse(client().search(new SearchRequest(indexName).source(sparseSourceHighlighterBuilder)), response -> {
                assertHighlight(response, 0, SPARSE_SEMANTIC_FIELD, 0, 2, equalTo("inference test"));
                assertHighlight(response, 0, SPARSE_SEMANTIC_FIELD, 1, 2, equalTo("another inference test"));
            });

            // Semantic search with text embedding
            SearchSourceBuilder textSourceBuilder = new SearchSourceBuilder().query(
                new SemanticQueryBuilder(DENSE_SEMANTIC_FIELD, "inference")
            ).trackTotalHits(true);

            assertResponse(
                client().search(new SearchRequest(indexName).source(textSourceBuilder)),
                response -> { assertHitCount(response, 1L); }
            );

            // Highlighting semantic search with text embedding
            SearchSourceBuilder textSourceHighlighterBuilder = new SearchSourceBuilder().query(
                new SemanticQueryBuilder(DENSE_SEMANTIC_FIELD, "inference")
            ).highlighter(new HighlightBuilder().field(DENSE_SEMANTIC_FIELD)).trackTotalHits(true);

            assertResponse(client().search(new SearchRequest(indexName).source(textSourceHighlighterBuilder)), response -> {
                assertHighlight(response, 0, DENSE_SEMANTIC_FIELD, 0, 2, equalTo("inference test"));
                assertHighlight(response, 0, DENSE_SEMANTIC_FIELD, 1, 2, equalTo("another inference test"));
            });

            beforeIndexDeletion();
            assertAcked(client().admin().indices().prepareDelete(indexName));
        }
    }

    public static class FakeMlPlugin extends Plugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return new MlInferenceNamedXContentProvider().getNamedWriteables();
        }
    }
}
