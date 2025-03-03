/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticTextInput;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ShardBulkInferenceActionFilterIT extends ESIntegTestCase {
    public static final String INDEX_NAME = "test-index";

    private final boolean useLegacyFormat;
    private final boolean useSyntheticSource;

    public ShardBulkInferenceActionFilterIT(boolean useLegacyFormat, boolean useSyntheticSource) {
        this.useLegacyFormat = useLegacyFormat;
        this.useSyntheticSource = useSyntheticSource;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(
            new Object[] { true, false },
            new Object[] { true, true },
            new Object[] { false, false },
            new Object[] { false, true }
        );
    }

    @Before
    public void setup() throws Exception {
        Utils.storeSparseModel(client());
        Utils.storeDenseModel(
            client(),
            randomIntBetween(1, 100),
            // dot product means that we need normalized vectors; it's not worth doing that in this test
            randomValueOtherThan(SimilarityMeasure.DOT_PRODUCT, () -> randomFrom(SimilarityMeasure.values())),
            // TODO: Allow element type BIT once TestDenseInferenceServiceExtension supports it
            randomValueOtherThan(DenseVectorFieldMapper.ElementType.BIT, () -> randomFrom(DenseVectorFieldMapper.ElementType.values()))
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateInferencePlugin.class);
    }

    @Override
    public Settings indexSettings() {
        var builder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat);
        if (useSyntheticSource) {
            builder.put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true);
            builder.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.name());
        }
        return builder.build();
    }

    public void testBulkOperations() throws Exception {
        prepareCreate(INDEX_NAME).setMapping(
            String.format(
                Locale.ROOT,
                """
                    {
                        "properties": {
                            "sparse_field": {
                                "type": "semantic_text",
                                "inference_id": "%s"
                            },
                            "dense_field": {
                                "type": "semantic_text",
                                "inference_id": "%s"
                            }
                        }
                    }
                    """,
                TestSparseInferenceServiceExtension.TestInferenceService.NAME,
                TestDenseInferenceServiceExtension.TestInferenceService.NAME
            )
        ).get();

        int totalBulkReqs = randomIntBetween(2, 100);
        long totalDocs = 0;
        Set<String> ids = new HashSet<>();
        for (int bulkReqs = 0; bulkReqs < totalBulkReqs; bulkReqs++) {
            BulkRequestBuilder bulkReqBuilder = client().prepareBulk();
            int totalBulkSize = randomIntBetween(1, 100);
            for (int bulkSize = 0; bulkSize < totalBulkSize; bulkSize++) {
                if (ids.size() > 0 && rarely(random())) {
                    String id = randomFrom(ids);
                    ids.remove(id);
                    DeleteRequestBuilder request = new DeleteRequestBuilder(client(), INDEX_NAME).setId(id);
                    bulkReqBuilder.add(request);
                    continue;
                }
                String id = Long.toString(totalDocs++);
                boolean isIndexRequest = randomBoolean();
                Map<String, Object> source = new HashMap<>();
                source.put("sparse_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
                source.put("dense_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
                if (isIndexRequest) {
                    bulkReqBuilder.add(new IndexRequestBuilder(client()).setIndex(INDEX_NAME).setId(id).setSource(source));
                    ids.add(id);
                } else {
                    boolean isUpsert = randomBoolean();
                    UpdateRequestBuilder request = new UpdateRequestBuilder(client()).setIndex(INDEX_NAME).setDoc(source);
                    if (isUpsert || ids.size() == 0) {
                        request.setDocAsUpsert(true);
                    } else {
                        // Update already existing document
                        id = randomFrom(ids);
                    }
                    request.setId(id);
                    bulkReqBuilder.add(request);
                    ids.add(id);
                }
            }
            BulkResponse bulkResponse = bulkReqBuilder.get();
            if (bulkResponse.hasFailures()) {
                // Get more details in case something fails
                for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                    if (bulkItemResponse.isFailed()) {
                        fail(
                            bulkItemResponse.getFailure().getCause(),
                            "Failed to index document %s: %s",
                            bulkItemResponse.getId(),
                            bulkItemResponse.getFailureMessage()
                        );
                    }
                }
            }
            assertFalse(bulkResponse.hasFailures());
        }

        client().admin().indices().refresh(new RefreshRequest(INDEX_NAME)).get();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true);
        SearchResponse searchResponse = client().search(new SearchRequest(INDEX_NAME).source(sourceBuilder)).get();
        try {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) ids.size()));
        } finally {
            searchResponse.decRef();
        }
    }

    public void testItemFailures() {
        prepareCreate(INDEX_NAME).setMapping(
            String.format(
                Locale.ROOT,
                """
                    {
                        "properties": {
                            "sparse_field": {
                                "type": "semantic_text",
                                "inference_id": "%s"
                            },
                            "dense_field": {
                                "type": "semantic_text",
                                "inference_id": "%s"
                            }
                        }
                    }
                    """,
                TestSparseInferenceServiceExtension.TestInferenceService.NAME,
                TestDenseInferenceServiceExtension.TestInferenceService.NAME
            )
        ).get();

        BulkRequestBuilder bulkReqBuilder = client().prepareBulk();
        int totalBulkSize = randomIntBetween(100, 200);  // Use a bulk request size large enough to require batching
        for (int bulkSize = 0; bulkSize < totalBulkSize; bulkSize++) {
            String id = Integer.toString(bulkSize);

            // Set field values that will cause errors when generating inference requests
            Map<String, Object> source = new HashMap<>();
            source.put("sparse_field", List.of(Map.of("foo", "bar"), Map.of("baz", "bar")));
            source.put("dense_field", List.of(Map.of("foo", "bar"), Map.of("baz", "bar")));

            bulkReqBuilder.add(new IndexRequestBuilder(client()).setIndex(INDEX_NAME).setId(id).setSource(source));
        }

        BulkResponse bulkResponse = bulkReqBuilder.get();
        assertThat(bulkResponse.hasFailures(), equalTo(true));
        for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
            assertThat(bulkItemResponse.isFailed(), equalTo(true));
            assertThat(bulkItemResponse.getFailureMessage(), containsString("expected [String|Number|Boolean]"));
        }
    }
}
