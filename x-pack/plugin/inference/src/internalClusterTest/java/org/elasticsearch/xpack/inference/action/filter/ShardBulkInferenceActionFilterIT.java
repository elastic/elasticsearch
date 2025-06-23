/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.Utils.storeModel;
import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.INDICES_INFERENCE_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticTextInput;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ShardBulkInferenceActionFilterIT extends ESIntegTestCase {
    public static final String INDEX_NAME = "test-index";

    private final boolean useLegacyFormat;
    private final boolean useSyntheticSource;
    private ModelRegistry modelRegistry;

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
        modelRegistry = internalCluster().getCurrentMasterNodeInstance(ModelRegistry.class);
        DenseVectorFieldMapper.ElementType elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());
        // dot product means that we need normalized vectors; it's not worth doing that in this test
        SimilarityMeasure similarity = randomValueOtherThan(
            SimilarityMeasure.DOT_PRODUCT,
            () -> randomFrom(DenseVectorFieldMapperTestUtils.getSupportedSimilarities(elementType))
        );
        int dimensions = DenseVectorFieldMapperTestUtils.randomCompatibleDimensions(elementType, 100);
        Utils.storeSparseModel(modelRegistry);
        Utils.storeDenseModel(modelRegistry, dimensions, similarity, elementType);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        long batchSizeInBytes = randomLongBetween(1, ByteSizeValue.ofKb(1).getBytes());
        return Settings.builder()
            .put(otherSettings)
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .put(INDICES_INFERENCE_BATCH_SIZE.getKey(), ByteSizeValue.ofBytes(batchSizeInBytes))
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class);
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
        assertRandomBulkOperations(INDEX_NAME, isIndexRequest -> {
            Map<String, Object> map = new HashMap<>();
            map.put("sparse_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
            map.put("dense_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
            return map;
        });
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

    public void testRestart() throws Exception {
        Model model1 = new TestSparseInferenceServiceExtension.TestSparseModel(
            "another_inference_endpoint",
            new TestSparseInferenceServiceExtension.TestServiceSettings("sparse_model", null, false)
        );
        storeModel(modelRegistry, model1);
        prepareCreate("index_restart").setMapping("""
            {
                "properties": {
                    "sparse_field": {
                        "type": "semantic_text",
                        "inference_id": "new_inference_endpoint"
                    },
                    "other_field": {
                        "type": "semantic_text",
                        "inference_id": "another_inference_endpoint"
                    }
                }
            }
            """).get();
        Model model2 = new TestSparseInferenceServiceExtension.TestSparseModel(
            "new_inference_endpoint",
            new TestSparseInferenceServiceExtension.TestServiceSettings("sparse_model", null, false)
        );
        storeModel(modelRegistry, model2);

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback());
        ensureGreen(InferenceIndex.INDEX_NAME, "index_restart");

        assertRandomBulkOperations("index_restart", isIndexRequest -> {
            Map<String, Object> map = new HashMap<>();
            map.put("sparse_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
            map.put("other_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
            return map;
        });

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback());
        ensureGreen(InferenceIndex.INDEX_NAME, "index_restart");

        assertRandomBulkOperations("index_restart", isIndexRequest -> {
            Map<String, Object> map = new HashMap<>();
            map.put("sparse_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
            map.put("other_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
            return map;
        });
    }

    private void assertRandomBulkOperations(String indexName, Function<Boolean, Map<String, Object>> sourceSupplier) throws Exception {
        int numHits = numHits(indexName);
        int totalBulkReqs = randomIntBetween(2, 10);
        Set<String> ids = new HashSet<>();
        for (int bulkReqs = 0; bulkReqs < totalBulkReqs; bulkReqs++) {
            BulkRequestBuilder bulkReqBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            int totalBulkSize = randomIntBetween(1, 100);
            for (int bulkSize = 0; bulkSize < totalBulkSize; bulkSize++) {
                if (ids.size() > 0 && rarely(random())) {
                    String id = randomFrom(ids);
                    ids.remove(id);
                    DeleteRequestBuilder request = new DeleteRequestBuilder(client(), indexName).setId(id);
                    bulkReqBuilder.add(request);
                    continue;
                }
                boolean isIndexRequest = ids.size() == 0 || randomBoolean();
                Map<String, Object> source = sourceSupplier.apply(isIndexRequest);
                if (isIndexRequest) {
                    String id = randomAlphaOfLength(20);
                    bulkReqBuilder.add(new IndexRequestBuilder(client()).setIndex(indexName).setId(id).setSource(source));
                    ids.add(id);
                } else {
                    String id = randomFrom(ids);
                    bulkReqBuilder.add(new UpdateRequestBuilder(client()).setIndex(indexName).setId(id).setDoc(source));
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
        assertThat(numHits(indexName), equalTo(numHits + ids.size()));
    }

    private int numHits(String indexName) throws Exception {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true);
        SearchResponse searchResponse = client().search(new SearchRequest(indexName).source(sourceBuilder)).get();
        try {
            return (int) searchResponse.getHits().getTotalHits().value();
        } finally {
            searchResponse.decRef();
        }
    }
}
