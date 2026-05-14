/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import com.carrotsearch.randomizedtesting.annotations.Name;
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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.InferenceSecretsIndex;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapperTests;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.Utils.storeModel;
import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.INDICES_INFERENCE_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomInferenceString;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticInput;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticTextInput;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public class ShardBulkInferenceActionFilterIT extends ESIntegTestCase {
    public static final String INDEX_NAME = "test-index";

    private static final String SPARSE_INFERENCE_ID = "sparse-endpoint";
    private static final String DENSE_INFERENCE_ID = "dense-endpoint";
    private static final String EMBEDDING_INFERENCE_ID = "embedding-endpoint";

    private final boolean useLegacyFormat;
    private final boolean useSyntheticSource;
    private ModelRegistry modelRegistry;

    public ShardBulkInferenceActionFilterIT(
        @Name("useLegacyFormat") boolean useLegacyFormat,
        @Name("useSyntheticSource") boolean useSyntheticSource
    ) {
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
        DenseVectorFieldMapper.ElementType elementType = randomValueOtherThan(
            DenseVectorFieldMapper.ElementType.BFLOAT16,
            () -> randomFrom(DenseVectorFieldMapper.ElementType.values())
        );
        // dot product means that we need normalized vectors; it's not worth doing that in this test
        SimilarityMeasure similarity = randomValueOtherThan(
            SimilarityMeasure.DOT_PRODUCT,
            () -> randomFrom(DenseVectorFieldMapperTestUtils.getSupportedSimilarities(elementType))
        );
        int dimensions = DenseVectorFieldMapperTestUtils.randomCompatibleDimensions(elementType, 100);
        Utils.storeSparseModel(SPARSE_INFERENCE_ID, modelRegistry);
        Utils.storeDenseModel(DENSE_INFERENCE_ID, modelRegistry, dimensions, similarity, elementType);
        Utils.storeEmbeddingModel(EMBEDDING_INFERENCE_ID, modelRegistry, dimensions, similarity, elementType);
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        // For setting index version
        return false;
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
        if (useLegacyFormat) {
            builder.put(IndexMetadata.SETTING_VERSION_CREATED, getRandomCompatiblePreSemanticFieldIndexVersion());
        }
        if (useSyntheticSource) {
            builder.put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true);
            builder.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.name());
        }
        return builder.build();
    }

    public void testSemanticTextBulkOperations() throws Exception {
        prepareCreate(INDEX_NAME).setMapping(String.format(Locale.ROOT, """
            {
                "properties": {
                    "sparse_field": {
                        "type": "semantic_text",
                        "inference_id": "%s"
                    },
                    "dense_field": {
                        "type": "semantic_text",
                        "inference_id": "%s"
                    },
                    "embedding_field": {
                        "type": "semantic_text",
                        "inference_id": "%s"
                    }
                }
            }
            """, SPARSE_INFERENCE_ID, DENSE_INFERENCE_ID, EMBEDDING_INFERENCE_ID)).get();
        assertRandomBulkOperations(INDEX_NAME, isIndexRequest -> {
            Map<String, Object> map = new HashMap<>();
            map.put("sparse_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
            map.put("dense_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
            map.put("embedding_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
            return map;
        });
    }

    public void testSemanticTextItemFailures() {
        prepareCreate(INDEX_NAME).setMapping(String.format(Locale.ROOT, """
            {
                "properties": {
                    "sparse_field": {
                        "type": "semantic_text",
                        "inference_id": "%s"
                    },
                    "dense_field": {
                        "type": "semantic_text",
                        "inference_id": "%s"
                    },
                    "embedding_field": {
                        "type": "semantic_text",
                        "inference_id": "%s"
                    }
                }
            }
            """, SPARSE_INFERENCE_ID, DENSE_INFERENCE_ID, EMBEDDING_INFERENCE_ID)).get();

        // Set a single inference string value on fields that use sparse_embedding & text_embedding inference services
        assertItemFailures(INDEX_NAME, () -> Map.of("sparse_field", randomInferenceString(), "dense_field", randomInferenceString()), r -> {
            // In the legacy format, the value is parsed as a SemanticTextField, which requires an "inference" block.
            // In the new format, ShardBulkInferenceAction filter attempts to parse the object and fails.
            String expectedMessage = useLegacyFormat ? "Required [inference]" : "expected [String|Number|Boolean]";
            assertThat(rootCause(r.getFailure().getCause()).getMessage(), containsString(expectedMessage));
        });

        // Set multiple inference string values on fields that use sparse_embedding & text_embedding inference services.
        // In both cases (legacy and new format), ShardBulkInferenceActionFilter attempts to parse the list of objects and fails.
        assertItemFailures(
            INDEX_NAME,
            () -> Map.of(
                "sparse_field",
                List.of(randomInferenceString(), randomInferenceString()),
                "dense_field",
                List.of(randomInferenceString(), randomInferenceString())
            ),
            r -> assertThat(rootCause(r.getFailure().getCause()).getMessage(), containsString("expected [String|Number|Boolean]"))
        );

        // Set a list of lists value on fields that use sparse_embedding & text_embedding inference services.
        // In both cases (legacy and new format), ShardBulkInferenceActionFilter attempts to parse the list of lists and fails.
        assertItemFailures(
            INDEX_NAME,
            () -> Map.of("sparse_field", List.of(List.of("foo", "bar")), "dense_field", List.of(List.of("foo", "bar"))),
            r -> assertThat(rootCause(r.getFailure().getCause()).getMessage(), containsString("expected [String|Number|Boolean]"))
        );

        // Set an inference string value on a field that uses an embedding inference service
        assertItemFailures(INDEX_NAME, () -> Map.of("embedding_field", randomInferenceString()), r -> {
            // In the legacy format, the value is parsed as a SemanticTextField, which requires an "inference" block.
            // In the new format, it is rejected by SemanticTextFieldMapper.
            String expectedMessage = useLegacyFormat
                ? "Required [inference]"
                : "[semantic_text] field [embedding_field] does not support object values";
            assertThat(rootCause(r.getFailure().getCause()).getMessage(), containsString(expectedMessage));
        });

        // Set multiple inference string values on a field that uses an embedding inference service
        assertItemFailures(INDEX_NAME, () -> Map.of("embedding_field", List.of(randomInferenceString(), randomInferenceString())), r -> {
            // In the legacy format, ShardBulkInferenceActionFilter attempts to parse the list of objects and fails.
            // In the new format, the value is rejected by SemanticTextFieldMapper.
            String expectedMessage = useLegacyFormat
                ? "expected [String|Number|Boolean]"
                : "[semantic_text] field [embedding_field] does not support object values";
            assertThat(rootCause(r.getFailure().getCause()).getMessage(), containsString(expectedMessage));
        });

        // Set a list of lists value on a field that uses an embedding inference service.
        // In both cases (legacy and new format), ShardBulkInferenceActionFilter attempts to parse the list of lists and fails.
        assertItemFailures(INDEX_NAME, () -> Map.of("embedding_field", List.of(List.of("foo", "bar"))), r -> {
            String expectedMessage = useLegacyFormat
                ? "expected [String|Number|Boolean]"
                : "expected [String|Number|Boolean|InferenceString]";
            assertThat(rootCause(r.getFailure().getCause()).getMessage(), containsString(expectedMessage));
        });
    }

    /**
     * Semantic text fields in indices created prior to the introduction of the semantic field should not be able to parse objects
     * (other than the legacy format object)
     */
    public void testSemanticTextItemFailuresWithPreSemanticFieldIndexVersion() {
        // Set an index version that does not support the semantic field type
        Settings settings = Settings.builder()
            .put(indexSettings())
            .put(IndexMetadata.SETTING_VERSION_CREATED, getRandomCompatiblePreSemanticFieldIndexVersion())
            .build();

        prepareCreate(INDEX_NAME).setSettings(settings).setMapping(String.format(Locale.ROOT, """
            {
                "properties": {
                    "sparse_field": {
                        "type": "semantic_text",
                        "inference_id": "%s"
                    },
                    "dense_field": {
                        "type": "semantic_text",
                        "inference_id": "%s"
                    },
                    "embedding_field": {
                        "type": "semantic_text",
                        "inference_id": "%s"
                    }
                }
            }
            """, SPARSE_INFERENCE_ID, DENSE_INFERENCE_ID, EMBEDDING_INFERENCE_ID)).get();

        // Set a single inference string value on fields that use sparse_embedding & text_embedding inference services
        assertItemFailures(INDEX_NAME, () -> Map.of("sparse_field", randomInferenceString(), "dense_field", randomInferenceString()), r -> {
            // In the legacy format, the value is parsed as a SemanticTextField, which requires an "inference" block.
            // In the new format, ShardBulkInferenceAction filter attempts to parse the object and fails.
            String expectedMessage = useLegacyFormat ? "Required [inference]" : "expected [String|Number|Boolean]";
            assertThat(rootCause(r.getFailure().getCause()).getMessage(), containsString(expectedMessage));
        });

        // Set multiple inference string values on fields that use sparse_embedding & text_embedding inference services.
        // In both cases (legacy and new format), ShardBulkInferenceActionFilter attempts to parse the list of objects and fails.
        assertItemFailures(
            INDEX_NAME,
            () -> Map.of(
                "sparse_field",
                List.of(randomInferenceString(), randomInferenceString()),
                "dense_field",
                List.of(randomInferenceString(), randomInferenceString())
            ),
            r -> assertThat(rootCause(r.getFailure().getCause()).getMessage(), containsString("expected [String|Number|Boolean]"))
        );

        // Set an inference string value on a field that uses an embedding inference service
        assertItemFailures(INDEX_NAME, () -> Map.of("embedding_field", randomInferenceString()), r -> {
            // In the legacy format, the value is parsed as a SemanticTextField, which requires an "inference" block.
            // In the new format, ShardBulkInferenceAction filter attempts to parse the object and fails.
            String expectedMessage = useLegacyFormat ? "Required [inference]" : "expected [String|Number|Boolean]";
            assertThat(rootCause(r.getFailure().getCause()).getMessage(), containsString(expectedMessage));
        });

        // Set multiple inference string values on a field that uses an embedding inference service.
        // In both cases (legacy and new format), ShardBulkInferenceActionFilter attempts to parse the list of objects and fails.
        assertItemFailures(
            INDEX_NAME,
            () -> Map.of("embedding_field", List.of(randomInferenceString(), randomInferenceString())),
            r -> assertThat(rootCause(r.getFailure().getCause()).getMessage(), containsString("expected [String|Number|Boolean]"))
        );
    }

    public void testSemanticBulkOperations() throws Exception {
        assumeTrue("Semantic field feature flag is enabled", SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled());
        assumeFalse("Legacy format does not apply to the semantic field", useLegacyFormat);

        prepareCreate(INDEX_NAME).setMapping(String.format(Locale.ROOT, """
            {
                "properties": {
                    "semantic_field": {
                        "type": "semantic",
                        "inference_id": "%s"
                    }
                }
            }
            """, EMBEDDING_INFERENCE_ID)).get();
        assertRandomBulkOperations(INDEX_NAME, isIndexRequest -> {
            Map<String, Object> map = new HashMap<>();
            map.put("semantic_field", isIndexRequest && rarely() ? null : randomSemanticInput(true));
            return map;
        });
    }

    public void testSemanticItemFailures() {
        assumeTrue("Semantic field feature flag is enabled", SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled());
        assumeFalse("Legacy format does not apply to the semantic field", useLegacyFormat);

        prepareCreate(INDEX_NAME).setMapping(String.format(Locale.ROOT, """
            {
                "properties": {
                    "semantic_field": {
                        "type": "semantic",
                        "inference_id": "%s"
                    }
                }
            }
            """, EMBEDDING_INFERENCE_ID)).get();

        // Malformed inference string
        assertItemFailures(
            INDEX_NAME,
            () -> Map.of("semantic_field", Map.of("type", "image", "format", "text", "value", "x")),
            r -> assertThat(
                rootCause(r.getFailure().getCause()).getMessage(),
                containsString("Data type [image] does not support data format [text], supported formats are [base64]")
            )
        );

        // Text expressed as an inference string object
        assertItemFailures(
            INDEX_NAME,
            () -> Map.of("semantic_field", new InferenceString(DataType.TEXT, "foo")),
            r -> assertThat(
                rootCause(r.getFailure().getCause()).getMessage(),
                containsString("Objects for text values are not supported, use a string literal instead")
            )
        );

        // List of lists of values
        assertItemFailures(
            INDEX_NAME,
            () -> Map.of("semantic_field", List.of(List.of("foo", "bar"))),
            r -> assertThat(
                rootCause(r.getFailure().getCause()).getMessage(),
                containsString("expected [String|Number|Boolean|InferenceString]")
            )
        );
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
        ensureGreen(InferenceIndex.INDEX_NAME, "index_restart", InferenceSecretsIndex.INDEX_NAME);

        assertRandomBulkOperations("index_restart", isIndexRequest -> {
            Map<String, Object> map = new HashMap<>();
            map.put("sparse_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
            map.put("other_field", isIndexRequest && rarely() ? null : randomSemanticTextInput());
            return map;
        });

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback());
        ensureGreen(InferenceIndex.INDEX_NAME, "index_restart", InferenceSecretsIndex.INDEX_NAME);

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

    private void assertItemFailures(
        String indexName,
        Supplier<Map<String, Object>> sourceSupplier,
        Consumer<BulkItemResponse> responseValidator
    ) {
        BulkRequestBuilder bulkReqBuilder = client().prepareBulk();
        int totalBulkSize = randomIntBetween(100, 200);  // Use a bulk request size large enough to require batching
        for (int bulkIdx = 0; bulkIdx < totalBulkSize; bulkIdx++) {
            String id = Integer.toString(bulkIdx);
            Map<String, Object> source = sourceSupplier.get();
            bulkReqBuilder.add(new IndexRequestBuilder(client()).setIndex(indexName).setId(id).setSource(source));
        }

        BulkResponse bulkResponse = bulkReqBuilder.get(TEST_REQUEST_TIMEOUT);
        assertThat(bulkResponse.hasFailures(), equalTo(true));
        for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
            assertThat(bulkItemResponse.isFailed(), equalTo(true));
            responseValidator.accept(bulkItemResponse);
        }
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

    private IndexVersion getRandomCompatiblePreSemanticFieldIndexVersion() {
        if (useLegacyFormat) {
            if (useSyntheticSource) {
                // Must be in a range that supports both legacy format and synthetic source recovery.
                if (randomBoolean()) {
                    // 9.x
                    return IndexVersionUtils.randomVersionBetween(
                        IndexVersions.USE_SYNTHETIC_SOURCE_FOR_RECOVERY,
                        IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_TEXT_LEGACY_FORMAT_FORBIDDEN)
                    );
                }

                // 8.x
                return IndexVersionUtils.randomVersionBetween(
                    IndexVersions.USE_SYNTHETIC_SOURCE_FOR_RECOVERY_BACKPORT,
                    IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
                );
            }

            return SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(true);
        }

        // New format, strictly before SEMANTIC_FIELD_TYPE. The new-format lower bound
        // (INFERENCE_METADATA_FIELDS{,_BACKPORT}) is already above the synthetic-source recovery lower bound
        // in both major branches, so useSyntheticSource does not further constrain the window.
        if (randomBoolean()) {
            // 9.x
            return IndexVersionUtils.randomVersionBetween(
                IndexVersions.INFERENCE_METADATA_FIELDS,
                IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_FIELD_TYPE)
            );
        }

        // 8.x
        return IndexVersionUtils.randomVersionBetween(
            IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT,
            IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
        );
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
