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
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticTextInput;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class ShardBulkInferenceActionFilterBasicLicenseIT extends ESIntegTestCase {
    public static final String INDEX_NAME = "test-index";

    private final boolean useLegacyFormat;

    public ShardBulkInferenceActionFilterBasicLicenseIT(boolean useLegacyFormat) {
        this.useLegacyFormat = useLegacyFormat;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @Before
    public void setup() throws Exception {
        ModelRegistry modelRegistry = internalCluster().getCurrentMasterNodeInstance(ModelRegistry.class);
        Utils.storeSparseModel(modelRegistry);
        Utils.storeDenseModel(
            modelRegistry,
            randomIntBetween(1, 100),
            // dot product means that we need normalized vectors; it's not worth doing that in this test
            randomValueOtherThan(SimilarityMeasure.DOT_PRODUCT, () -> randomFrom(SimilarityMeasure.values())),
            // TODO: Allow element type BIT once TestDenseInferenceServiceExtension supports it
            randomValueOtherThan(DenseVectorFieldMapper.ElementType.BIT, () -> randomFrom(DenseVectorFieldMapper.ElementType.values()))
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "basic").build();
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
        return builder.build();
    }

    public void testLicenseInvalidForInference() {
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

        BulkRequestBuilder bulkRequest = client().prepareBulk();
        int totalBulkReqs = randomIntBetween(2, 100);
        for (int i = 0; i < totalBulkReqs; i++) {
            Map<String, Object> source = new HashMap<>();
            source.put("sparse_field", randomSemanticTextInput());
            source.put("dense_field", randomSemanticTextInput());

            bulkRequest.add(new IndexRequestBuilder(client()).setIndex(INDEX_NAME).setId(Long.toString(i)).setSource(source));
        }

        BulkResponse bulkResponse = bulkRequest.get();
        for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
            assertTrue(bulkItemResponse.isFailed());
            assertThat(bulkItemResponse.getFailure().getCause(), instanceOf(ElasticsearchSecurityException.class));
            assertThat(
                bulkItemResponse.getFailure().getCause().getMessage(),
                containsString(Strings.format("current license is non-compliant for [%s]", XPackField.INFERENCE))
            );
        }
    }

    public void testNullSourceSucceeds() {
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

        BulkRequestBuilder bulkRequest = client().prepareBulk();
        int totalBulkReqs = randomIntBetween(2, 100);
        Map<String, Object> source = new HashMap<>();
        source.put("sparse_field", null);
        source.put("dense_field", null);
        for (int i = 0; i < totalBulkReqs; i++) {
            bulkRequest.add(new IndexRequestBuilder(client()).setIndex(INDEX_NAME).setId(Long.toString(i)).setSource(source));
        }

        BulkResponse bulkResponse = bulkRequest.get();
        assertFalse(bulkResponse.hasFailures());
    }
}
