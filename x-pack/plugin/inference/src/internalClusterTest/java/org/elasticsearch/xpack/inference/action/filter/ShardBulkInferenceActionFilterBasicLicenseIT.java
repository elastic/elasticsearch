/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticTextInput;
import static org.hamcrest.Matchers.instanceOf;

public class ShardBulkInferenceActionFilterBasicLicenseIT extends ESIntegTestCase {
    public static final String INDEX_NAME = "test-index";

    private final boolean useLegacyFormat;
    private final boolean useSyntheticSource;

    public ShardBulkInferenceActionFilterBasicLicenseIT(boolean useLegacyFormat, boolean useSyntheticSource) {
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
        if (useSyntheticSource) {
            builder.put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true);
            builder.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.name());
        }
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
            source.put("sparse_field", rarely() ? null : randomSemanticTextInput());
            source.put("dense_field", rarely() ? null : randomSemanticTextInput());

            bulkRequest.add(new IndexRequestBuilder(client()).setIndex(INDEX_NAME).setId(Long.toString(i)).setSource(source));
        }

        BulkResponse bulkResponse = bulkRequest.get();
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertTrue(itemResponse.isFailed());
            assertThat(itemResponse.getFailure().getCause(), instanceOf(ElasticsearchSecurityException.class));
        }
    }
}
