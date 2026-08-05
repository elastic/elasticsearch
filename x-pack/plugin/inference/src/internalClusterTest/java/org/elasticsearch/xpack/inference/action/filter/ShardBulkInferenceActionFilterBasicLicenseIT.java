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
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapperTests;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilterIT.registerModel;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticInput;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticTextInput;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public class ShardBulkInferenceActionFilterBasicLicenseIT extends ESIntegTestCase {
    public static final String INDEX_NAME = "test-index";
    private static final String SPARSE_INFERENCE_ID = "sparse-endpoint";
    private static final String DENSE_INFERENCE_ID = "dense-endpoint";
    private static final String EMBEDDING_INFERENCE_ID = "embedding-endpoint";

    private final SemanticFieldType fieldTypeTestParameter;
    private final boolean useLegacyFormat;

    private enum SemanticFieldType {
        SEMANTIC_TEXT {
            @Override
            String getMapping() {
                return Strings.format("""
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
            """, SPARSE_INFERENCE_ID, DENSE_INFERENCE_ID);
            }

            @Override
            Map<String, Object> sampleSource() {
                return Map.of(
                    "sparse_field", randomSemanticTextInput(),
                    "dense_field", randomSemanticTextInput()
                );
            }

            @Override
            Map<String, Object> nullSource() {
                final Map<String, Object> source = new HashMap<>();
                source.put("sparse_field", null);
                source.put("dense_field", null);
                return source;
            }
        },
        SEMANTIC {
            @Override
            String getMapping() {
                return Strings.format("""
            {
                "properties": {
                    "semantic_field": {
                        "type": "semantic",
                        "inference_id": "%s"
                    }
                }
            }
            """, EMBEDDING_INFERENCE_ID);
            }

            @Override
            Map<String, Object> sampleSource() {
                return Map.of("semantic_field", randomSemanticInput(true));
            }

            @Override
            Map<String, Object> nullSource() {
                final Map<String, Object> source = new HashMap<>();
                source.put("semantic_field", null);
                return source;
            }
        };

        abstract String getMapping();
        abstract Map<String, Object> sampleSource();
        abstract Map<String, Object> nullSource();
    }

    public ShardBulkInferenceActionFilterBasicLicenseIT(SemanticFieldType fieldTypeTestParameter, boolean useLegacyFormat) {
        this.fieldTypeTestParameter = fieldTypeTestParameter;
        this.useLegacyFormat = useLegacyFormat;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { SemanticFieldType.SEMANTIC_TEXT, true }, new Object[] { SemanticFieldType.SEMANTIC_TEXT, false },
            new Object[] { SemanticFieldType.SEMANTIC, false });
    }

    @Before
    public void setup() throws Exception {
        ModelRegistry modelRegistry = internalCluster().getCurrentMasterNodeInstance(ModelRegistry.class);
        registerModel(modelRegistry, SPARSE_INFERENCE_ID, TaskType.SPARSE_EMBEDDING);
        registerModel(modelRegistry, DENSE_INFERENCE_ID, TaskType.TEXT_EMBEDDING);
        registerModel(modelRegistry, EMBEDDING_INFERENCE_ID, TaskType.EMBEDDING);
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
    protected boolean forbidPrivateIndexSettings() {
        // For setting index version
        return false;
    }

    @Override
    public Settings indexSettings() {
        var settingsBuilder = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10));

        if (useLegacyFormat) {
            settingsBuilder.put(
                IndexMetadata.SETTING_VERSION_CREATED,
                SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat)
            );
            settingsBuilder.put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat);
        }

        return settingsBuilder.build();
    }

    public void testLicenseInvalidForInference() {
        prepareCreate(INDEX_NAME).setMapping(fieldTypeTestParameter.getMapping()).get();

        BulkRequestBuilder bulkRequest = client().prepareBulk();
        int totalBulkReqs = randomIntBetween(2, 100);
        for (int i = 0; i < totalBulkReqs; i++) {
            bulkRequest.add(new IndexRequestBuilder(client()).setIndex(INDEX_NAME)
                .setId(Long.toString(i)).setSource(fieldTypeTestParameter.sampleSource()));
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
        prepareCreate(INDEX_NAME).setMapping(fieldTypeTestParameter.getMapping()).get();

        BulkRequestBuilder bulkRequest = client().prepareBulk();
        int totalBulkReqs = randomIntBetween(2, 100);
        for (int i = 0; i < totalBulkReqs; i++) {
            bulkRequest.add(new IndexRequestBuilder(client()).setIndex(INDEX_NAME).setId(Long.toString(i))
                .setSource(fieldTypeTestParameter.nullSource()));
        }

        BulkResponse bulkResponse = bulkRequest.get();
        assertFalse(bulkResponse.hasFailures());
    }
}
