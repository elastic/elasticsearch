/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.NonDynamicFieldMapperTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class SemanticNonDynamicFieldMapperTests extends NonDynamicFieldMapperTestCase {

    private enum FieldType {
        SEMANTIC {
            @Override
            String typeName() {
                return SemanticFieldMapper.CONTENT_TYPE;
            }

            @Override
            String inferenceId() {
                return "test-endpoint";
            }

            @Override
            void storeModel(ModelRegistry modelRegistry) throws Exception {
                Utils.storeEmbeddingModel(
                    inferenceId(),
                    modelRegistry,
                    1024,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.FLOAT
                );
            }
        },
        SEMANTIC_TEXT {
            @Override
            String typeName() {
                return SemanticTextFieldMapper.CONTENT_TYPE;
            }

            @Override
            String inferenceId() {
                return "sparse-endpoint";
            }

            @Override
            void storeModel(ModelRegistry modelRegistry) throws Exception {
                Utils.storeSparseModel(inferenceId(), modelRegistry);
            }
        };

        abstract String typeName();

        abstract String inferenceId();

        abstract void storeModel(ModelRegistry modelRegistry) throws Exception;
    }

    @ParametersFactory(argumentFormatting = "type=%s")
    public static List<Object[]> parameters() {
        return List.of(new Object[] { FieldType.SEMANTIC }, new Object[] { FieldType.SEMANTIC_TEXT });
    }

    private final FieldType fieldType;

    public SemanticNonDynamicFieldMapperTests(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    @Before
    public void setup() throws Exception {
        ModelRegistry modelRegistry = node().injector().getInstance(ModelRegistry.class);
        fieldType.storeModel(modelRegistry);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateInferencePlugin.class);
    }

    @Override
    protected String getTypeName() {
        return fieldType.typeName();
    }

    @Override
    protected String getMapping() {
        return String.format(Locale.ROOT, """
            "type": "%s",
            "inference_id": "%s"
            """, fieldType.typeName(), fieldType.inferenceId());
    }
}
