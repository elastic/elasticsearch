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
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class SemanticNonDynamicFieldMapperTests extends NonDynamicFieldMapperTestCase {
    private static final Model EMBEDDING_MODEL = new TestDenseInferenceServiceExtension.TestDenseModel(
        "test-endpoint",
        TaskType.EMBEDDING,
        new TestDenseInferenceServiceExtension.TestServiceSettings(
            "embedding_model",
            1024,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT
        )
    );

    @ParametersFactory(argumentFormatting = "type=%s")
    public static List<Object[]> parameters() {
        return List.of(new Object[] { SemanticFieldMapper.CONTENT_TYPE }, new Object[] { SemanticTextFieldMapper.CONTENT_TYPE });
    }

    private final String fieldType;

    public SemanticNonDynamicFieldMapperTests(String fieldType) {
        this.fieldType = fieldType;
    }

    @Before
    public void setup() throws Exception {
        ModelRegistry modelRegistry = node().injector().getInstance(ModelRegistry.class);
        Utils.storeModel(modelRegistry, EMBEDDING_MODEL);
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
        return fieldType;
    }

    @Override
    protected String getMapping() {
        return String.format(Locale.ROOT, """
            "type": "%s",
            "inference_id": "%s"
            """, fieldType, "test-endpoint");
    }
}
