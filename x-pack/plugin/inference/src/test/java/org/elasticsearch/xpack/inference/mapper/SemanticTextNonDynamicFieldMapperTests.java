/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.NonDynamicFieldMapperTests;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class SemanticTextNonDynamicFieldMapperTests extends NonDynamicFieldMapperTests {

    @Before
    public void setup() throws Exception {
        ModelRegistry modelRegistry = node().injector().getInstance(ModelRegistry.class);
        Utils.storeSparseModel(modelRegistry);
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
        return SemanticTextFieldMapper.CONTENT_TYPE;
    }

    @Override
    protected String getMapping() {
        return String.format(Locale.ROOT, """
            "type": "%s",
            "inference_id": "%s"
            """, SemanticTextFieldMapper.CONTENT_TYPE, TestSparseInferenceServiceExtension.TestInferenceService.NAME);
    }
}
