/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class InferencePluginTests extends ESTestCase {
    private InferencePlugin inferencePlugin;

    private Boolean elasticInferenceServiceEnabled = true;

    private void setElasticInferenceServiceEnabled(Boolean elasticInferenceServiceEnabled) {
        this.elasticInferenceServiceEnabled = elasticInferenceServiceEnabled;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        Settings settings = Settings.builder().build();
        inferencePlugin = new InferencePlugin(settings) {
            @Override
            protected Boolean isElasticInferenceServiceEnabled() {
                return elasticInferenceServiceEnabled;
            }
        };
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        // set default value for elasticInferenceServiceEnabled
        setElasticInferenceServiceEnabled(true);
    }

    @Test
    public void testElasticInferenceServiceSettingsPresent() throws Exception {
        boolean anyMatch = inferencePlugin.getSettings()
            .stream()
            .map(Setting::getKey)
            .anyMatch(key -> key.startsWith(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX));

        MatcherAssert.assertThat("xpack.inference.elastic settings are present", anyMatch, is(true));
    }

    @Test
    public void testElasticInferenceServiceSettingsNotPresent() throws Exception {
        setElasticInferenceServiceEnabled(false); // disable elastic inference service
        boolean noneMatch = inferencePlugin.getSettings()
            .stream()
            .map(Setting::getKey)
            .noneMatch(key -> key.startsWith(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX));

        assertThat("xpack.inference.elastic settings are not present", noneMatch, is(true));
    }
}
