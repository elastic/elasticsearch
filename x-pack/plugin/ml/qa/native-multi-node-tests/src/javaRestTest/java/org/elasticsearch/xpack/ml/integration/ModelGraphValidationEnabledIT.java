/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Booleans;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for {@code xpack.ml.model_graph_validation_enabled}.
 * <p>
 * The PyTorch native process command line (including {@code --skipModelValidation} when disabled) is covered by unit
 * tests on {@code PyTorchBuilder}. This suite verifies the cluster setting is exposed and persisted via the REST API
 * (including operator-gated dynamic updates).
 */
public class ModelGraphValidationEnabledIT extends PyTorchModelRestTestCase {

    @SuppressWarnings("unchecked")
    public void testModelGraphValidationClusterSettingPersisted() throws IOException {
        try {
            Request put = new Request("PUT", "_cluster/settings");
            put.setJsonEntity("""
                {"persistent": {"xpack.ml.model_graph_validation_enabled": false}}""");
            assertOK(client().performRequest(put));

            Request get = new Request("GET", "_cluster/settings");
            Response getResponse = client().performRequest(get);
            Map<String, Object> map = entityAsMap(getResponse);
            @SuppressWarnings("unchecked")
            Map<String, Object> persistent = (Map<String, Object>) map.get("persistent");
            assertThat(persistent, notNullValue());
            Object value = XContentMapValues.extractValue(persistent, "xpack", "ml", "model_graph_validation_enabled");
            if (value == null) {
                value = persistent.get("xpack.ml.model_graph_validation_enabled");
            }
            assertThat(Booleans.parseBoolean(value.toString()), equalTo(false));
        } finally {
            Request reset = new Request("PUT", "_cluster/settings");
            reset.setJsonEntity("""
                {"persistent": {"xpack.ml.model_graph_validation_enabled": null}}""");
            assertOK(client().performRequest(reset));
        }
    }
}
