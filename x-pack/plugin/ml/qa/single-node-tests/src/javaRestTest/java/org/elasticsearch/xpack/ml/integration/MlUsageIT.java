/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// Test the phone home/telemetry data
public class MlUsageIT extends ESRestTestCase {

    @SuppressWarnings("unchecked")
    public void testMLUsage() throws IOException {
        Request request = new Request("GET", "/_xpack/usage");
        var usage = entityAsMap(client().performRequest(request).getEntity());

        var ml = (Map<String, Object>) usage.get("ml");
        assertNotNull(usage.toString(), ml);
        var memoryUsage = (Map<String, Object>) ml.get("memory");
        assertNotNull(ml.toString(), memoryUsage);
        assertThat(memoryUsage.toString(), (Integer) memoryUsage.get("anomaly_detectors_memory_bytes"), greaterThanOrEqualTo(0));
        assertThat(memoryUsage.toString(), (Integer) memoryUsage.get("data_frame_analytics_memory_bytes"), greaterThanOrEqualTo(0));
        assertThat(memoryUsage.toString(), (Integer) memoryUsage.get("pytorch_inference_memory_bytes"), greaterThanOrEqualTo(0));
        assertThat(memoryUsage.toString(), (Integer) memoryUsage.get("total_used_memory_bytes"), greaterThanOrEqualTo(0));
    }
}
