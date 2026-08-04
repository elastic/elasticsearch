/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InferenceRestIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .systemProperty("tests.seed", System.getProperty("tests.seed"))
        .setting("xpack.security.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .plugin("inference-service-test")
        .distribution(DistributionType.DEFAULT)
        .build();

    public InferenceRestIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected Settings restClientSettings() {
        var baseSettings = super.restClientSettings();
        return Settings.builder()
            .put(baseSettings)
            .put(CLIENT_SOCKET_TIMEOUT, "300s")  // Long timeout for model download
            .build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @After
    public void cleanup() throws Exception {
        for (var model : getAllModels()) {
            var inferenceId = model.get("inference_id");
            try {
                var endpoint = Strings.format("_inference/%s?force", inferenceId);
                adminClient().performRequest(new Request("DELETE", endpoint));
            } catch (Exception ex) {
                logger.warn(() -> "failed to delete inference endpoint " + inferenceId, ex);
            }
        }
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> getAllModels() throws IOException {
        var request = new Request("GET", "_inference/_all");
        var response = client().performRequest(request);
        return (List<Map<String, Object>>) entityAsMap(response).get("endpoints");
    }
}
