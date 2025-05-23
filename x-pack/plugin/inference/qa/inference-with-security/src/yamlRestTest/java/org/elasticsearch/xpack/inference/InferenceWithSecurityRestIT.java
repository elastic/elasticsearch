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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InferenceWithSecurityRestIT extends ESClientYamlSuiteTestCase {

    private static final String TEST_ADMIN_USERNAME = "x_pack_rest_user";
    private static final String TEST_ADMIN_PASSWORD = "x-pack-test-password";

    private static final String INFERENCE_USERNAME = "inference_user";
    private static final String INFERENCE_PASSWORD = "inference_user_password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .systemProperty("tests.seed", System.getProperty("tests.seed"))
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user(TEST_ADMIN_USERNAME, TEST_ADMIN_PASSWORD)    // admin user for setup and teardown
        .user(INFERENCE_USERNAME, INFERENCE_PASSWORD, "monitor_only_user", false)
        .plugin("inference-service-test")
        .distribution(DistributionType.DEFAULT)
        .build();

    public InferenceWithSecurityRestIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(INFERENCE_USERNAME, new SecureString(INFERENCE_PASSWORD.toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, new SecureString(TEST_ADMIN_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
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
