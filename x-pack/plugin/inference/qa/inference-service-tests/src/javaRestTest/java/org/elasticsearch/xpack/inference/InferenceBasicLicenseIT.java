/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.mockSparseServiceModelConfig;

public class InferenceBasicLicenseIT extends InferenceLicenseBaseRestTest {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "true")
        .user("x_pack_rest_user", "x-pack-test-password")
        .plugin("inference-service-test")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testPutModel_RestrictedWithBasicLicense() throws Exception {
        var endpoint = Strings.format("_inference/%s/%s?error_trace", TaskType.SPARSE_EMBEDDING, "endpoint-id");
        var modelConfig = mockSparseServiceModelConfig(null, true);
        sendRestrictedRequest("PUT", endpoint, modelConfig);
    }

    public void testUpdateModel_RestrictedWithBasicLicense() throws Exception {
        var endpoint = Strings.format("_inference/%s/%s/_update?error_trace", TaskType.SPARSE_EMBEDDING, "endpoint-id");
        var requestBody = """
            {
              "task_settings": {
                "num_threads": 2
              }
            }
            """;
        sendRestrictedRequest("PUT", endpoint, requestBody);
    }

    public void testPerformInference_RestrictedWithBasicLicense() throws Exception {
        var endpoint = Strings.format("_inference/%s/%s?error_trace", TaskType.SPARSE_EMBEDDING, "endpoint-id");
        var requestBody = """
            {
              "input": ["washing", "machine"]
            }
            """;
        sendRestrictedRequest("POST", endpoint, requestBody);
    }

    public void testGetServices_NonRestrictedWithBasicLicense() throws Exception {
        var endpoint = "_inference/_services";
        sendNonRestrictedRequest("GET", endpoint, null, 200, false);
    }

    public void testGetModels_NonRestrictedWithBasicLicense() throws Exception {
        var endpoint = "_inference/_all";
        sendNonRestrictedRequest("GET", endpoint, null, 200, false);
    }

    public void testDeleteModel_NonRestrictedWithBasicLicense() throws Exception {
        var endpoint = Strings.format("_inference/%s/%s?error_trace", TaskType.SPARSE_EMBEDDING, "endpoint-id");
        sendNonRestrictedRequest("DELETE", endpoint, null, 404, true);
    }
}
