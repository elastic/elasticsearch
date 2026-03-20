/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.contextconstrained;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ContextConstrainedActionIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .module("x-pack-inference")
        .module("x-pack-ilm")
        .module("x-pack-ml")
        .plugin("inference-service-test")
        .plugin("context-constrained-test")
        .user("admin", "admin-password")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * Verifies that dispatching a context-constrained action via NodeClient without
     * calling ContextConstrainedAction.openContext() is rejected by the AuthorizationService.
     */
    public void testContextConstrainedActionRejectedWithoutOpenContext() throws Exception {
        String inferenceId = "test-endpoint";
        Request createEndpoint = new Request("PUT", "/_inference/text_embedding/" + inferenceId);
        createEndpoint.setJsonEntity("""
            {
              "service": "text_embedding_test_service",
              "service_settings": {
                "model": "my_model",
                "dimensions": 256,
                "similarity": "cosine",
                "api_key": "abc64"
              }
            }
            """);
        assertOK(client().performRequest(createEndpoint));

        Request createIndex = new Request("PUT", "/test-index");
        createIndex.setJsonEntity(Strings.format("""
            {
              "mappings": {
                "properties": {
                  "content": {
                    "type": "semantic_text",
                    "inference_id": "%s"
                  }
                }
              }
            }
            """, inferenceId));
        assertOK(client().performRequest(createIndex));

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", "/test-index/_inference_fields_internal"))
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(500));
        assertThat(e.getMessage(), containsString("may only be executed as a child of"));
    }
}
