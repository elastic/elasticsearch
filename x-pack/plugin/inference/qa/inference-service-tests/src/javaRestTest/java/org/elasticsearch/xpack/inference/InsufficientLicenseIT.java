/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class InsufficientLicenseIT extends ESRestTestCase {

    private static final String PASSWORD = "secret-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "true")
        .plugin("inference-service-test")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        // use the privileged users here but not in the tests
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testInsufficientLicense() throws IOException {
        var putRequest = new Request("PUT", "_inference/sparse_embedding/license_test");
        putRequest.setJsonEntity("""
            {
              "service": "elser",
              "service_settings": {
                "num_allocations": 1,
                "num_threads": 1
              }
            }
            """);
        var getRequest = new Request("GET", "_inference/sparse_embedding/license_test");

        try (RestClient client = buildClient(restClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            // Creating inference endpoint will return a license error
            ResponseException putException = expectThrows(ResponseException.class, () -> client.performRequest(putRequest));
            assertThat(putException.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.FORBIDDEN.getStatus()));
            assertThat(putException.getMessage(), containsString("current license is non-compliant for [ml]"));

            // Assert no inference endpoint created
            ResponseException getException = expectThrows(ResponseException.class, () -> client.performRequest(getRequest));
            assertThat(getException.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.NOT_FOUND.getStatus()));
            assertThat(getException.getMessage(), containsString("Inference endpoint not found [license_test]"));
        }
    }
}
