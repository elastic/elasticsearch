/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import static org.hamcrest.Matchers.is;

public class StackTemplatesRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .keystore("bootstrap.password", "x-pack-test-password")
        .user("x_pack_rest_user", "x-pack-test-password")
        .systemProperty("es.queryable_built_in_roles_enabled", "false")
        .build();

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password"));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    public void testTemplatesCanBeDisabled() throws Exception {
        RestClient client = client();
        // Ensure the logs template has been added
        assertBusy(() -> {
            try {
                Request request = new Request("GET", "_index_template/logs");
                assertOK(client.performRequest(request));
            } catch (ResponseException e) {
                fail(e.getMessage());
            }
        });
        // Disable the stack templates
        {
            Request request = new Request("PUT", "_cluster/settings");
            request.setJsonEntity("""
                {
                  "persistent": {
                    "stack.templates.enabled": false
                  }
                }
                """);
            assertOK(client.performRequest(request));
        }
        Request getRequest = new Request("GET", "_index_template/logs");
        assertOK(client.performRequest(getRequest));

        Request deleteRequest = new Request("DELETE", "_index_template/logs");
        assertOK(client.performRequest(deleteRequest));
        ResponseException exception = expectThrows(ResponseException.class, () -> client.performRequest(deleteRequest));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(404));
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
