/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

public class DataStreamWithSecurityIT extends ESRestTestCase {

    private static final String PASSWORD = "secret-test-password";
    private static final String DATA_STREAM_NAME = "my-ds";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .user("test_admin", PASSWORD, "superuser", false)
        .user("limited_user", PASSWORD, "only_get", false)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        // If this test is running in a test framework that handles its own authorization, we don't want to overwrite it.
        if (super.restClientSettings().keySet().contains(ThreadContext.PREFIX + ".Authorization")) {
            return super.restClientSettings();
        } else {
            // Note: We use the admin user because the other one is too unprivileged, so it breaks the initialization of the test
            String token = basicAuthHeaderValue("test_admin", new SecureString(PASSWORD.toCharArray()));
            return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
        }
    }

    private Settings simpleUserRestClientSettings() {
        // Note: This user is assigned the role "only_get". That role is defined in roles.yml.
        String token = basicAuthHeaderValue("limited_user", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testGetDataStreamWithoutPermission() throws Exception {
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/my-ds-template");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": ["my-ds*"],
              "data_stream": {}
            }
            """);
        assertOK(adminClient().performRequest(putComposableIndexTemplateRequest));
        assertOK(adminClient().performRequest(new Request("PUT", "/_data_stream/" + DATA_STREAM_NAME)));
        Request createDocRequest = new Request("POST", "/" + DATA_STREAM_NAME + "/_doc");
        createDocRequest.setJsonEntity("{ \"@timestamp\": \"2022-01-01\", \"message\": \"foo\" }");
        assertOK(adminClient().performRequest(createDocRequest));

        // Both the verbose and non-verbose versions should work with the "simple" user
        try (var simpleUserClient = buildClient(simpleUserRestClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            Request getDs = new Request("GET", "/_data_stream");
            assertOK(simpleUserClient.performRequest(getDs));

            Request getDsVerbose = new Request("GET", "/_data_stream?verbose=true");
            assertOK(simpleUserClient.performRequest(getDsVerbose));
        }
    }

}
