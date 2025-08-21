/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * This test case sets up users with different privileges respective to managing a data stream's lifecycle. Furthermore,
 * it provides some helper methods and sets up a data stream.
 */
public abstract class DataStreamLifecyclePermissionsTestCase extends ESRestTestCase {

    private static final String PASSWORD = "secret-test-password";
    // Needs to match the pattern of the names in roles.yml
    static final String DATA_STREAM_NAME = "data-stream-lifecycle-test";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .user("test_admin", PASSWORD, "superuser", false)
        .user("test_data_stream_lifecycle", PASSWORD, "manage_data_stream_lifecycle", false)
        .user("test_non_privileged", PASSWORD, "under_privilged", false)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .build();
    String index;

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
            // Note: This user is assigned the role "manage_data_stream_lifecycle". That role is defined in roles.yml.
            String token = basicAuthHeaderValue("test_data_stream_lifecycle", new SecureString(PASSWORD.toCharArray()));
            return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
        }
    }

    @Override
    protected Settings restAdminSettings() {
        /*
         * Here we can't just check whether there is an authorization setting because super.restAdminSettings() calls restClientSettings(),
         * and that will have an authorization setting regardless. So we have to check whether it's been set to have the
         * test_data_stream_lifecycle user (which is what this class's restClientSettings() does if there is no other authorization already
         * set).
         */
        Settings superSettings = super.restAdminSettings();
        String authKey = ThreadContext.PREFIX + ".Authorization";
        String nonAdminToken = basicAuthHeaderValue("test_data_stream_lifecycle", new SecureString(PASSWORD.toCharArray()));
        if (superSettings.keySet().contains(authKey) && superSettings.get(authKey).equals(nonAdminToken) == false) {
            return superSettings;
        } else {
            String token = basicAuthHeaderValue("test_admin", new SecureString(PASSWORD.toCharArray()));
            return Settings.builder().put(authKey, token).build();
        }
    }

    Settings restPrivilegedClientSettings() {
        // Note: This user is assigned the role "under_privilged". That role is defined in roles.yml.
        String token = basicAuthHeaderValue("test_data_stream_lifecycle", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    Settings restUnprivilegedClientSettings() {
        // Note: This user is assigned the role "under_privilged". That role is defined in roles.yml.
        String token = basicAuthHeaderValue("test_non_privileged", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws IOException {
        createDataStreamAsAdmin(DATA_STREAM_NAME);
        Response getDataStreamResponse = adminClient().performRequest(new Request("GET", "/_data_stream/" + DATA_STREAM_NAME));
        final List<Map<String, Object>> nodes = ObjectPath.createFromResponse(getDataStreamResponse).evaluate("data_streams");
        index = (String) ((List<Map<String, Object>>) nodes.get(0).get("indices")).get(0).get("index_name");
    }

    /*
     * This makes the given request with the given client. It asserts a 200 response if expectSuccess is true, and asserts an exception
     * with a 403 response if expectStatus is false.
     */
    void makeRequest(RestClient client, Request request, boolean expectSuccess) throws IOException {
        if (expectSuccess) {
            Response response = client.performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        } else {
            ResponseException exception = expectThrows(ResponseException.class, () -> client.performRequest(request));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.FORBIDDEN.getStatus()));
        }
    }

    void createDataStreamAsAdmin(String name) throws IOException {
        String mappingsTemplateName = name + "_mappings";
        Request mappingsRequest = new Request("PUT", "/_component_template/" + mappingsTemplateName);
        mappingsRequest.setJsonEntity("""
            {
              "template": {
                "mappings": {
                  "properties": {
                    "@timestamp": {
                      "type": "date",
                      "format": "date_optional_time||epoch_millis"
                    },
                    "message": {
                      "type": "wildcard"
                    }
                  }
                }
              }
            }""");
        assertOK(adminClient().performRequest(mappingsRequest));

        String settingsTemplateName = name + "_settings";
        Request settingsRequest = new Request("PUT", "/_component_template/" + settingsTemplateName);
        settingsRequest.setJsonEntity("""
            {
              "template": {
                  "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                  }
              }
            }""");
        assertOK(adminClient().performRequest(settingsRequest));

        Request indexTemplateRequest = new Request("PUT", "/_index_template/" + name + "_template");
        indexTemplateRequest.setJsonEntity(Strings.format("""
            {
                "index_patterns": ["%s*"],
                "data_stream": { },
                "composed_of": [ "%s", "%s" ]
            }""", name, mappingsTemplateName, settingsTemplateName));
        assertOK(adminClient().performRequest(indexTemplateRequest));

        Request request = new Request("PUT", "/_data_stream/" + name);
        assertOK(adminClient().performRequest(request));
    }
}
