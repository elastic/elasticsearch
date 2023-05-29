/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PermissionsIT extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        // Note: This user is defined in build.gradle, and assigned the role "manage_dlm". That role is defined in roles.yml.
        String token = basicAuthHeaderValue("test_dlm", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private Settings restUnprivilegedClientSettings() {
        // Note: This user is defined in build.gradle, and assigned the role "not_privileged". That role is defined in roles.yml.
        String token = basicAuthHeaderValue("test_non_privileged", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @SuppressWarnings("unchecked")
    public void testManageDLM() throws Exception {
        {
            /*
             * This test checks that a user with the "manage_dlm" index privilege on "dlm-*" data streams can delete and put a lifecycle
             * on the "dlm-test" data stream, while a user with who does not have that privilege (but does have all of the other same
             * "dlm-*" privileges) cannot delete or put a lifecycle on that datastream.
             */
            String dataStreamName = "dlm-test"; // Needs to match the pattern of the names in roles.yml
            createDataStreamAsAdmin(dataStreamName);
            Response getDatastreamRepsonse = adminClient().performRequest(new Request("GET", "/_data_stream/" + dataStreamName));
            final List<Map<String, Object>> nodes = ObjectPath.createFromResponse(getDatastreamRepsonse).evaluate("data_streams");
            String index = (String) ((List<Map<String, Object>>) nodes.get(0).get("indices")).get(0).get("index_name");

            Request explainLifecycleRequest = new Request("GET", "/" + randomFrom("_all", "*", index) + "/_lifecycle/explain");
            Request getLifecycleRequest = new Request("GET", "_data_stream/" + randomFrom("_all", "*", dataStreamName) + "/_lifecycle");
            Request deleteLifecycleRequest = new Request(
                "DELETE",
                "_data_stream/" + randomFrom("_all", "*", dataStreamName) + "/_lifecycle"
            );
            Request putLifecycleRequest = new Request("PUT", "_data_stream/" + randomFrom("_all", "*", dataStreamName) + "/_lifecycle");
            putLifecycleRequest.setJsonEntity("{}");

            makeRequest(client(), explainLifecycleRequest, true);
            makeRequest(client(), getLifecycleRequest, true);
            makeRequest(client(), deleteLifecycleRequest, true);
            makeRequest(client(), putLifecycleRequest, true);

            try (
                RestClient nonDlmManagerClient = buildClient(restUnprivilegedClientSettings(), getClusterHosts().toArray(new HttpHost[0]))
            ) {
                makeRequest(nonDlmManagerClient, explainLifecycleRequest, true);
                makeRequest(nonDlmManagerClient, getLifecycleRequest, true);
                makeRequest(nonDlmManagerClient, deleteLifecycleRequest, false);
                makeRequest(nonDlmManagerClient, putLifecycleRequest, false);
            }
        }
        {
            // Now test that the user who has the manage_dlm privilege on dlm-* data streams cannot manage other data streams:
            String otherDataStreamName = "other-dlm-test";
            createDataStreamAsAdmin(otherDataStreamName);
            Response getOtherDataStreamResponse = adminClient().performRequest(new Request("GET", "/_data_stream/" + otherDataStreamName));
            final List<Map<String, Object>> otherNodes = ObjectPath.createFromResponse(getOtherDataStreamResponse).evaluate("data_streams");
            String otherIndex = (String) ((List<Map<String, Object>>) otherNodes.get(0).get("indices")).get(0).get("index_name");
            Request putOtherLifecycleRequest = new Request("PUT", "_data_stream/" + otherDataStreamName + "/_lifecycle");
            putOtherLifecycleRequest.setJsonEntity("{}");
            makeRequest(client(), new Request("GET", "/" + otherIndex + "/_lifecycle/explain"), false);
            makeRequest(client(), new Request("GET", "_data_stream/" + otherDataStreamName + "/_lifecycle"), false);
            makeRequest(client(), new Request("DELETE", "_data_stream/" + otherDataStreamName + "/_lifecycle"), false);
            makeRequest(client(), putOtherLifecycleRequest, false);
        }
    }

    /*
     * This makes the given request with the given client. It asserts a 200 response if expectSuccess is true, and asserts an exception
     * with a 403 response if expectStatus is false.
     */
    private void makeRequest(RestClient client, Request request, boolean expectSuccess) throws IOException {
        if (expectSuccess) {
            Response response = client.performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        } else {
            ResponseException exception = expectThrows(ResponseException.class, () -> client.performRequest(request));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.FORBIDDEN.getStatus()));
        }
    }

    private void createDataStreamAsAdmin(String name) throws IOException {
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
