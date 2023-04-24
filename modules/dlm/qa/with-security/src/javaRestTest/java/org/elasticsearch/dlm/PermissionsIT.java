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
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

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

    @Before
    public void init() throws Exception {
        Request request = new Request("PUT", "/_cluster/settings");
        XContentBuilder clusterSettingsEntity = JsonXContent.contentBuilder();
        clusterSettingsEntity.startObject();
        clusterSettingsEntity.startObject("persistent");
        clusterSettingsEntity.field("indices.dlm.poll_interval", "1s");
        clusterSettingsEntity.field(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
        clusterSettingsEntity.endObject();
        clusterSettingsEntity.endObject();
        request.setJsonEntity(Strings.toString(clusterSettingsEntity));
        assertOK(adminClient().performRequest(request));
    }

    /**
     * Tests that a policy that simply deletes an index after 0s succeeds when an index
     * with user `test_admin` is created referencing a policy created by `test_dlm` when both
     * users have read/write permissions on the index. The goal is to verify that one
     * does not need to be the same user who created both the policy and the index to have the
     * index be properly managed by DLM.
     */
    @SuppressWarnings("unchecked")
    public void testManageDLM() throws Exception {
        String dataStreamName = "dlm-test"; // Needs to match the pattern of the names in roles.yml
        createDataStreamAsAdmin(dataStreamName);
        Response createDatastreamRepsonse = adminClient().performRequest(new Request("GET", "/_data_stream/" + dataStreamName));
        final List<Map<String, Object>> nodes = ObjectPath.createFromResponse(createDatastreamRepsonse).evaluate("data_streams");
        String index = (String) ((List<Map<String, Object>>) nodes.get(0).get("indices")).get(0).get("index_name");

        Request explainLifecycleRequest = new Request("POST", "/" + index + "/_lifecycle/explain");
        Request getLifecycleRequest = new Request("GET", "_data_stream/" + dataStreamName + "/_lifecycle");
        Request deleteLifecycleRequest = new Request("DELETE", "_data_stream/" + dataStreamName + "/_lifecycle");
        Request putLifecycleRequest = new Request("PUT", "_data_stream/" + dataStreamName + "/_lifecycle");
        putLifecycleRequest.setJsonEntity("{}");

        makeRequest(client(), explainLifecycleRequest, true);
        makeRequest(client(), getLifecycleRequest, true);
        makeRequest(client(), deleteLifecycleRequest, true);
        makeRequest(client(), putLifecycleRequest, true);

        try (RestClient nonDlmManagerClient = buildClient(restUnprivilegedClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            makeRequest(nonDlmManagerClient, explainLifecycleRequest, true);
            makeRequest(nonDlmManagerClient, getLifecycleRequest, true);
            makeRequest(nonDlmManagerClient, deleteLifecycleRequest, false);
            makeRequest(nonDlmManagerClient, putLifecycleRequest, false);
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
