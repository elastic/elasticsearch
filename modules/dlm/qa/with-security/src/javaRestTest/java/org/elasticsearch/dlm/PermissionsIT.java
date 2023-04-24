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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class PermissionsIT extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_dlm", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    protected Settings restUnprivilegedClientSettings() {
        String token = basicAuthHeaderValue("test_non_privileged", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void init() throws Exception {
        Request request = new Request("PUT", "/_cluster/settings");
        XContentBuilder pollIntervalEntity = JsonXContent.contentBuilder();
        pollIntervalEntity.startObject();
        pollIntervalEntity.startObject("persistent");
        pollIntervalEntity.field(DataLifecycleService.DLM_POLL_INTERVAL, "1s");
        pollIntervalEntity.field(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
        pollIntervalEntity.endObject();
        pollIntervalEntity.endObject();
        request.setJsonEntity(Strings.toString(pollIntervalEntity));
        assertOK(adminClient().performRequest(request));
    }

    /**
     * Tests that a policy that simply deletes an index after 0s succeeds when an index
     * with user `test_admin` is created referencing a policy created by `test_dlm` when both
     * users have read/write permissions on the index. The goal is to verify that one
     * does not need to be the same user who created both the policy and the index to have the
     * index be properly managed by DLM.
     */
    public void testManageDLM() throws Exception {
        String index = "dlm-00001";
        createIndexAsAdmin(index);
        assertBusy(() -> assertTrue(indexExists(index)));
        Request explainLifecycleRequest = new Request("POST", "/" + index + "/_lifecycle/explain");
        Request getLifecycleRequest = new Request("GET", "_data_stream/" + index + "/_lifecycle");
        Request deleteLifecycleRequest = new Request("DELETE", "_data_stream/" + index + "/_lifecycle");
        Request putLifecycleRequest = new Request("PUT", "_data_stream/" + index + "/_lifecycle");
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

    private void makeRequest(RestClient client, Request request, boolean expectSuccess) throws IOException {
        if (expectSuccess) {
            Response response = client.performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        } else {
            ResponseException exception = expectThrows(ResponseException.class, () -> client.performRequest(request));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.FORBIDDEN.getStatus()));
        }
    }

    private void createIndexAsAdmin(String name) throws IOException {
        Request request = new Request("PUT", "/" + name);
        assertOK(adminClient().performRequest(request));
    }

}
