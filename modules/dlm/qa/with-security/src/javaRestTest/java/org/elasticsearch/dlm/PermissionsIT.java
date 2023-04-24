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
        String token = basicAuthHeaderValue("test_not_dlm", new SecureString("x-pack-test-password".toCharArray()));
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

    public void testStuff() {
        assertTrue(true);
    }

    /**
     * Tests that a policy that simply deletes an index after 0s succeeds when an index
     * with user `test_admin` is created referencing a policy created by `test_dlm` when both
     * users have read/write permissions on the index. The goal is to verify that one
     * does not need to be the same user who created both the policy and the index to have the
     * index be properly managed by DLM.
     */
    public void testCanManageIndexAndPolicyDifferentUsers() throws Exception {
        String index = "dlm-00001";
        createIndexAsAdmin(index);
        assertBusy(() -> assertTrue(indexExists(index)));
        Response response = client().performRequest(new Request("POST", "/" + index + "/_lifecycle/explain"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        response = client().performRequest(new Request("GET", "_data_stream/" + index + "/_lifecycle"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        response = client().performRequest(new Request("DELETE", "_data_stream/" + index + "/_lifecycle"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        Request putRequest = new Request("PUT", "_data_stream/" + index + "/_lifecycle");
        putRequest.setJsonEntity("{}");
        response = client().performRequest(putRequest);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        try (
            RestClient nonDlmManagerClient = buildClient(
                restUnprivilegedClientSettings(),
                getClusterHosts().toArray(new HttpHost[getClusterHosts().size()])
            )
        ) {
            response = nonDlmManagerClient.performRequest(new Request("POST", "/" + index + "/_lifecycle/explain"));
            assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
            response = nonDlmManagerClient.performRequest(new Request("GET", "_data_stream/" + index + "/_lifecycle"));
            assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> nonDlmManagerClient.performRequest(new Request("DELETE", "_data_stream/" + index + "/_lifecycle"))
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.FORBIDDEN.getStatus()));
            exception = expectThrows(ResponseException.class, () -> nonDlmManagerClient.performRequest(putRequest));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.FORBIDDEN.getStatus()));
        }
    }

    private void createIndexAsAdmin(String name) throws IOException {
        Request request = new Request("PUT", "/" + name);
        assertOK(adminClient().performRequest(request));
    }

}
