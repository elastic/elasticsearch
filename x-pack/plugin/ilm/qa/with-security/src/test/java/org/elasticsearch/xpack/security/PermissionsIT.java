/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public class PermissionsIT extends ESRestTestCase {

    private String deletePolicy = "deletePolicy";
    private Settings indexSettingsWithPolicy;

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_ilm", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Before
    public void init() throws Exception {
        Request request = new Request("PUT", "/_cluster/settings");
        XContentBuilder pollIntervalEntity = JsonXContent.contentBuilder();
        pollIntervalEntity.startObject();
        pollIntervalEntity.startObject("transient");
        pollIntervalEntity.field(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");
        pollIntervalEntity.endObject();
        pollIntervalEntity.endObject();
        request.setJsonEntity(Strings.toString(pollIntervalEntity));
        assertOK(adminClient().performRequest(request));
        indexSettingsWithPolicy = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_NAME, deletePolicy)
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        createNewSingletonPolicy(deletePolicy,"delete", new DeleteAction());
    }

    /**
     * Tests that a policy that simply deletes an index after 0s succeeds when an index
     * with user `test_admin` is created referencing a policy created by `test_ilm` when both
     * users have read/write permissions on the the index. The goal is to verify that one
     * does not need to be the same user who created both the policy and the index to have the
     * index be properly managed by ILM.
     */
    public void testCanManageIndexAndPolicyDifferentUsers() throws Exception {
        String index = "ilm-00001";
        createIndexAsAdmin(index, indexSettingsWithPolicy, "");
        assertBusy(() -> assertFalse(indexExists(index)));
    }

    /**
     * This tests the awkward behavior where an admin can have permissions to create a policy,
     * but then not have permissions to operate on an index that was later associated with that policy by another
     * user
     */
    @SuppressWarnings("unchecked")
    public void testCanManageIndexWithNoPermissions() throws Exception {
        createIndexAsAdmin("not-ilm", indexSettingsWithPolicy, "");
        Request request = new Request("GET", "/not-ilm/_ilm/explain");
        // test_ilm user does not have permissions on this index
        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.FORBIDDEN.getStatus()));

        assertBusy(() -> {
            Response response = adminClient().performRequest(request);
            assertOK(response);
            try (InputStream is = response.getEntity().getContent()) {
                Map<String, Object> mapResponse = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                Map<String, Object> indexExplain = (Map<String, Object>) ((Map<String, Object>) mapResponse.get("indices")).get("not-ilm");
                assertThat(indexExplain.get("managed"), equalTo(true));
                assertThat(indexExplain.get("step"), equalTo("ERROR"));
                assertThat(indexExplain.get("failed_step"), equalTo("delete"));
                Map<String, String> stepInfo = (Map<String, String>) indexExplain.get("step_info");
                assertThat(stepInfo.get("type"), equalTo("security_exception"));
                assertThat(stepInfo.get("reason"), equalTo("action [indices:admin/delete] is unauthorized for user [test_ilm]"));
            }
        });
    }

    public void testCanViewExplainOnUnmanagedIndex() throws Exception {
        createIndexAsAdmin("view-only-ilm", indexSettingsWithPolicy, "");
        Request request = new Request("GET", "/view-only-ilm/_ilm/explain");
        // test_ilm user has permissions to view
        assertOK(client().performRequest(request));
    }

    private void createNewSingletonPolicy(String policy, String phaseName, LifecycleAction action) throws IOException {
        Phase phase = new Phase(phaseName, TimeValue.ZERO, singletonMap(action.getWriteableName(), action));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, singletonMap(phase.getName(), phase));
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policy);
        request.setEntity(entity);
        client().performRequest(request);
    }

    private void createIndexAsAdmin(String name, Settings settings, String mapping) throws IOException {
        Request request = new Request("PUT", "/" + name);
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(settings)
            + ", \"mappings\" : {" + mapping + "} }");
        assertOK(adminClient().performRequest(request));
    }
}
