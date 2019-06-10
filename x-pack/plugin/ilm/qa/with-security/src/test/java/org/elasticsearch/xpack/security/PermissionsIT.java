/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.RolloverAction;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PermissionsIT extends ESRestTestCase {
    private static final String jsonDoc = "{ \"name\" : \"elasticsearch\", \"body\": \"foo bar\" }";

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
        createNewSingletonPolicy(client(), deletePolicy,"delete", new DeleteAction());
    }

    /**
     * Tests that a policy that simply deletes an index after 0s succeeds when an index
     * with user `test_admin` is created referencing a policy created by `test_ilm` when both
     * users have read/write permissions on the index. The goal is to verify that one
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
                assertThat(indexExplain.get("failed_step"), equalTo("wait-for-shard-history-leases"));
                Map<String, String> stepInfo = (Map<String, String>) indexExplain.get("step_info");
                assertThat(stepInfo.get("type"), equalTo("security_exception"));
                assertThat(stepInfo.get("reason"), equalTo("action [indices:monitor/stats] is unauthorized for user [test_ilm]"));
            }
        });
    }

    public void testCanViewExplainOnUnmanagedIndex() throws Exception {
        createIndexAsAdmin("view-only-ilm", indexSettingsWithPolicy, "");
        Request request = new Request("GET", "/view-only-ilm/_ilm/explain");
        // test_ilm user has permissions to view
        assertOK(client().performRequest(request));
    }

    /**
     * Tests when the user is limited by alias of an index is able to write to index
     * which was rolled over by an ILM policy.
     */
    @TestLogging("org.elasticsearch:DEBUG")
    public void testWhenUserLimitedByOnlyAliasOfIndexCanWriteToIndexWhichWasRolledoverByILMPolicy()
            throws IOException, InterruptedException {
        /*
         * Setup:
         * - ILM policy to rollover index when max docs condition is met
         * - Index template to which the ILM policy applies and create Index
         * - Create role with just write and manage privileges on alias
         * - Create user and assign newly created role.
         */
        createNewSingletonPolicy(adminClient(), "foo-policy", "hot", new RolloverAction(null, null, 2L));
        createIndexTemplate("foo-template", "foo-logs-*", "foo_alias", "foo-policy");
        createIndexAsAdmin("foo-logs-000001", "foo_alias", randomBoolean());
        createRole("foo_alias_role", "foo_alias");
        createUser("test_user", "x-pack-test-password", "foo_alias_role");

        // test_user: index docs using alias in the newly created index
        indexDocs("test_user", "x-pack-test-password", "foo_alias", 2);
        refresh("foo_alias");

        // wait so the ILM policy triggers rollover action, verify that the new index exists
        assertThat(awaitBusy(() -> {
            Request request = new Request("HEAD", "/" + "foo-logs-000002");
            int status;
            try {
                status = adminClient().performRequest(request).getStatusLine().getStatusCode();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return status == 200;
        }), is(true));

        // test_user: index docs using alias, now should be able write to new index
        indexDocs("test_user", "x-pack-test-password", "foo_alias", 1);
        refresh("foo_alias");

        // verify that the doc has been indexed into new write index
        awaitBusy(() -> {
            Request request = new Request("GET", "/foo-logs-000002/_search");
            Response response;
            try {
                response = adminClient().performRequest(request);
                try (InputStream content = response.getEntity().getContent()) {
                    Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
                    return ((Integer) XContentMapValues.extractValue("hits.total.value", map)) == 1;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void createNewSingletonPolicy(RestClient client, String policy, String phaseName, LifecycleAction action) throws IOException {
        Phase phase = new Phase(phaseName, TimeValue.ZERO, singletonMap(action.getWriteableName(), action));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, singletonMap(phase.getName(), phase));
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policy);
        request.setEntity(entity);
        assertOK(client.performRequest(request));
    }

    private void createIndexAsAdmin(String name, Settings settings, String mapping) throws IOException {
        Request request = new Request("PUT", "/" + name);
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(settings)
            + ", \"mappings\" : {" + mapping + "} }");
        assertOK(adminClient().performRequest(request));
    }

    private void createIndexAsAdmin(String name, String alias, boolean isWriteIndex) throws IOException {
        Request request = new Request("PUT", "/" + name);
        request.setJsonEntity("{ \"aliases\": { \""+alias+"\": {" + ((isWriteIndex) ? "\"is_write_index\" : true" : "")
            + "} } }");
        assertOK(adminClient().performRequest(request));
    }

    private void createIndexTemplate(String name, String pattern, String alias, String policy) throws IOException {
        Request request = new Request("PUT", "/_template/" + name);
        request.setJsonEntity("{\n" +
                "                \"index_patterns\": [\""+pattern+"\"],\n" +
                "                \"settings\": {\n" +
                "                   \"number_of_shards\": 1,\n" +
                "                   \"number_of_replicas\": 0,\n" +
                "                   \"index.lifecycle.name\": \""+policy+"\",\n" +
                "                   \"index.lifecycle.rollover_alias\": \""+alias+"\"\n" +
                "                 }\n" +
                "              }");
        assertOK(adminClient().performRequest(request));
    }

    private void createUser(String name, String password, String role) throws IOException {
        Request request = new Request("PUT", "/_security/user/" + name);
        request.setJsonEntity("{ \"password\": \""+password+"\", \"roles\": [ \""+ role+"\"] }");
        assertOK(adminClient().performRequest(request));
    }

    private void createRole(String name, String alias) throws IOException {
        Request request = new Request("PUT", "/_security/role/" + name);
        request.setJsonEntity("{ \"indices\": [ { \"names\" : [ \""+ alias+"\"], \"privileges\": [ \"write\", \"manage\" ] } ] }");
        assertOK(adminClient().performRequest(request));
    }

    private void indexDocs(String user, String passwd, String index, int noOfDocs) throws IOException {
        RestClientBuilder builder = RestClient.builder(adminClient().getNodes().toArray(new Node[0]));
        String token = basicAuthHeaderValue(user, new SecureString(passwd.toCharArray()));
        configureClient(builder, Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build());
        builder.setStrictDeprecationMode(true);
        try (RestClient userClient = builder.build();) {

            for (int cnt = 0; cnt < noOfDocs; cnt++) {
                Request request = new Request("POST", "/" + index + "/_doc");
                request.setJsonEntity(jsonDoc);
                assertOK(userClient.performRequest(request));
            }
        }
    }

    private void refresh(String index) throws IOException {
        Request request = new Request("POST", "/" + index + "/_refresh");
        assertOK(adminClient().performRequest(request));
    }
}
