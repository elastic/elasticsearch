/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.slm.DeleteSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.ExecuteSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.ExecuteSnapshotLifecyclePolicyResponse;
import org.elasticsearch.client.slm.ExecuteSnapshotLifecycleRetentionRequest;
import org.elasticsearch.client.slm.GetSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.PutSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.client.slm.SnapshotRetentionConfiguration;
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
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
                assertThat(stepInfo.get("reason"), equalTo("action [indices:monitor/stats] is unauthorized" +
                    " for user [test_ilm]" +
                    " on indices [not-ilm]," +
                    " this action is granted by the privileges [monitor,manage,all]"));
            }
        });
    }

    public void testSLMWithPermissions() throws Exception {
        String repo = "my_repository";
        createIndexAsAdmin("index", Settings.builder().put("index.number_of_replicas", 0).build(), "");

        // Set up two roles and users, one for reading SLM, another for managing SLM
        Request roleRequest = new Request("PUT", "/_security/role/slm-read");
        roleRequest.setJsonEntity("{ \"cluster\": [\"read_slm\"] }");
        assertOK(adminClient().performRequest(roleRequest));
        roleRequest = new Request("PUT", "/_security/role/slm-manage");
        roleRequest.setJsonEntity("{ \"cluster\": [\"manage_slm\", \"cluster:admin/repository/*\", \"cluster:admin/snapshot/*\"]," +
            "\"indices\": [{ \"names\": [\".slm-history*\"],\"privileges\": [\"all\"] }] }");
        assertOK(adminClient().performRequest(roleRequest));

        createUser("slm_admin", "slm-pass", "slm-manage");
        createUser("slm_user", "slm-user-pass", "slm-read");

        final HighLevelClient hlAdminClient = new HighLevelClient(adminClient());

        // Build two high level clients, each using a different user
        final RestClientBuilder adminBuilder = RestClient.builder(adminClient().getNodes().toArray(new Node[0]));
        final String adminToken = basicAuthHeaderValue("slm_admin", new SecureString("slm-pass".toCharArray()));
        configureClient(adminBuilder, Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", adminToken)
            .build());
        adminBuilder.setStrictDeprecationMode(true);
        final RestHighLevelClient adminHLRC = new RestHighLevelClient(adminBuilder);

        final RestClientBuilder userBuilder = RestClient.builder(adminClient().getNodes().toArray(new Node[0]));
        final String userToken = basicAuthHeaderValue("slm_user", new SecureString("slm-user-pass".toCharArray()));
        configureClient(userBuilder, Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", userToken)
            .build());
        userBuilder.setStrictDeprecationMode(true);
        final RestHighLevelClient readHlrc = new RestHighLevelClient(userBuilder);

        PutRepositoryRequest repoRequest = new PutRepositoryRequest();

        Settings.Builder settingsBuilder = Settings.builder().put("location", ".");
        repoRequest.settings(settingsBuilder);
        repoRequest.name(repo);
        repoRequest.type(FsRepository.TYPE);
        org.elasticsearch.action.support.master.AcknowledgedResponse response =
            hlAdminClient.snapshot().createRepository(repoRequest, RequestOptions.DEFAULT);
        assertTrue(response.isAcknowledged());

        Map<String, Object> config = new HashMap<>();
        config.put("indices", Collections.singletonList("index"));
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
            "policy_id", "name", "1 2 3 * * ?", repo, config,
            new SnapshotRetentionConfiguration(TimeValue.ZERO, null, null));
        PutSnapshotLifecyclePolicyRequest request = new PutSnapshotLifecyclePolicyRequest(policy);

        expectThrows(ElasticsearchStatusException.class,
            () -> readHlrc.indexLifecycle().putSnapshotLifecyclePolicy(request, RequestOptions.DEFAULT));

        adminHLRC.indexLifecycle().putSnapshotLifecyclePolicy(request, RequestOptions.DEFAULT);

        GetSnapshotLifecyclePolicyRequest getRequest = new GetSnapshotLifecyclePolicyRequest("policy_id");
        readHlrc.indexLifecycle().getSnapshotLifecyclePolicy(getRequest, RequestOptions.DEFAULT);
        adminHLRC.indexLifecycle().getSnapshotLifecyclePolicy(getRequest, RequestOptions.DEFAULT);

        ExecuteSnapshotLifecyclePolicyRequest executeRequest = new ExecuteSnapshotLifecyclePolicyRequest("policy_id");
        expectThrows(ElasticsearchStatusException.class, () ->
            readHlrc.indexLifecycle().executeSnapshotLifecyclePolicy(executeRequest, RequestOptions.DEFAULT));

        ExecuteSnapshotLifecyclePolicyResponse executeResp =
            adminHLRC.indexLifecycle().executeSnapshotLifecyclePolicy(executeRequest, RequestOptions.DEFAULT);
        final String snapName = executeResp.getSnapshotName();

        assertBusy(() -> {
            try {
                logger.info("--> checking for snapshot to be created");
                GetSnapshotsRequest getSnaps = new GetSnapshotsRequest(repo);
                getSnaps.snapshots(new String[]{snapName});
                GetSnapshotsResponse getResp = adminHLRC.snapshot().get(getSnaps, RequestOptions.DEFAULT);
                assertThat(getResp.getSnapshots(repo).get(0).state(), equalTo(SnapshotState.SUCCESS));
            } catch (ElasticsearchException e) {
                fail("expected snapshot to exist but it does not: " + e.getDetailedMessage());
            }
        });

        ExecuteSnapshotLifecycleRetentionRequest executeRetention = new ExecuteSnapshotLifecycleRetentionRequest();
        expectThrows(ElasticsearchStatusException.class, () ->
            readHlrc.indexLifecycle().executeSnapshotLifecycleRetention(executeRetention, RequestOptions.DEFAULT));

        AcknowledgedResponse retentionResp =
            adminHLRC.indexLifecycle().executeSnapshotLifecycleRetention(executeRetention, RequestOptions.DEFAULT);
        assertTrue(retentionResp.isAcknowledged());

        assertBusy(() -> {
            try {
                logger.info("--> checking for snapshot to be deleted");
                GetSnapshotsRequest getSnaps = new GetSnapshotsRequest(repo);
                getSnaps.snapshots(new String[]{snapName});
                GetSnapshotsResponse getResp = adminHLRC.snapshot().get(getSnaps, RequestOptions.DEFAULT);
                assertThat(getResp.getSnapshots(repo).size(), equalTo(0));
            } catch (ElasticsearchException e) {
                // great, we want it to not exist
                assertThat(e.getDetailedMessage(), containsString("snapshot_missing_exception"));
            }
        });

        DeleteSnapshotLifecyclePolicyRequest deleteRequest = new DeleteSnapshotLifecyclePolicyRequest("policy_id");
        expectThrows(ElasticsearchStatusException.class, () ->
            readHlrc.indexLifecycle().deleteSnapshotLifecyclePolicy(deleteRequest, RequestOptions.DEFAULT));

        adminHLRC.indexLifecycle().deleteSnapshotLifecyclePolicy(deleteRequest, RequestOptions.DEFAULT);

        hlAdminClient.close();
        readHlrc.close();
        adminHLRC.close();
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
    public void testWhenUserLimitedByOnlyAliasOfIndexCanWriteToIndexWhichWasRolledoverByILMPolicy() throws Exception {
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
        assertBusy(() -> {
            Request request = new Request("HEAD", "/" + "foo-logs-000002");
            int status = adminClient().performRequest(request).getStatusLine().getStatusCode();
            assertThat(status, equalTo(200));
        });

        // test_user: index docs using alias, now should be able write to new index
        indexDocs("test_user", "x-pack-test-password", "foo_alias", 1);
        refresh("foo_alias");

        // verify that the doc has been indexed into new write index
        assertBusy(() -> {
            Request request = new Request("GET", "/foo-logs-000002/_search");
            Response response = adminClient().performRequest(request);
            try (InputStream content = response.getEntity().getContent()) {
                Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
                Integer totalHits = (Integer) XContentMapValues.extractValue("hits.total.value", map);
                assertThat(totalHits, equalTo(1));
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

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, (client) -> {}, Collections.emptyList());
        }
    }

}
