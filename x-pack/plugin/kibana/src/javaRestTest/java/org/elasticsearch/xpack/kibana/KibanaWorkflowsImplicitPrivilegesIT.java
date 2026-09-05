/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class KibanaWorkflowsImplicitPrivilegesIT extends ESRestTestCase {

    private static final String ADMIN_USER = "test-admin";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    private static final String BASE_USER = "wf_base_reader";
    private static final String BASE_USER_PASSWORD = "wf-base-password";

    private static final String MANAGED_USER = "wf_managed_reader";
    private static final String MANAGED_USER_PASSWORD = "wf-managed-password";

    private static final String KIBANA_APPLICATION = "kibana-.kibana";
    private static final String EXEC_READ_PRIVILEGE = "feature_wf_exec.read";
    private static final String EXEC_READ_MANAGED_PRIVILEGE = "feature_wf_exec_managed.read";
    private static final String READ_EXECUTION_ACTION = "api:workflowsManagement:readExecution";
    private static final String READ_MANAGED_EXECUTION_ACTION = "api:workflowsManagement:managed:readExecution";

    private static final String EXECUTIONS_INDEX = ".workflows-executions-test-001";
    private static final String STEP_EXECUTIONS_INDEX = ".workflows-step-executions-test-001";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("kibana-workflows-implicit-privileges-cluster")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.ml.enabled", "false")
        .user(ADMIN_USER, ADMIN_PASSWORD)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", basicAuth(ADMIN_USER, ADMIN_PASSWORD)).build();
    }

    public void testBaseRoleSeesOnlyNonManagedDocsInGrantedSpace() throws Exception {
        putPrivilege(EXEC_READ_PRIVILEGE, READ_EXECUTION_ACTION);
        putRole("wf_base_role", EXEC_READ_PRIVILEGE, "space:marketing");
        putUser(BASE_USER, BASE_USER_PASSWORD, "wf_base_role");
        createExecutionsIndexWithDocs();

        assertImplicitGrantSurfaced("wf_base_role", "marketing");
        assertUserSeesDocuments(BASE_USER, BASE_USER_PASSWORD, 2);
        assertUserSeesStepDocuments(BASE_USER, BASE_USER_PASSWORD, 1);
    }

    public void testManagedRoleSeesAllDocsInGrantedSpace() throws Exception {
        putPrivilege(EXEC_READ_PRIVILEGE, READ_EXECUTION_ACTION);
        putPrivilege(EXEC_READ_MANAGED_PRIVILEGE, READ_MANAGED_EXECUTION_ACTION);
        putRoleBothActions("wf_managed_role", "space:marketing");
        putUser(MANAGED_USER, MANAGED_USER_PASSWORD, "wf_managed_role");
        createExecutionsIndexWithDocs();

        assertUserSeesDocuments(MANAGED_USER, MANAGED_USER_PASSWORD, 3);
        assertUserSeesStepDocuments(MANAGED_USER, MANAGED_USER_PASSWORD, 3);
    }

    private void putPrivilege(String name, String action) throws Exception {
        final Request request = new Request("PUT", "/_security/privilege");
        request.setJsonEntity(Strings.format("""
            {
              "%s": {
                "%s": {
                  "actions": ["%s"]
                }
              }
            }
            """, KIBANA_APPLICATION, name, action));
        assertOK(client().performRequest(request));
    }

    private void putRole(String roleName, String privilegeName, String resource) throws Exception {
        final Request request = new Request("PUT", "/_security/role/" + roleName);
        request.setJsonEntity(Strings.format("""
            {
              "cluster": [],
              "applications": [
                {
                  "application": "%s",
                  "privileges": ["%s"],
                  "resources": ["%s"]
                }
              ]
            }
            """, KIBANA_APPLICATION, privilegeName, resource));
        assertOK(client().performRequest(request));
    }

    private void putRoleBothActions(String roleName, String resource) throws Exception {
        final Request request = new Request("PUT", "/_security/role/" + roleName);
        request.setJsonEntity(Strings.format("""
            {
              "cluster": [],
              "applications": [
                {
                  "application": "%s",
                  "privileges": ["%s", "%s"],
                  "resources": ["%s"]
                }
              ]
            }
            """, KIBANA_APPLICATION, EXEC_READ_PRIVILEGE, EXEC_READ_MANAGED_PRIVILEGE, resource));
        assertOK(client().performRequest(request));
    }

    private void putUser(String username, String password, String role) throws Exception {
        final Request request = new Request("PUT", "/_security/user/" + username);
        request.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles": ["%s"]
            }
            """, password, role));
        assertOK(client().performRequest(request));
    }

    private void createExecutionsIndexWithDocs() throws Exception {
        final Request create = new Request("PUT", "/" + EXECUTIONS_INDEX);
        create.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "spaceId":    { "type": "keyword" },
                  "managed":    { "type": "boolean" },
                  "status":     { "type": "keyword" },
                  "workflowDefinition": { "type": "object", "enabled": false },
                  "usage": {
                    "type": "object",
                    "properties": { "totalTokens": { "type": "long" } }
                  }
                }
              }
            }
            """);
        create.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(client().performRequest(create));

        final Request createSteps = new Request("PUT", "/" + STEP_EXECUTIONS_INDEX);
        createSteps.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "spaceId": { "type": "keyword" },
                  "managed": { "type": "boolean" },
                  "status":  { "type": "keyword" }
                }
              }
            }
            """);
        createSteps.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(client().performRequest(createSteps));

        indexDoc("marketing-non-managed", "marketing", false, 10L, "secret-yaml");
        indexDocNoManagedField("marketing-non-managed-fieldless", "marketing", 20L, "secret-yaml2");
        indexDoc("marketing-managed", "marketing", true, 30L, "secret-yaml3");
        indexDoc("finance-non-managed", "finance", false, 40L, "secret-yaml4");

        indexStepDoc("marketing-non-managed", "marketing", false);
        indexStepDocWithoutManaged("marketing-managed-fieldless", "marketing");
        indexStepDoc("marketing-managed", "marketing", true);
        indexStepDoc("finance-non-managed", "finance", false);
    }

    private void indexDoc(String id, String spaceId, boolean managed, long totalTokens, String yamlContent) throws Exception {
        final Request request = new Request("PUT", "/" + EXECUTIONS_INDEX + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(Strings.format("""
            {
              "spaceId": "%s",
              "managed": %s,
              "status": "completed",
              "workflowDefinition": { "yaml": "%s" },
              "usage": { "totalTokens": %d }
            }
            """, spaceId, managed, yamlContent, totalTokens));
        assertOK(client().performRequest(request));
    }

    private void indexDocNoManagedField(String id, String spaceId, long totalTokens, String yamlContent) throws Exception {
        final Request request = new Request("PUT", "/" + EXECUTIONS_INDEX + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(Strings.format("""
            {
              "spaceId": "%s",
              "status": "completed",
              "workflowDefinition": { "yaml": "%s" },
              "usage": { "totalTokens": %d }
            }
            """, spaceId, yamlContent, totalTokens));
        assertOK(client().performRequest(request));
    }

    private void indexStepDoc(String id, String spaceId, boolean managed) throws Exception {
        final Request request = new Request("PUT", "/" + STEP_EXECUTIONS_INDEX + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(Strings.format("""
            {
              "spaceId": "%s",
              "managed": %s,
              "status": "completed"
            }
            """, spaceId, managed));
        assertOK(client().performRequest(request));
    }

    private void indexStepDocWithoutManaged(String id, String spaceId) throws Exception {
        final Request request = new Request("PUT", "/" + STEP_EXECUTIONS_INDEX + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(Strings.format("""
            {
              "spaceId": "%s",
              "status": "completed"
            }
            """, spaceId));
        assertOK(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private void assertImplicitGrantSurfaced(String roleName, String expectedSpace) throws Exception {
        final Request request = new Request("GET", "/_security/role/" + roleName);
        request.addParameter("include_implicit", "true");
        final Response response = client().performRequest(request);
        assertOK(response);

        final Map<String, Object> body = entityAsMap(response);
        final Map<String, Object> role = (Map<String, Object>) body.get(roleName);
        final List<Map<String, Object>> indices = (List<Map<String, Object>>) role.get("indices");

        final List<Map<String, Object>> implicitEntries = indices.stream()
            .filter(entry -> Boolean.TRUE.equals(entry.get("implicitly_granted")))
            .filter(entry -> ((List<String>) entry.get("names")).contains(".workflows-executions*"))
            .toList();
        assertThat("expected one implicit .workflows-executions* grant, got " + indices, implicitEntries, hasSize(1));

        final String query = (String) implicitEntries.get(0).get("query");
        assertThat(query, containsString("spaceId"));
        assertThat(query, containsString(expectedSpace));
        assertThat(query, containsString("must_not"));
        assertThat(query, containsString("\"managed\":true"));
    }

    @SuppressWarnings("unchecked")
    private void assertUserSeesDocuments(String username, String password, int expectedCount) throws Exception {
        final Request search = new Request("GET", "/" + EXECUTIONS_INDEX + "/_search");
        search.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuth(username, password)));
        final Response response = client().performRequest(search);
        assertOK(response);

        final Map<String, Object> body = entityAsMap(response);
        final Map<String, Object> hits = (Map<String, Object>) body.get("hits");
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        assertThat("unexpected doc count for " + username, hitList, hasSize(expectedCount));
        assertEsqlSeesSameDocuments(username, password, EXECUTIONS_INDEX, hitList);

        final Map<String, Object> source = (Map<String, Object>) hitList.get(0).get("_source");
        assertNotNull("usage must be present (FLS object-pattern check)", source.get("usage"));
        @SuppressWarnings("unchecked")
        final Map<String, Object> usage = (Map<String, Object>) source.get("usage");
        assertNotNull("usage.totalTokens must be present", usage.get("totalTokens"));
        assertNull("workflowDefinition must be absent (FLS excluded)", source.get("workflowDefinition"));
    }

    @SuppressWarnings("unchecked")
    private void assertUserSeesStepDocuments(String username, String password, int expectedCount) throws Exception {
        final Request search = new Request("GET", "/" + STEP_EXECUTIONS_INDEX + "/_search");
        search.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuth(username, password)));
        final Response response = client().performRequest(search);
        assertOK(response);
        final Map<String, Object> body = entityAsMap(response);
        final Map<String, Object> hits = (Map<String, Object>) body.get("hits");
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        assertThat(hitList, hasSize(expectedCount));
        assertEsqlSeesSameDocuments(username, password, STEP_EXECUTIONS_INDEX, hitList);
    }

    @SuppressWarnings("unchecked")
    private void assertEsqlSeesSameDocuments(String username, String password, String index, List<Map<String, Object>> searchHits)
        throws Exception {
        final Request request = new Request("POST", "/_query");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuth(username, password)));
        request.setJsonEntity(Strings.format("""
            {
              "query": "FROM %s METADATA _id | KEEP _id, spaceId, managed, status | LIMIT 100"
            }
            """, index));
        final Response response = client().performRequest(request);
        assertOK(response);

        final List<List<Object>> rows = (List<List<Object>>) entityAsMap(response).get("values");
        final List<String> searchIds = searchHits.stream().map(hit -> (String) hit.get("_id")).sorted().toList();
        final List<String> esqlIds = rows.stream().map(row -> (String) row.get(0)).sorted().toList();
        assertThat("ES|QL and _search must return the same documents", esqlIds, equalTo(searchIds));
    }

    private static String basicAuth(String username, String password) {
        final String token = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}
