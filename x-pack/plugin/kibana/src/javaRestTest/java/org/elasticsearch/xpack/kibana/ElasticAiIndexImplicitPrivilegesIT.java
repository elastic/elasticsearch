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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end coverage for {@code ElasticAiIndexImplicitPrivilegesProvider} against a real
 * default-distribution node. Unlike an in-JVM {@code internalClusterTest}, this exercises the
 * full production path: the plugin is bundled into the default distribution and auto-discovered
 * via the {@code SecurityExtension} SPI, so no test plugin is installed.
 * <p>
 * The happy path verifies that a role holding {@code ai_index:<kiType>/read} Kibana application privileges,
 * granted in different spaces (with no explicit index privileges), can read the Elastic AI Index
 * {@code ai-index-idx-sml-data}, and that the implicit document-level-security filter restricts results
 * to the following rule: a document is visible only when the user holds <em>all</em> the
 * actions it requires <em>within a single space</em>. Actions accumulated across different spaces
 * must not grant access, and a document scoped to every space via {@code "*"} must still be visible
 * to a space-scoped user.
 */
public class ElasticAiIndexImplicitPrivilegesIT extends ESRestTestCase {

    private static final String ADMIN_USER = "test-admin";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    private static final String SML_USER = "kibana_sml_user";
    private static final String SML_USER_PASSWORD = "kibana-sml-password";
    private static final String AI_INDEX_READER_ROLE = "ai_marketing_reader";

    private static final String KIBANA_APPLICATION = "kibana-.kibana";
    private static final String DASHBOARDS_PRIVILEGE = "feature_dashboards.read";
    private static final String WORKFLOWS_PRIVILEGE = "feature_workflows.read";
    private static final String LOGIN_ACTION = "login:";
    private static final String ELASTIC_AI_INDEX_DASHBOARD_READ_ACTION = "ai_index:dashboard/read";
    private static final String ELASTIC_AI_INDEX_WORKFLOW_READ_ACTION = "ai_index:workflow/read";
    // Registered alongside the ai_index: action to prove non-ai_index: actions are filtered out of the DLS query.
    private static final String SAVED_OBJECT_GET_ACTION = "saved_object:dashboard/get";
    private static final String ELASTIC_AI_INDEX = "ai-index-idx-sml-data";

    // The SML storage adapter creates a CONCRETE index "<name>-000001" and fronts it with an ALIAS
    // named exactly ELASTIC_AI_INDEX. So "ai-index-idx-sml-data" is never a concrete index in production.
    private static final String ELASTIC_AI_INDEX_BACKING = ELASTIC_AI_INDEX + "-000001";

    // Shared between the _search and ES|QL assertions: both engines must resolve each role's DLS
    // filter to exactly these sets.
    private static final List<String> SPACE_SCOPED_VISIBLE_DOC_IDS = List.of(
        "all-spaces-dashboard",
        "global-no-perms",
        "marketing-dashboard",
        "mixed-counts",
        "shared-dashboard"
    );
    // The wildcard-resource grant reaches finance-dashboard (no space restriction), but it holds
    // only dashboard/read, so mixed-counts (satisfiable only with workflow/read) drops out.
    private static final List<String> WILDCARD_GRANT_VISIBLE_DOC_IDS = List.of(
        "all-spaces-dashboard",
        "finance-dashboard",
        "global-no-perms",
        "marketing-dashboard",
        "shared-dashboard"
    );

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("kibana-elastic-ai-index-implicit-privileges-cluster")
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

    public void testSpaceAndPrivilegeScopedRoleImplicitlyReadsAiIndexDataWithDls() throws Exception {
        // 1. Register the Kibana application privileges.
        putKibanaPrivileges();

        // 2. A role holding ONLY those application privileges, scoped to two different spaces with
        // different actions in each — no explicit index privileges.
        putAiIndexReaderRole(AI_INDEX_READER_ROLE);

        // 3. A user that holds the role.
        putUser(SML_USER, SML_USER_PASSWORD, AI_INDEX_READER_ROLE);

        // 4. As admin, create the Elastic AI Index with explicit nested mappings and index the fixtures.
        createAiIndexWithDocs();

        // 5. The implicit grant surfaces through the get-role API, carrying the nested DLS query.
        assertImplicitGrantSurfaced(AI_INDEX_READER_ROLE);

        // 6. The user can read the Elastic AI Index without any explicit index privilege, and DLS restricts the
        // visible documents to exactly those that satisfy a whole nested element — through both
        // the _search path and ES|QL, which executes on its own engine.
        assertUserSeesOnlyAuthorizedDocs(SPACE_SCOPED_VISIBLE_DOC_IDS);
    }

    /**
     * A role granted on {@code resources: ["*"]} reads documents in every space — including spaces
     * the user holds no explicit grant in — but the wildcard is not a bypass: only documents
     * satisfiable with the actions actually held become visible.
     */
    public void testWildcardResourceRoleImplicitlyReadsAllSpacesWithDls() throws Exception {
        putKibanaPrivileges();
        putAllSpacesReaderRole("ai_all_spaces_reader");
        putUser(SML_USER, SML_USER_PASSWORD, "ai_all_spaces_reader");
        createAiIndexWithDocs();

        assertUserSeesOnlyAuthorizedDocs(WILDCARD_GRANT_VISIBLE_DOC_IDS);
    }

    /**
     * The DLS filter deliberately leaves the nested query's {@code ignore_unmapped} at {@code false}:
     * when the permissions field is not mapped as {@code nested} (here: left to dynamic mapping,
     * which produces {@code object}), searches must fail loudly rather than silently hiding or
     * exposing documents.
     */
    public void testNonNestedPermissionsMappingFailsSearchLoudly() throws Exception {
        putKibanaPrivileges();
        putAiIndexReaderRole(AI_INDEX_READER_ROLE);
        putUser(SML_USER, SML_USER_PASSWORD, AI_INDEX_READER_ROLE);

        final Request create = new Request("PUT", "/" + ELASTIC_AI_INDEX_BACKING);
        create.setJsonEntity(Strings.format("""
            { "aliases": { "%s": { "is_write_index": true } } }
            """, ELASTIC_AI_INDEX));
        assertOK(client().performRequest(create));
        indexDoc("marketing-dashboard", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:dashboard/read"], "count": 1 }
              ]}}
            }
            """);

        final Request search = new Request("GET", "/" + ELASTIC_AI_INDEX + "/_search");
        search.setOptions(getRequestOptions());
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(search));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("failed to find nested object under path"));
    }

    private void putKibanaPrivileges() throws Exception {
        final Request request = new Request("PUT", "/_security/privilege");
        request.setJsonEntity(
            Strings.format(
                """
                    {
                      "%s": {
                        "%s": {
                          "actions": ["%s", "%s", "%s"]
                        },
                        "%s": {
                          "actions": ["%s", "%s"]
                        }
                      }
                    }
                    """,
                KIBANA_APPLICATION,
                DASHBOARDS_PRIVILEGE,
                LOGIN_ACTION,
                SAVED_OBJECT_GET_ACTION,
                ELASTIC_AI_INDEX_DASHBOARD_READ_ACTION,
                WORKFLOWS_PRIVILEGE,
                LOGIN_ACTION,
                ELASTIC_AI_INDEX_WORKFLOW_READ_ACTION
            )
        );
        assertOK(client().performRequest(request));
    }

    /**
     * Grants dashboard/read in marketing and workflow/read in finance. The actions must differ per
     * space, otherwise the cross-space-leak fixture would be satisfied by either element and the
     * permission leak this test guards would be invisible.
     */
    private void putAiIndexReaderRole(String roleName) throws Exception {
        final Request request = new Request("PUT", "/_security/role/" + roleName);
        request.setJsonEntity(Strings.format("""
            {
              "cluster": [],
              "applications": [
                {
                  "application": "%s",
                  "privileges": ["%s"],
                  "resources": ["space:marketing"]
                },
                {
                  "application": "%s",
                  "privileges": ["%s"],
                  "resources": ["space:finance"]
                }
              ]
            }
            """, KIBANA_APPLICATION, DASHBOARDS_PRIVILEGE, KIBANA_APPLICATION, WORKFLOWS_PRIVILEGE));
        assertOK(client().performRequest(request));
    }

    /** Grants dashboard/read in every space via the wildcard resource. */
    private void putAllSpacesReaderRole(String roleName) throws Exception {
        final Request request = new Request("PUT", "/_security/role/" + roleName);
        request.setJsonEntity(Strings.format("""
            {
              "cluster": [],
              "applications": [
                {
                  "application": "%s",
                  "privileges": ["%s"],
                  "resources": ["*"]
                }
              ]
            }
            """, KIBANA_APPLICATION, DASHBOARDS_PRIVILEGE));
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

    private void createAiIndexWithDocs() throws Exception {
        // Explicit mappings: the ai-index-* template deliberately does NOT carry the permissions
        // shape, so this test owns the mapping the provider is written against. Dynamic mapping
        // cannot substitute — it never produces `nested` for an array of objects, and a nested
        // query against an `object` field throws rather than under-matching.
        //
        // The mapping lives on the CONCRETE index; the grant and every request name the ALIAS. That
        // split is deliberate: it is the production arrangement, and it is what proves the nested
        // DLS filter is applied to the backing index when the request is authorized via the alias.
        final Request create = new Request("PUT", "/" + ELASTIC_AI_INDEX_BACKING);
        create.setJsonEntity(Strings.format("""
            {
              "aliases": { "%s": { "is_write_index": true } },
              "mappings": {
                "properties": {
                  "type": { "type": "keyword" },
                  "permissions": {
                    "type": "object",
                    "properties": {
                      "kibana": {
                        "type": "object",
                        "properties": {
                          "privileges": {
                            "type": "nested",
                            "properties": {
                              "name":  { "type": "keyword" },
                              "space": { "type": "keyword" },
                              "count": { "type": "long" }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """, ELASTIC_AI_INDEX));
        assertOK(client().performRequest(create));

        // Documents deliberately carry no title/description/content: the template maps a semantic_text
        // sub-field on each of those, and populating one would require an inference-capable license.
        // The assertions run off document ids only. The VISIBLE/HIDDEN notes below describe the
        // space-scoped role; the wildcard-resource test pins its own set in WILDCARD_GRANT_VISIBLE_DOC_IDS.

        // VISIBLE: user holds ai_index:dashboard/read in marketing.
        indexDoc("marketing-dashboard", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:dashboard/read"], "count": 1 }
              ]}}
            }
            """);

        // HIDDEN: right action, wrong space — user holds dashboard/read in marketing, not finance.
        indexDoc("finance-dashboard", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "finance", "name": ["ai_index:dashboard/read"], "count": 1 }
              ]}}
            }
            """);

        // HIDDEN: right space, wrong action — user holds workflow/read in finance, not marketing.
        indexDoc("marketing-workflow", """
            {
              "type": "workflow",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:workflow/read"], "count": 1 }
              ]}}
            }
            """);

        // VISIBLE: shared into two spaces; the user satisfies the marketing element.
        // Proves nested matching is existential — OR across spaces.
        indexDoc("shared-dashboard", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:dashboard/read"], "count": 1 },
                { "space": "engineering", "name": ["ai_index:dashboard/read"], "count": 1 }
              ]}}
            }
            """);

        // HIDDEN: Requires BOTH actions in marketing AND both in finance. The user holds
        // dashboard/read in marketing and workflow/read in finance — one of two in each.
        indexDoc("cross-space-leak", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:dashboard/read", "ai_index:workflow/read"], "count": 2 },
                { "space": "finance",   "name": ["ai_index:dashboard/read", "ai_index:workflow/read"], "count": 2 }
              ]}}
            }
            """);

        // VISIBLE: proves `count` is read from the MATCHING entry, not from the first entry or from
        // some document-wide value.
        indexDoc("mixed-counts", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:dashboard/read", "ai_index:workflow/read"], "count": 2 },
                { "space": "finance",   "name": ["ai_index:workflow/read"], "count": 1 }
              ]}}
            }
            """);

        // VISIBLE: scoped to every space via the "*" marker, and the user holds the action it
        // requires (in marketing). An all-spaces document lives in marketing too, so a
        // marketing-scoped user must see it — this is what the "*" arm of the space match buys.
        indexDoc("all-spaces-dashboard", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "*", "name": ["ai_index:dashboard/read"], "count": 1 }
              ]}}
            }
            """);

        // HIDDEN: all-spaces, but requires an action the user holds in no space at all.
        // Proves the "*" space arm widens which elements are eligible, never which actions are held.
        indexDoc("all-spaces-connector", """
            {
              "type": "connector",
              "permissions": { "kibana": { "privileges": [
                { "space": "*", "name": ["ai_index:connector/read"], "count": 1 }
              ]}}
            }
            """);

        // VISIBLE: no permissions block → public document.
        indexDoc("global-no-perms", """
            {
              "type": "dashboard"
            }
            """);

        assertOK(client().performRequest(new Request("POST", "/" + ELASTIC_AI_INDEX + "/_refresh")));
    }

    /** Writes through the alias, as the SML storage adapter does (its bulk sets require_alias). */
    private void indexDoc(String id, String body) throws Exception {
        final Request request = new Request("PUT", "/" + ELASTIC_AI_INDEX + "/_doc/" + id);
        request.setJsonEntity(body);
        assertOK(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private void assertImplicitGrantSurfaced(String roleName) throws Exception {
        final Request request = new Request("GET", "/_security/role/" + roleName);
        request.addParameter("include_implicit", "true");
        final Response response = client().performRequest(request);
        assertOK(response);

        final Map<String, Object> body = entityAsMap(response);
        final Map<String, Object> role = (Map<String, Object>) body.get(roleName);
        final List<Map<String, Object>> indices = (List<Map<String, Object>>) role.get("indices");

        final List<Map<String, Object>> implicitEntries = indices.stream()
            .filter(entry -> Boolean.TRUE.equals(entry.get("implicitly_granted")))
            .filter(entry -> ((List<String>) entry.get("names")).stream().anyMatch(n -> n.startsWith("ai-index-")))
            .toList();
        assertThat("expected exactly one implicit Elastic AI Index grant, got " + indices, implicitEntries, hasSize(1));

        final Map<String, Object> implicit = implicitEntries.get(0);
        assertThat((List<String>) implicit.get("privileges"), equalTo(List.of("read")));

        final String query = (String) implicit.get("query");
        assertThat(query, containsString("\"nested\""));
        assertThat(query, containsString("permissions.kibana.privileges.space"));
        assertThat(query, containsString("permissions.kibana.privileges.name"));
        assertThat(query, containsString("permissions.kibana.privileges.count"));
        assertThat(query, containsString(ELASTIC_AI_INDEX_DASHBOARD_READ_ACTION));
        assertThat(query, containsString(ELASTIC_AI_INDEX_WORKFLOW_READ_ACTION));
        assertThat(query, containsString("terms_set"));
        // No delimiter anywhere — space and action are separate fields now.
        assertThat(query, not(containsString("|")));
        // Only ai_index: actions become DLS terms — login:/saved_object: in the same grant are dropped.
        assertThat(query, not(containsString(LOGIN_ACTION)));
        assertThat(query, not(containsString(SAVED_OBJECT_GET_ACTION)));
    }

    /**
     * Asserts the DLS-visible set through both query engines — _search endpoint and ES|QL
     * Pinning the identical positive set catches DLS regressions where the two engines drift apart.
     */
    private void assertUserSeesOnlyAuthorizedDocs(List<String> expectedIds) throws Exception {
        final Request searchRequest = new Request("GET", "/" + ELASTIC_AI_INDEX + "/_search");
        searchRequest.setOptions(getRequestOptions());

        final Request esqlRequest = new Request("POST", "/_query");
        esqlRequest.setOptions(getRequestOptions());
        // The explicit LIMIT avoids the "no limit defined" warning header, which the test REST client treats as a failure.
        esqlRequest.setJsonEntity(Strings.format("{ \"query\": \"FROM %s METADATA _id | KEEP _id | LIMIT 100\" }", ELASTIC_AI_INDEX));

        final Response searchResponse = client().performRequest(searchRequest);
        final Response esqlResponse = client().performRequest(esqlRequest);

        // Assert: both engines must resolve the DLS filter to the same visible set.
        assertOK(searchResponse);
        final List<Map<String, Object>> searchHits = ObjectPath.createFromResponse(searchResponse).evaluate("hits.hits");
        assertVisibleIds("_search", searchHits.stream().map(hit -> (String) hit.get("_id")).toList(), expectedIds);

        assertOK(esqlResponse);
        final List<List<Object>> esqlRows = ObjectPath.createFromResponse(esqlResponse).evaluate("values");
        assertVisibleIds("ES|QL", esqlRows.stream().map(row -> (String) row.get(0)).toList(), expectedIds);
    }

    private static void assertVisibleIds(String engine, List<String> ids, List<String> expectedIds) {
        final List<String> visibleIds = ids.stream().sorted().toList();
        assertThat("unexpected number of visible docs via " + engine, visibleIds, hasSize(expectedIds.size()));
        assertThat("via " + engine, visibleIds, equalTo(expectedIds));
    }

    private static RequestOptions getRequestOptions() {
        return RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuth(SML_USER, SML_USER_PASSWORD)).build();
    }

    private static String basicAuth(String username, String password) {
        final String token = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}
