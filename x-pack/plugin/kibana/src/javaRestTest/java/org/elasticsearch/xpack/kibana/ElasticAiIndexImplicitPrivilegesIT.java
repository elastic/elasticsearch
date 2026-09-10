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
import org.elasticsearch.client.WarningsHandler;
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

import static org.hamcrest.Matchers.containsInAnyOrder;
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
 * {@code .ai-index-idx-sml-data}, and that the implicit document-level-security filter restricts results
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
    private static final String ELASTIC_AI_INDEX = ".ai-index-idx-sml-data";
    // Installed by the stack plugin's AiIndexTemplateRegistry; match every `.ai-index-idx-*` / `.ai-index-ds-*` name.
    private static final String AI_INDEX_MANAGED_TEMPLATE = "ai-index-idx-managed";
    private static final String AI_INDEX_DS_MANAGED_TEMPLATE = "ai-index-ds-managed";

    // Shared between the _search and ES|QL assertions: both engines must resolve each role's DLS
    // filter to exactly these sets.
    private static final List<String> SPACE_SCOPED_VISIBLE_DOC_IDS = List.of(
        "all-spaces-dashboard",
        "all-spaces-public",
        "global-no-perms",
        "marketing-dashboard",
        "marketing-public",
        "mixed-counts",
        "shared-dashboard"
    );
    // The wildcard-resource grant reaches finance-dashboard and engineering-public, but holds only
    // dashboard/read, so mixed-counts (needs workflow/read) drops out.
    private static final List<String> WILDCARD_GRANT_VISIBLE_DOC_IDS = List.of(
        "all-spaces-dashboard",
        "all-spaces-public",
        "engineering-public",
        "finance-dashboard",
        "global-no-perms",
        "marketing-dashboard",
        "marketing-public",
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

        // 4. As admin, create the Elastic AI Index (mapped by the built-in template) and index the fixtures.
        createAiIndexWithDocs();

        // 5. The implicit grant surfaces through the get-role API, carrying the nested DLS query.
        assertImplicitGrantSurfaced(AI_INDEX_READER_ROLE);

        // 6. The user can read the Elastic AI Index without any explicit index privilege, and DLS restricts the
        // visible documents to exactly those that satisfy a whole nested element — through both
        // the _search path and ES|QL, which executes on its own engine.
        assertUserSeesOnlyAuthorizedDocs(ELASTIC_AI_INDEX, SPACE_SCOPED_VISIBLE_DOC_IDS);
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

        assertUserSeesOnlyAuthorizedDocs(ELASTIC_AI_INDEX, WILDCARD_GRANT_VISIBLE_DOC_IDS);
    }

    /**
     * The grant is over the {@code .ai-index-idx-*} and {@code .ai-index-ds-*} patterns rather than one
     * concrete index, so any other Elastic-managed AI index or data stream is readable under the same DLS filter.
     */
    public void testGrantCoversEveryElasticManagedAiIndex() throws Exception {
        putKibanaPrivileges();
        putAiIndexReaderRole(AI_INDEX_READER_ROLE);
        putUser(SML_USER, SML_USER_PASSWORD, AI_INDEX_READER_ROLE);

        final String otherIndex = ".ai-index-idx-other";
        createAiIndex(otherIndex);
        indexDoc(otherIndex, "other-marketing-dashboard", dashboardIn("marketing"));
        indexDoc(otherIndex, "other-finance-dashboard", dashboardIn("finance"));
        assertOK(client().performRequest(new Request("POST", "/" + otherIndex + "/_refresh")));
        assertUserSeesOnlyAuthorizedDocs(otherIndex, List.of("other-marketing-dashboard"));

        final String otherDataStream = ".ai-index-ds-other";
        createAiDataStream(otherDataStream);
        indexDoc(otherDataStream, "ds-marketing-dashboard", dashboardIn("marketing"));
        indexDoc(otherDataStream, "ds-finance-dashboard", dashboardIn("finance"));
        assertOK(client().performRequest(new Request("POST", "/" + otherDataStream + "/_refresh")));
        assertUserSeesOnlyAuthorizedDocs(otherDataStream, List.of("ds-marketing-dashboard"));
    }

    /** A dashboard requiring {@code ai_index:dashboard/read} in {@code space}; stamped so it is valid for a data stream too. */
    private static String dashboardIn(String space) {
        return Strings.format("""
            {
              "@timestamp": "2026-01-01T00:00:00Z",
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "%s", "name": ["ai_index:dashboard/read"], "count": 1 }
              ]}}
            }
            """, space);
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

    /**
     * Creates {@code index} bare, as Kibana does, so every mapping on it comes from the
     * {@code ai-index-idx-managed} template.
     */
    private void createAiIndex(String index) throws Exception {
        waitForTemplate(AI_INDEX_MANAGED_TEMPLATE);

        final Request create = new Request("PUT", "/" + index);
        // Creating a dot-prefixed index emits a deprecation warning that is irrelevant to this test.
        create.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(client().performRequest(create));

        // Fail fast if the template stopped matching
        final Map<String, Object> mapping = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_mapping")));
        assertThat(
            "expected [" + AI_INDEX_MANAGED_TEMPLATE + "] to map permissions.kibana.privileges as nested",
            new ObjectPath(mapping.get(index)).evaluate("mappings.properties.permissions.properties.kibana.properties.privileges.type"),
            equalTo("nested")
        );
    }

    /** Creates the data stream {@code name} bare; its backing index is mapped by the {@code ai-index-ds-managed} template. */
    private void createAiDataStream(String name) throws Exception {
        waitForTemplate(AI_INDEX_DS_MANAGED_TEMPLATE);
        assertOK(client().performRequest(new Request("PUT", "/_data_stream/" + name)));

        final Map<String, Object> dataStream = entityAsMap(client().performRequest(new Request("GET", "/_data_stream/" + name)));
        assertThat(new ObjectPath(dataStream).evaluate("data_streams.0.template"), equalTo(AI_INDEX_DS_MANAGED_TEMPLATE));
    }

    /**
     * The registry installs its templates asynchronously after startup, so wait: without the template,
     * dynamic mapping never produces `nested`.
     */
    private void waitForTemplate(String template) throws Exception {
        assertBusy(() -> {
            try {
                assertOK(client().performRequest(new Request("GET", "/_index_template/" + template)));
            } catch (ResponseException e) {
                fail(e.getMessage());
            }
        });
    }

    private void createAiIndexWithDocs() throws Exception {
        createAiIndex(ELASTIC_AI_INDEX);

        // Documents deliberately carry no title/description/content: the template maps a semantic_text
        // sub-field on each of those, and populating one would require an inference-capable license.
        // The assertions run off document ids only. The VISIBLE/HIDDEN notes below describe the
        // space-scoped role; the wildcard-resource test pins its own set in WILDCARD_GRANT_VISIBLE_DOC_IDS.

        // VISIBLE: user holds ai_index:dashboard/read in marketing.
        indexDoc(ELASTIC_AI_INDEX, "marketing-dashboard", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:dashboard/read"], "count": 1 }
              ]}}
            }
            """);

        // HIDDEN: right action, wrong space — user holds dashboard/read in marketing, not finance.
        indexDoc(ELASTIC_AI_INDEX, "finance-dashboard", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "finance", "name": ["ai_index:dashboard/read"], "count": 1 }
              ]}}
            }
            """);

        // HIDDEN: right space, wrong action — user holds workflow/read in finance, not marketing.
        indexDoc(ELASTIC_AI_INDEX, "marketing-workflow", """
            {
              "type": "workflow",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:workflow/read"], "count": 1 }
              ]}}
            }
            """);

        // VISIBLE: shared into two spaces; the user satisfies the marketing element.
        // Proves nested matching is existential — OR across spaces.
        indexDoc(ELASTIC_AI_INDEX, "shared-dashboard", """
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
        indexDoc(ELASTIC_AI_INDEX, "cross-space-leak", """
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
        indexDoc(ELASTIC_AI_INDEX, "mixed-counts", """
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
        indexDoc(ELASTIC_AI_INDEX, "all-spaces-dashboard", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "*", "name": ["ai_index:dashboard/read"], "count": 1 }
              ]}}
            }
            """);

        // HIDDEN: all-spaces, but requires an action the user holds in no space at all.
        // Proves the "*" space arm widens which elements are eligible, never which actions are held.
        indexDoc(ELASTIC_AI_INDEX, "all-spaces-connector", """
            {
              "type": "connector",
              "permissions": { "kibana": { "privileges": [
                { "space": "*", "name": ["ai_index:connector/read"], "count": 1 }
              ]}}
            }
            """);

        // VISIBLE: requires no action, in marketing where the user is. This is what Kibana writes for
        // an SML type that opts out of gating: empty `name`, `count: 0`.
        indexDoc(ELASTIC_AI_INDEX, "marketing-public", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": [], "count": 0 }
              ]}}
            }
            """);

        // HIDDEN: requires no action, but in a space the user is not in (public only within its space).
        indexDoc(ELASTIC_AI_INDEX, "engineering-public", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "engineering", "name": [], "count": 0 }
              ]}}
            }
            """);

        // VISIBLE: requires no action, in every space — genuinely public to any granted user.
        indexDoc(ELASTIC_AI_INDEX, "all-spaces-public", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "*", "name": [], "count": 0 }
              ]}}
            }
            """);

        // HIDDEN: malformed — `count: 0` while still naming an action. Only a buggy or hostile producer
        // writes this, so it must fail closed. No test role holds the named action, so `terms_set` never
        // visits it — isolating the count:0 escape (arm one) from the terms_set arm.
        indexDoc(ELASTIC_AI_INDEX, "malformed-zero-count", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:workflow/read"], "count": 0 }
              ]}}
            }
            """);

        // Malformed count:0 must remain hidden even when the named action is held.
        indexDoc(ELASTIC_AI_INDEX, "malformed-zero-count-held-action", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:dashboard/read"], "count": 0 }
              ]}}
            }
            """);

        // VISIBLE: no permissions block → public, via the must_not(nested(match_all)) branch. Kibana
        // never writes this, but the branch must work for any other producer.
        indexDoc(ELASTIC_AI_INDEX, "global-no-perms", """
            {
              "type": "dashboard"
            }
            """);

        assertOK(client().performRequest(new Request("POST", "/" + ELASTIC_AI_INDEX + "/_refresh")));
    }

    private void indexDoc(String index, String id, String body) throws Exception {
        // _create rather than _doc: data streams only accept create operations, and plain indices accept both.
        final Request request = new Request("PUT", "/" + index + "/_create/" + id);
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
            .filter(entry -> ((List<String>) entry.get("names")).stream().anyMatch(n -> n.startsWith(".ai-index-")))
            .toList();
        assertThat("expected exactly one implicit Elastic AI Index grant, got " + indices, implicitEntries, hasSize(1));

        final Map<String, Object> implicit = implicitEntries.get(0);
        assertThat((List<String>) implicit.get("names"), containsInAnyOrder(".ai-index-idx-*", ".ai-index-ds-*"));
        assertThat((List<String>) implicit.get("privileges"), equalTo(List.of("read")));

        final String query = (String) implicit.get("query");
        assertThat(query, containsString("\"nested\""));
        assertThat(query, containsString("permissions.kibana.privileges.space"));
        assertThat(query, containsString("permissions.kibana.privileges.name"));
        assertThat(query, containsString("permissions.kibana.privileges.count"));
        assertThat(query, containsString(ELASTIC_AI_INDEX_DASHBOARD_READ_ACTION));
        assertThat(query, containsString(ELASTIC_AI_INDEX_WORKFLOW_READ_ACTION));
        assertThat(query, containsString("terms_set"));
        assertThat(query, containsString("\"permissions.kibana.privileges.count\":{\"value\":0}"));
        // The zero-requirement escape is gated on the element carrying no action name.
        assertThat(query, containsString("\"must_not\":[{\"exists\":{\"field\":\"permissions.kibana.privileges.name\""));
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
    private void assertUserSeesOnlyAuthorizedDocs(String index, List<String> expectedIds) throws Exception {
        final Request searchRequest = new Request("GET", "/" + index + "/_search");
        searchRequest.setOptions(getRequestOptions());

        final Request esqlRequest = new Request("POST", "/_query");
        esqlRequest.setOptions(getRequestOptions());
        // The explicit LIMIT avoids the "no limit defined" warning header, which the test REST client treats as a failure.
        esqlRequest.setJsonEntity(Strings.format("{ \"query\": \"FROM %s METADATA _id | KEEP _id | LIMIT 100\" }", index));

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
