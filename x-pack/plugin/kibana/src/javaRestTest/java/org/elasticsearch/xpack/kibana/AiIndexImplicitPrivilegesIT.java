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
import static org.hamcrest.Matchers.not;

/**
 * End-to-end coverage for {@code AiIndexImplicitPrivilegesProvider} against a real
 * default-distribution node. Unlike an in-JVM {@code internalClusterTest}, this exercises the
 * full production path: the plugin is bundled into the default distribution and auto-discovered
 * via the {@code SecurityExtension} SPI, so no test plugin is installed.
 * <p>
 * This is also the only place the nested permissions mapping is exercised against real Lucene. The
 * unit tests assert on query <em>serialisation</em>; only this test proves that {@code terms_set}
 * with {@code minimum_should_match_field} resolves {@code count} from the matching nested child
 * document rather than from the root. If that assumption were wrong the search would return zero
 * hits, which is why the visibility assertion pins an exact positive set — <b>a zero-hit result is a
 * failure signal, not a pass.</b>
 * <p>
 * <b>The index declares its own mappings.</b> The {@code ai-index-*} index template deliberately does
 * not carry the permissions shape, so this test owns the mapping the provider is written against. Do
 * not "simplify" by deleting the explicit {@code mappings} block and relying on the template: dynamic
 * mapping never produces {@code nested} for an array of objects, and a {@code nested} query against an
 * {@code object} field throws rather than under-matching.
 * <p>
 * <b>The user's grant.</b> {@code ai_index:dashboard/read} in {@code space:marketing} and
 * {@code ai_index:workflow/read} in {@code space:finance} — deliberately <em>different</em> actions in
 * two spaces, which is what makes the cross-space leak case below testable at all. The fixtures cover:
 * <ul>
 *   <li>{@code marketing-dashboard} — visible; the user holds exactly what the marketing element
 *       requires.</li>
 *   <li>{@code finance-dashboard} — hidden; right action, wrong space.</li>
 *   <li>{@code marketing-workflow} — hidden; right space, wrong action.</li>
 *   <li>{@code shared-dashboard} — visible; shared into two spaces and the user satisfies one of them.
 *       Proves nested matching is existential, i.e. OR across spaces.</li>
 *   <li>{@code cross-space-leak} — hidden. <b>The regression case this design exists for.</b> The
 *       document requires both actions in marketing <em>and</em> both in finance; the user holds one of
 *       two in each. Under the previous flat composite-token design the user's two tokens hit the flat
 *       count of 2 and {@code terms_set} could not tell they came from different spaces, so the
 *       document was visible. Under {@code nested} each child is scored alone: marketing scores 1 &lt; 2,
 *       finance scores 1 &lt; 2, no child matches, root hidden.</li>
 *   <li>{@code global-no-perms} — visible; a document with no permissions block is public.</li>
 * </ul>
 * <p>
 * The registered privileges deliberately bundle {@code login:} and {@code saved_object:dashboard/get}
 * alongside the {@code ai_index:} actions, so the surfaced DLS query also demonstrates that actions
 * outside the {@code ai_index:} namespace never become DLS terms.
 */
public class AiIndexImplicitPrivilegesIT extends ESRestTestCase {

    private static final String ADMIN_USER = "test-admin";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    private static final String SML_USER = "kibana_sml_user";
    private static final String SML_USER_PASSWORD = "kibana-sml-password";

    private static final String KIBANA_APPLICATION = "kibana-.kibana";
    private static final String DASHBOARDS_PRIVILEGE = "feature_dashboards.read";
    private static final String WORKFLOWS_PRIVILEGE = "feature_workflows.read";
    private static final String LOGIN_ACTION = "login:";
    private static final String AI_INDEX_DASHBOARD_READ_ACTION = "ai_index:dashboard/read";
    private static final String AI_INDEX_WORKFLOW_READ_ACTION = "ai_index:workflow/read";
    // Registered alongside the ai_index: action to prove non-ai_index: actions are filtered out of the DLS query.
    private static final String SAVED_OBJECT_GET_ACTION = "saved_object:dashboard/get";

    // Matches the ai-index-idx-* pattern so the provider's AI_INDEX_INDICES grant applies to it.
    private static final String AI_INDEX = "ai-index-idx-test";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("kibana-ai-index-implicit-privileges-cluster")
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
        putAiIndexReaderRole("ai_marketing_reader");

        // 3. A user that holds the role.
        putUser(SML_USER, SML_USER_PASSWORD, "ai_marketing_reader");

        // 4. As admin, create the AI Index with explicit nested mappings and index the fixtures.
        createAiIndexWithDocs();

        // 5. The implicit grant surfaces through the get-role API, carrying the nested DLS query.
        assertImplicitGrantSurfaced("ai_marketing_reader");

        // 6. The user can read the AI Index without any explicit index privilege, and DLS restricts the
        // visible documents to exactly the three that satisfy a whole nested element.
        assertUserSeesOnlyAuthorizedDocs();
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
                AI_INDEX_DASHBOARD_READ_ACTION,
                WORKFLOWS_PRIVILEGE,
                LOGIN_ACTION,
                AI_INDEX_WORKFLOW_READ_ACTION
            )
        );
        assertOK(client().performRequest(request));
    }

    /**
     * Grants dashboard/read in marketing and workflow/read in finance. The actions must differ per
     * space, otherwise the cross-space-leak fixture would be satisfied by either element and the
     * regression this test guards would be invisible.
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
        final Request create = new Request("PUT", "/" + AI_INDEX);
        create.setJsonEntity("""
            {
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
            """);
        assertOK(client().performRequest(create));

        // Documents deliberately carry no title/description/content: the template maps a semantic_text
        // sub-field on each of those, and populating one would require an inference-capable license.
        // This test is about the DLS filter, and its assertions run off document ids only.

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

        // HIDDEN — THE REGRESSION CASE. This is the whole point of the change.
        // Requires BOTH actions in marketing AND both in finance. The user holds
        // dashboard/read in marketing and workflow/read in finance — one of two in each.
        // Under the old flat composite-token design this document was VISIBLE: the user's two
        // tokens hit the flat count of 2 and terms_set could not tell they came from different
        // spaces. Under nested, each child is scored alone: marketing scores 1 < 2, finance
        // scores 1 < 2, no child matches, root hidden.
        indexDoc("cross-space-leak", """
            {
              "type": "dashboard",
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:dashboard/read", "ai_index:workflow/read"], "count": 2 },
                { "space": "finance",   "name": ["ai_index:dashboard/read", "ai_index:workflow/read"], "count": 2 }
              ]}}
            }
            """);

        // VISIBLE: no permissions block → public document.
        indexDoc("global-no-perms", """
            {
              "type": "dashboard"
            }
            """);
    }

    private void indexDoc(String id, String body) throws Exception {
        final Request request = new Request("PUT", "/" + AI_INDEX + "/_doc/" + id);
        request.addParameter("refresh", "true");
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
        assertThat("expected exactly one implicit ai-index-* grant, got " + indices, implicitEntries, hasSize(1));

        final Map<String, Object> implicit = implicitEntries.get(0);
        assertThat((List<String>) implicit.get("privileges"), equalTo(List.of("read")));

        final String query = (String) implicit.get("query");
        assertThat(query, containsString("\"nested\""));
        assertThat(query, containsString("permissions.kibana.privileges.space"));
        assertThat(query, containsString("permissions.kibana.privileges.name"));
        assertThat(query, containsString("permissions.kibana.privileges.count"));
        assertThat(query, containsString(AI_INDEX_DASHBOARD_READ_ACTION));
        assertThat(query, containsString(AI_INDEX_WORKFLOW_READ_ACTION));
        assertThat(query, containsString("terms_set"));
        // No delimiter anywhere — space and action are separate fields now.
        assertThat(query, not(containsString("|")));
        // Only ai_index: actions become DLS terms — login:/saved_object: in the same grant are dropped.
        assertThat(query, not(containsString(LOGIN_ACTION)));
        assertThat(query, not(containsString(SAVED_OBJECT_GET_ACTION)));
    }

    @SuppressWarnings("unchecked")
    private void assertUserSeesOnlyAuthorizedDocs() throws Exception {
        final Request search = new Request("GET", "/" + AI_INDEX + "/_search");
        search.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuth(SML_USER, SML_USER_PASSWORD)));
        final Response response = client().performRequest(search);
        assertOK(response);

        final Map<String, Object> body = entityAsMap(response);
        final Map<String, Object> hits = (Map<String, Object>) body.get("hits");
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        final List<String> visibleIds = hitList.stream().map(h -> (String) h.get("_id")).sorted().toList();

        // A zero-hit result is a FAILURE signal, not a pass: if the mapping and the query disagree
        // about whether the field is nested, nothing matches and over-restriction masquerades as
        // correct DLS. The positive expectations below are what catch that.
        assertThat("expected three visible docs, got " + hitList, visibleIds, hasSize(3));
        assertThat(visibleIds, equalTo(List.of("global-no-perms", "marketing-dashboard", "shared-dashboard")));
    }

    private static String basicAuth(String username, String password) {
        final String token = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}
