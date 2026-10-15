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
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end coverage for {@code ElasticAiMemoryImplicitPrivilegesProvider} against a real default-distribution
 * node, in the style of {@link ElasticAiIndexImplicitPrivilegesIT}.
 * <p>
 * Two users hold the same Kibana privilege in the same space. The implicit grant lets each read
 * {@code .ai-memory-*} without an explicit index privilege, and the templated DLS filter restricts the visible
 * documents to those the space and privilege check admits <em>and</em> that are either unscoped or scoped to
 * that user's profile uid. The uid in the filter is computed by Elasticsearch from the username, so the test
 * activates each user's profile and checks that the real uid is the one the filter matches on.
 */
public class ElasticAiMemoryImplicitPrivilegesIT extends ESRestTestCase {

    private static final String ADMIN_USER = "test-admin";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    private static final String ALICE = "alice";
    private static final String BOB = "bob";
    private static final String CAROL = "carol";
    private static final String USER_PASSWORD = "kibana-memory-password";
    private static final String MEMORY_READER_ROLE = "ai_memory_marketing_reader";

    private static final String KIBANA_APPLICATION = "kibana-.kibana";
    private static final String MEMORY_PRIVILEGE = "feature_memory.read";
    private static final String LOGIN_ACTION = "login:";
    private static final String MEMORY_READ_ACTION = "ai_index:memory/read";
    private static final String MEMORY_INDEX = ".ai-memory-test";
    // Installed by the stack plugin's AiIndexTemplateRegistry; matches every `.ai-memory-*` name.
    private static final String AI_MEMORY_MANAGED_TEMPLATE = "ai-memory-managed";

    private static final List<String> ALICE_VISIBLE_DOC_IDS = List.of(
        "alice-marketing",
        "alice-no-perms",
        "empty-scope-marketing",
        "shared-marketing",
        "unscoped-marketing",
        "unscoped-no-perms"
    );
    private static final List<String> BOB_VISIBLE_DOC_IDS = List.of(
        "bob-marketing",
        "empty-scope-marketing",
        "shared-marketing",
        "unscoped-marketing",
        "unscoped-no-perms"
    );
    // A user with no activated profile has no uid to match, so only unscoped memories remain.
    private static final List<String> NO_PROFILE_VISIBLE_DOC_IDS = List.of(
        "empty-scope-marketing",
        "unscoped-marketing",
        "unscoped-no-perms"
    );

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("kibana-elastic-ai-memory-implicit-privileges-cluster")
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

    public void testUsersOnlySeeTheirOwnAndSharedMemories() throws Exception {
        putKibanaPrivileges();
        putMemoryReaderRole();
        putUser(ALICE);
        putUser(BOB);

        // The uid the DLS template matches on must be the one Elasticsearch actually assigns the profile.
        final String aliceUid = activateProfile(ALICE);
        final String bobUid = activateProfile(BOB);

        createMemoryIndexWithDocs(aliceUid, bobUid);

        assertImplicitGrantSurfaced();

        assertUserSeesOnlyAuthorizedDocs(ALICE, ALICE_VISIBLE_DOC_IDS);
        assertUserSeesOnlyAuthorizedDocs(BOB, BOB_VISIBLE_DOC_IDS);
    }

    /** The uid comes from the user's activated profile, so a user who never activated one fails closed to unscoped memories. */
    public void testUserWithoutActivatedProfileSeesOnlyUnscopedMemories() throws Exception {
        putKibanaPrivileges();
        putMemoryReaderRole();
        putUser(ALICE);
        putUser(BOB);
        putUser(CAROL);
        final String aliceUid = activateProfile(ALICE);
        final String bobUid = activateProfile(BOB);
        createMemoryIndexWithDocs(aliceUid, bobUid);

        assertUserSeesOnlyAuthorizedDocs(CAROL, NO_PROFILE_VISIBLE_DOC_IDS);
    }

    /** The grant is read-only: a user cannot write (and so cannot forge a scope_id) through it. */
    public void testImplicitGrantDoesNotAllowWrites() throws Exception {
        putKibanaPrivileges();
        putMemoryReaderRole();
        putUser(ALICE);
        createMemoryIndex();

        final Request write = new Request("PUT", "/" + MEMORY_INDEX + "/_create/forged");
        write.setOptions(userOptions(ALICE));
        write.setJsonEntity("{ \"type\": \"memory\", \"scope_id\": [\"user://someone-else\"] }");
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(write));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
    }

    private void putKibanaPrivileges() throws Exception {
        final Request request = new Request("PUT", "/_security/privilege");
        request.setJsonEntity(Strings.format("""
            {
              "%s": {
                "%s": {
                  "actions": ["%s", "%s"]
                }
              }
            }
            """, KIBANA_APPLICATION, MEMORY_PRIVILEGE, LOGIN_ACTION, MEMORY_READ_ACTION));
        assertOK(client().performRequest(request));
    }

    /** Grants memory/read in marketing only, with no explicit index privileges. */
    private void putMemoryReaderRole() throws Exception {
        final Request request = new Request("PUT", "/_security/role/" + MEMORY_READER_ROLE);
        request.setJsonEntity(Strings.format("""
            {
              "cluster": [],
              "applications": [
                {
                  "application": "%s",
                  "privileges": ["%s"],
                  "resources": ["space:marketing"]
                }
              ]
            }
            """, KIBANA_APPLICATION, MEMORY_PRIVILEGE));
        assertOK(client().performRequest(request));
    }

    private void putUser(String username) throws Exception {
        final Request request = new Request("PUT", "/_security/user/" + username);
        request.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles": ["%s"]
            }
            """, USER_PASSWORD, MEMORY_READER_ROLE));
        assertOK(client().performRequest(request));
    }

    private String activateProfile(String username) throws Exception {
        final Request request = new Request("POST", "/_security/profile/_activate");
        request.setJsonEntity(Strings.format("""
            {
              "grant_type": "password",
              "username": "%s",
              "password": "%s"
            }
            """, username, USER_PASSWORD));
        final Response response = client().performRequest(request);
        assertOK(response);
        return ObjectPath.createFromResponse(response).evaluate("uid");
    }

    /**
     * Creates the memory index bare, as Kibana does, so every mapping on it comes from the
     * {@code ai-memory-managed} template.
     */
    private void createMemoryIndex() throws Exception {
        assertBusy(() -> {
            try {
                assertOK(client().performRequest(new Request("GET", "/_index_template/" + AI_MEMORY_MANAGED_TEMPLATE)));
            } catch (ResponseException e) {
                fail(e.getMessage());
            }
        });

        final Request create = new Request("PUT", "/" + MEMORY_INDEX);
        // Creating a dot-prefixed index emits a deprecation warning that is irrelevant to this test.
        create.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(client().performRequest(create));

        final Map<String, Object> mapping = entityAsMap(client().performRequest(new Request("GET", "/" + MEMORY_INDEX + "/_mapping")));
        final ObjectPath mappings = new ObjectPath(mapping.get(MEMORY_INDEX));
        assertThat(
            "expected [" + AI_MEMORY_MANAGED_TEMPLATE + "] to map permissions.kibana.privileges as nested",
            mappings.evaluate("mappings.properties.permissions.properties.kibana.properties.privileges.type"),
            equalTo("nested")
        );
        assertThat(
            "expected [" + AI_MEMORY_MANAGED_TEMPLATE + "] to map scope_id as keyword",
            mappings.evaluate("mappings.properties.scope_id.type"),
            equalTo("keyword")
        );
    }

    private void createMemoryIndexWithDocs(String aliceUid, String bobUid) throws Exception {
        createMemoryIndex();

        // Documents deliberately carry no title/description/content: the template maps a semantic_text
        // sub-field on each of those, and populating one would require an inference-capable license.

        // VISIBLE to alice only: her scope, in her space.
        indexDoc("alice-marketing", memory("marketing", "user://" + aliceUid));

        // VISIBLE to bob only.
        indexDoc("bob-marketing", memory("marketing", "user://" + bobUid));

        // VISIBLE to both: alice shared it with bob by adding his scope.
        indexDoc("shared-marketing", memory("marketing", "user://" + aliceUid, "user://" + bobUid));

        // VISIBLE to both: no scope_id means shared with everyone who can read the index.
        indexDoc("unscoped-marketing", memory("marketing"));

        // VISIBLE to both: an empty scope_id indexes no value, so it is unscoped.
        indexDoc("empty-scope-marketing", """
            {
              "type": "memory",
              "scope_id": [],
              "permissions": { "kibana": { "privileges": [
                { "space": "marketing", "name": ["ai_index:memory/read"], "count": 1 }
              ]}}
            }
            """);

        // HIDDEN from both: alice's scope, but a space neither user is in. Scope never overrides space.
        indexDoc("alice-finance", memory("finance", "user://" + aliceUid));

        // VISIBLE to alice only: no permissions block is public to any granted user, scope still applies.
        indexDoc("alice-no-perms", Strings.format("""
            {
              "type": "memory",
              "scope_id": ["user://%s"]
            }
            """, aliceUid));

        // VISIBLE to both: genuinely public.
        indexDoc("unscoped-no-perms", """
            {
              "type": "memory"
            }
            """);

        // HIDDEN from both: scoped to a user who is not alice or bob.
        indexDoc("other-user-marketing", memory("marketing", "user://u_someone_else_0"));

        // HIDDEN from both: reserved scope kinds do not match a user scope.
        indexDoc("team-scoped-marketing", memory("marketing", "team://t1"));

        assertOK(client().performRequest(new Request("POST", "/" + MEMORY_INDEX + "/_refresh")));
    }

    /** A memory requiring {@code ai_index:memory/read} in {@code space}, readable by the given scopes. */
    private static String memory(String space, String... scopeIds) {
        final String scopes = String.join(",", Arrays.stream(scopeIds).map(s -> "\"" + s + "\"").toList());
        return Strings.format("""
            {
              "type": "memory",
              "scope_id": [%s],
              "permissions": { "kibana": { "privileges": [
                { "space": "%s", "name": ["ai_index:memory/read"], "count": 1 }
              ]}}
            }
            """, scopes, space);
    }

    private void indexDoc(String id, String body) throws Exception {
        final Request request = new Request("PUT", "/" + MEMORY_INDEX + "/_create/" + id);
        request.setJsonEntity(body);
        assertOK(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private void assertImplicitGrantSurfaced() throws Exception {
        final Request request = new Request("GET", "/_security/role/" + MEMORY_READER_ROLE);
        request.addParameter("include_implicit", "true");
        final Response response = client().performRequest(request);
        assertOK(response);

        final Map<String, Object> body = entityAsMap(response);
        final Map<String, Object> role = (Map<String, Object>) body.get(MEMORY_READER_ROLE);
        final List<Map<String, Object>> indices = (List<Map<String, Object>>) role.get("indices");

        final List<Map<String, Object>> implicitEntries = indices.stream()
            .filter(entry -> Boolean.TRUE.equals(entry.get("implicitly_granted")))
            .filter(entry -> ((List<String>) entry.get("names")).stream().anyMatch(n -> n.startsWith(".ai-memory-")))
            .toList();
        assertThat("expected exactly one implicit memory grant, got " + indices, implicitEntries, hasSize(1));

        final Map<String, Object> implicit = implicitEntries.get(0);
        assertThat((List<String>) implicit.get("names"), containsInAnyOrder(".ai-memory-*"));
        assertThat((List<String>) implicit.get("privileges"), equalTo(List.of("read")));

        // The query is stored unrendered: the user is only known per request.
        final String query = (String) implicit.get("query");
        assertThat(query, containsString("\"template\""));
        assertThat(query, containsString("permissions.kibana.privileges.space"));
        assertThat(query, containsString(MEMORY_READ_ACTION));
        assertThat(query, containsString("\"exists\":{\"field\":\"scope_id\""));
        assertThat(query, containsString("\"scope_id\":{\"value\":\"user://{{_user.profile_uid}}\"}"));
    }

    /** Asserts the DLS-visible set through both query engines, so the two cannot drift apart. */
    private void assertUserSeesOnlyAuthorizedDocs(String username, List<String> expectedIds) throws Exception {
        final Request searchRequest = new Request("GET", "/" + MEMORY_INDEX + "/_search");
        searchRequest.setOptions(userOptions(username));

        final Request esqlRequest = new Request("POST", "/_query");
        esqlRequest.setOptions(userOptions(username));
        esqlRequest.setJsonEntity(Strings.format("{ \"query\": \"FROM %s METADATA _id | KEEP _id | LIMIT 100\" }", MEMORY_INDEX));

        final Response searchResponse = client().performRequest(searchRequest);
        assertOK(searchResponse);
        final List<Map<String, Object>> searchHits = ObjectPath.createFromResponse(searchResponse).evaluate("hits.hits");
        assertVisibleIds(username + " via _search", searchHits.stream().map(hit -> (String) hit.get("_id")).toList(), expectedIds);

        final Response esqlResponse = client().performRequest(esqlRequest);
        assertOK(esqlResponse);
        final List<List<Object>> esqlRows = ObjectPath.createFromResponse(esqlResponse).evaluate("values");
        assertVisibleIds(username + " via ES|QL", esqlRows.stream().map(row -> (String) row.get(0)).toList(), expectedIds);
    }

    private static void assertVisibleIds(String engine, List<String> ids, List<String> expectedIds) {
        final List<String> visibleIds = ids.stream().sorted().toList();
        assertThat("unexpected visible docs for " + engine, visibleIds, equalTo(expectedIds));
    }

    private static RequestOptions userOptions(String username) {
        return RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuth(username, USER_PASSWORD)).build();
    }

    private static String basicAuth(String username, String password) {
        final String token = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}
