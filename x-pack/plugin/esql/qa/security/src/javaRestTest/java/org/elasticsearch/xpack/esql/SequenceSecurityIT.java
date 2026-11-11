/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;

/** Exercises sequence PIT authorization with real security and a remote data shard. */
public class SequenceSecurityIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        // The default distribution supplies real DLS/FLS enforcement and the native ES|QL plugin.
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("test-admin", "x-pack-test-password", "test-admin", true)
        .user("sequence_dls_fls", "x-pack-test-password", "sequence_dls_fls", false)
        .user("sequence_alias_only", "x-pack-test-password", "sequence_alias_only", false)
        .user("sequence_alias_exclusions", "x-pack-test-password", "sequence_alias_exclusions", false)
        .build();

    /** Check the tested cluster before any sequence-specific fixture or assertion. */
    @Before
    public void requireSequenceCommand() {
        assumeTrue("requires native SEQUENCE", hasCapabilities(adminClient(), List.of("sequence_command")));
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test-admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private static RequestOptions runAsUserOptions(String user) {
        return RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user).build();
    }

    private Response runESQLCommand(String user, String command) throws IOException {
        Request request = new Request("POST", "_query");
        request.addParameter("error_trace", "true");
        var body = JsonXContent.contentBuilder().startObject().field("query", command);
        request.setJsonEntity(Strings.toString(body.endObject()));
        request.setOptions(runAsUserOptions(user));
        return assertOK(client().performRequest(request));
    }

    /** The existing default-distribution fixture verifies real Painless determinism declarations and HTTP error delivery. */
    public void testSequenceRepeatabilityWithPainless() throws Exception {
        createSequenceSecurityEvents(true);
        Request mappings = new Request("PUT", "sequence-security-events/_mapping");
        mappings.setJsonEntity("""
            {"runtime":{
              "stable":{"type":"long","script":{"source":"emit(1L)"}},
              "unstable":{"type":"long","script":{"source":"emit((long)(Math.random()*1000000))"}}
            }}
            """);
        assertOK(adminClient().performRequest(mappings));

        Map<String, Object> ordinary = entityAsMap(runESQLCommand("test-admin", "FROM sequence-security-events | KEEP unstable | LIMIT 1"));
        assertEquals(1, requireType(List.class, ordinary.get("values")).size());
        String stages = """
            SEQUENCE BY %s WITH {"maxspan":"30s"} PRIMARY 1
            (FROM sequence-security-events | WHERE phase=="A" AND tenant=="allowed"),
            (FROM sequence-security-events | WHERE phase=="B" AND tenant=="allowed")
            | EVAL member2_payload = MEMBER(2, payload)
            | KEEP %s, payload, member2_payload | LIMIT 10
            """;
        String stableQuery = stages.formatted("stable", "stable");
        Map<String, Object> stable = entityAsMap(runESQLCommand("test-admin", stableQuery));
        assertEquals(List.of(List.of(1, "visible-A", "visible-B")), stable.get("values"));

        ResponseException runtime = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("test-admin", stages.formatted("unstable", "unstable"))
        );
        assertEquals(HttpStatus.SC_BAD_REQUEST, runtime.getResponse().getStatusLine().getStatusCode());
        String runtimeError = EntityUtils.toString(runtime.getResponse().getEntity());
        assertThat(runtimeError, containsString("requires repeatable evaluation"));
        assertThat(runtimeError, containsString("nondeterministic runtime field [unstable]"));

        Request requestFilter = new Request("POST", "_query");
        requestFilter.setOptions(runAsUserOptions("test-admin"));
        requestFilter.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .field("query", stableQuery)
                    .field("filter", Map.of("script", Map.of("script", Map.of("source", "Math.random()<1.0"))))
                    .endObject()
            )
        );
        ResponseException filter = expectThrows(ResponseException.class, () -> client().performRequest(requestFilter));
        assertEquals(HttpStatus.SC_BAD_REQUEST, filter.getResponse().getStatusLine().getStatusCode());
        String filterError = EntityUtils.toString(filter.getResponse().getEntity());
        assertThat(filterError, containsString("requires repeatable evaluation"));
        assertThat(filterError, containsString("nondeterministic script"));
    }

    /** Automatic retention must use the same restricted originals even after stage and outer edits. */
    public void testAutomaticRetentionPreservesDocumentAndFieldSecurity() throws Exception {
        createSequenceSecurityEvents(true);
        Map<String, Object> ordinary = retentionQuery("sequence_dls_fls", """
            FROM sequence-security-events METADATA _id, _source
            | WHERE _id IN ("visible-A", "visible-B") | KEEP _id, _source | SORT _id | LIMIT 10
            """);
        List<?> ordinaryRows = requireType(List.class, ordinary.get("values"));
        assertEquals(2, ordinaryRows.size());
        Map<?, ?> first = requireType(Map.class, requireType(List.class, ordinaryRows.get(0)).get(1));
        Map<?, ?> second = requireType(Map.class, requireType(List.class, ordinaryRows.get(1)).get(1));
        assertEquals(Set.of("@timestamp", "key", "phase", "payload", "number"), first.keySet());
        assertEquals(first.keySet(), second.keySet());

        for (int primary : List.of(1, 2)) {
            int other = 3 - primary;
            Map<String, Object> result = retentionQuery("sequence_dls_fls", retentionStages(primary, true) + """
                | EVAL payload = CONCAT(payload, "-outer"), other_payload = MEMBER(%s, payload)
                | KEEP payload, other_payload | LIMIT 10 | RETURN MATCHES
                """.formatted(other));
            assertRetainedPair(result, primary, first, second);
            List<?> row = requireType(List.class, requireType(List.class, result.get("values")).getFirst());
            assertEquals(primary == 1 ? "visible-A-stage-outer" : "visible-B-stage-outer", row.get(0));
            assertEquals(other == 1 ? "visible-A-stage" : "visible-B-stage", row.get(1));
            assertEquals(9007199254740993L, first.get("number"));
        }
    }

    /** Controls prove forbidden fields exist, but neither primary nor explicit member access can read them. */
    public void testAutomaticRetentionDoesNotExposeForbiddenFields() throws Exception {
        createSequenceSecurityEvents(true);
        assertRetentionFieldDenied("sequence_dls_fls", "FROM sequence-security-events | KEEP secret | LIMIT 10");
        for (int primary : List.of(1, 2)) {
            for (String expression : List.of("secret", "MEMBER(" + (3 - primary) + ", secret)")) {
                String query = retentionStages(primary, false)
                    + " | EVAL leaked = "
                    + expression
                    + " | KEEP leaked | LIMIT 10 | RETURN MATCHES";
                Map<String, Object> unrestricted = retentionQuery("test-admin", query);
                List<?> rows = requireType(List.class, unrestricted.get("values"));
                assertEquals(2, rows.size());
                assertEquals(Set.of("hidden-A", "orphan-A"), new java.util.HashSet<>(retainedRoleValues(unrestricted, 1, "id")));
                for (Object value : rows)
                    assertNotNull(requireType(List.class, value).getFirst());
                assertRetentionFieldDenied("sequence_dls_fls", query);
            }
        }
    }

    /** Selecting a different primary must not merge either stage's alias, document or field grants. */
    public void testAutomaticRetentionKeepsStageAliasGrantsIndependent() throws Exception {
        createSequenceSecurityEvents(true);
        createSequenceSecurityAliases();
        createSequenceSecurityAdditionalIndex();
        String user = "sequence-retention-scoped";
        Request role = new Request("PUT", "_security/role/" + user);
        role.setJsonEntity("""
            {"indices":[
              {"names":["sequence-alias-a"],"privileges":["read"],
               "query":{"term":{"tenant":"allowed"}},
               "field_security":{"grant":["@timestamp","key","phase","payload","number"]}},
              {"names":["sequence-alias-b","sequence-alias-excluded"],"privileges":["read"],
               "field_security":{"grant":["*"]}}
            ]}
            """);
        assertOK(adminClient().performRequest(role));
        try {
            Request createUser = new Request("PUT", "_security/user/" + user);
            createUser.setJsonEntity(
                Strings.toString(
                    JsonXContent.contentBuilder()
                        .startObject()
                        .field("roles", List.of(user))
                        .field("password", randomAlphaOfLength(32))
                        .endObject()
                )
            );
            assertOK(adminClient().performRequest(createUser));
            try {
                Map<?, ?> first = ordinaryRetentionSource(user, "sequence-alias-a", "visible-A");
                Map<?, ?> second = ordinaryRetentionSource(user, "sequence-alias-b", "visible-B");
                assertEquals(Set.of("@timestamp", "key", "phase", "payload", "number"), first.keySet());
                assertEquals("secret-B", second.get("secret"));
                assertEquals("allowed", second.get("tenant"));
                for (int primary : List.of(1, 2)) {
                    String broadSecret = primary == 2 ? "secret" : "MEMBER(2, secret)";
                    String query = """
                        SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY %s
                        (FROM sequence-alias-a),
                        (FROM sequence-alias-b,sequence-alias-excluded | WHERE phase=="B")
                        | EVAL broad_secret = %s | KEEP payload, broad_secret | LIMIT 10 | RETURN MATCHES
                        """.formatted(primary, broadSecret);
                    Map<String, Object> result = retentionQuery(user, query);
                    assertRetainedPair(result, primary, first, second);
                    List<?> row = requireType(List.class, requireType(List.class, result.get("values")).getFirst());
                    assertEquals(primary == 1 ? "visible-A" : "visible-B", row.get(0));
                    assertEquals("secret-B", row.get(1));
                }
                // ES|QL deliberately reports unauthorized indices as unknown, unlike the search API's 403.
                for (String query : List.of(
                    "FROM sequence-security-events | KEEP payload | LIMIT 10",
                    retentionStages(2, false) + " | KEEP payload | LIMIT 10 | RETURN MATCHES"
                )) {
                    ResponseException denied = expectThrows(ResponseException.class, () -> retentionQuery(user, query));
                    assertEquals(HttpStatus.SC_BAD_REQUEST, denied.getResponse().getStatusLine().getStatusCode());
                    String error = EntityUtils.toString(denied.getResponse().getEntity());
                    assertThat(error, containsString("verification_exception"));
                    assertThat(error, containsString("Unknown index [sequence-security-events]"));
                }
            } finally {
                assertOK(adminClient().performRequest(new Request("DELETE", "_security/user/" + user)));
            }
        } finally {
            assertOK(adminClient().performRequest(new Request("DELETE", "_security/role/" + user)));
        }
    }

    private static String retentionStages(int primary, boolean edit) {
        String stageEdit = edit ? " | EVAL payload = CONCAT(payload, \"-stage\") | KEEP key, payload, number" : "";
        return """
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY %s
            (FROM sequence-security-events | WHERE phase=="A"%s),
            (FROM sequence-security-events | WHERE phase=="B"%s)
            """.formatted(primary, stageEdit, stageEdit);
    }

    private Map<String, Object> retentionQuery(String user, String query) throws IOException {
        Request request = new Request("POST", "_query");
        request.addParameter("drop_null_columns", "false");
        request.setOptions(runAsUserOptions(user));
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .field("query", query)
                    .field(
                        "filter",
                        Map.of("range", Map.of("@timestamp", Map.of("gte", "2026-09-25T21:14:51Z", "lt", "2026-09-25T21:15:30Z")))
                    )
                    .endObject()
            )
        );
        Map<String, Object> result = entityAsMap(assertOK(client().performRequest(request)));
        assertEquals(Boolean.FALSE, result.get("is_partial"));
        return result;
    }

    private void assertRetentionFieldDenied(String user, String query) {
        ResponseException denied = expectThrows(ResponseException.class, () -> retentionQuery(user, query));
        assertEquals(HttpStatus.SC_BAD_REQUEST, denied.getResponse().getStatusLine().getStatusCode());
        try {
            String reason = EntityUtils.toString(denied.getResponse().getEntity());
            assertThat(reason, containsString("Unknown column"));
            assertThat(reason, containsString("secret"));
        } catch (IOException ex) {
            throw new AssertionError(ex);
        }
    }

    private Map<?, ?> ordinaryRetentionSource(String user, String index, String id) throws IOException {
        Map<String, Object> result = retentionQuery(
            user,
            "FROM " + index + " METADATA _id, _source | WHERE _id == \"" + id + "\" | KEEP _source | LIMIT 10"
        );
        List<?> rows = requireType(List.class, result.get("values"));
        assertEquals(1, rows.size());
        return requireType(Map.class, requireType(List.class, rows.getFirst()).getFirst());
    }

    private static Map<?, ?> retentionContract(Map<String, Object> result) {
        Map<?, ?> found = null;
        java.util.Set<Object> ids = new java.util.HashSet<>();
        for (Object c : requireType(List.class, result.get("columns"))) {
            Map<?, ?> column = requireType(Map.class, c);
            Map<?, ?> metadata = requireType(Map.class, requireType(Map.class, column.get("_meta")).get("sequence"));
            assertTrue(ids.add(requireType(String.class, metadata.get("column_id"))));
            if (metadata.containsKey("contract")) {
                assertNull("exactly one authoritative description", found);
                found = requireType(Map.class, metadata.get("contract"));
            }
        }
        assertNotNull(found);
        assertEquals(2, found.get("member_count"));
        return found;
    }

    private static List<?> retainedRoleValues(Map<String, Object> result, int stage, String role) {
        Map<?, ?> contract = retentionContract(result);
        List<?> members = requireType(List.class, contract.get("members"));
        assertEquals(2, members.size());
        Map<?, ?> member = requireType(Map.class, members.get(stage - 1));
        assertEquals(stage, member.get("stage"));
        String id = requireType(String.class, member.get(role));
        List<?> columns = requireType(List.class, result.get("columns"));
        int position = -1;
        for (int i = 0; i < columns.size(); i++) {
            Map<?, ?> c = requireType(Map.class, columns.get(i));
            Map<?, ?> m = requireType(Map.class, requireType(Map.class, c.get("_meta")).get("sequence"));
            if (id.equals(m.get("column_id"))) position = i;
        }
        assertTrue("role resolves to an emitted column", position >= 0);
        final int resolved = position;
        List<?> rows = requireType(List.class, result.get("values"));
        return rows.stream().map(r -> requireType(List.class, r).get(resolved)).toList();
    }

    private static void assertRetainedPair(Map<String, Object> result, int primary, Map<?, ?> first, Map<?, ?> second) {
        assertEquals(1, requireType(List.class, result.get("values")).size());
        assertEquals(primary, retentionContract(result).get("primary_stage"));
        assertEquals(List.of("visible-A"), retainedRoleValues(result, 1, "id"));
        assertEquals(List.of("visible-B"), retainedRoleValues(result, 2, "id"));
        for (int stage : List.of(1, 2))
            assertEquals(List.of("sequence-security-events"), retainedRoleValues(result, stage, "index"));
        assertEquals(List.of(first), retainedRoleValues(result, 1, "source"));
        assertEquals(List.of(second), retainedRoleValues(result, 2, "source"));
    }

    /** Both successful and rejected queries must release PIT contexts and request accounting. */
    @org.junit.After
    public void assertRetentionResourcesReleased() throws Exception {
        assertBusy(() -> {
            Request stats = new Request("GET", "_nodes/stats/indices,breaker");
            stats.addParameter("filter_path", "nodes.*.indices.search.open_contexts,nodes.*.breakers.request.estimated_size_in_bytes");
            Map<?, ?> nodes = requireType(Map.class, entityAsMap(adminClient().performRequest(stats)).get("nodes"));
            assertEquals(2, nodes.size());
            for (Object value : nodes.values()) {
                Map<?, ?> node = requireType(Map.class, value);
                Map<?, ?> search = requireType(Map.class, requireType(Map.class, node.get("indices")).get("search"));
                Map<?, ?> breaker = requireType(Map.class, requireType(Map.class, node.get("breakers")).get("request"));
                assertEquals(0L, requireType(Number.class, search.get("open_contexts")).longValue());
                assertEquals(0L, requireType(Number.class, breaker.get("estimated_size_in_bytes")).longValue());
            }
        });
    }

    /** Original source metadata retains DLS/FLS after stage and downstream projection. */
    public void testStageSourceRetainsSecurityThroughProjection() throws Exception {
        createSequenceSecurityEvents(true);
        Map<String, Object> result = retentionQuery("sequence_dls_fls", """
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM sequence-security-events METADATA _id, _index, _source | WHERE phase=="A" | KEEP key, _id, _index, _source),
            (FROM sequence-security-events METADATA _id, _index, _source | WHERE phase=="B" | KEEP key, _id, _index, _source)
            | KEEP key | LIMIT 10 | RETURN MATCHES
            """);
        Map<?, ?> firstSource = requireType(Map.class, retainedRoleValues(result, 1, "source").getFirst());
        Map<?, ?> secondSource = requireType(Map.class, retainedRoleValues(result, 2, "source").getFirst());
        assertRetainedPair(result, 1, firstSource, secondSource);
        assertEquals("shared", requireType(List.class, requireType(List.class, result.get("values")).getFirst()).getFirst());
        Set<String> granted = Set.of("@timestamp", "key", "phase", "payload", "number");
        assertEquals(granted, firstSource.keySet());
        assertEquals(granted, secondSource.keySet());
        assertEquals("visible-A", firstSource.get("payload"));
        assertEquals("visible-B", secondSource.get("payload"));
        assertEquals(9007199254740993L, firstSource.get("number"));
        assertFalse(firstSource.containsKey("secret"));
        assertFalse(firstSource.containsKey("tenant"));
    }

    /** Ordinary source columns match normal FROM under the same DLS/FLS grant. */
    public void testStageSourceMatchesOrdinaryFieldSecurity() throws Exception {
        createSequenceSecurityEvents(true);
        Map<String, Object> ordinary = entityAsMap(runESQLCommand("sequence_dls_fls", """
            FROM sequence-security-events METADATA _id, _index, _source
            | WHERE _id IN ("visible-A", "visible-B") | KEEP _id, _index, _source | SORT _id | LIMIT 10
            """));
        List<?> ordinaryRows = requireType(List.class, ordinary.get("values"));
        assertEquals(2, ordinaryRows.size());
        Map<?, ?> firstSource = requireType(Map.class, requireType(List.class, ordinaryRows.get(0)).get(2));
        Map<?, ?> secondSource = requireType(Map.class, requireType(List.class, ordinaryRows.get(1)).get(2));
        Set<String> granted = Set.of("@timestamp", "key", "phase", "payload", "number");
        assertEquals(granted, firstSource.keySet());
        assertEquals(granted, secondSource.keySet());
        assertEquals("visible-A", firstSource.get("payload"));
        assertEquals("visible-B", secondSource.get("payload"));
        assertEquals(9007199254740993L, firstSource.get("number"));
        String stages = """
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM sequence-security-events METADATA _id, _index, _source | WHERE phase=="A"),
            (FROM sequence-security-events METADATA _id, _index, _source | WHERE phase=="B")
            """;
        Map<String, Object> columns = retentionQuery("sequence_dls_fls", stages + " | KEEP key | LIMIT 10 | RETURN MATCHES");
        assertRetainedPair(columns, 1, firstSource, secondSource);
        assertEquals("shared", requireType(List.class, requireType(List.class, columns.get("values")).getFirst()).getFirst());
    }

    /** CSV acceptance must match ordinary source rendering; it is a display format rather than JSON evidence. */
    public void testStageSourceCsvMatchesOrdinarySourceRendering() throws Exception {
        createSequenceSecurityEvents(true);
        String ordinary = runSourceCsv("""
            FROM sequence-security-events METADATA _id, _source
            | WHERE _id == "visible-A" | KEEP _source | LIMIT 10
            """);
        String sequence = runSourceCsv("""
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM sequence-security-events METADATA _source | WHERE phase=="A"),
            (FROM sequence-security-events METADATA _source | WHERE phase=="B")
            | KEEP _source | LIMIT 10
            """);
        assertEquals(ordinary, sequence);
        assertThat(sequence, containsString("visible-A"));
        assertThat(sequence, containsString("9007199254740993"));
        assertFalse(sequence.contains("secret"));
        assertFalse(sequence.contains("tenant"));
    }

    private String runSourceCsv(String query) throws IOException {
        Request request = new Request("POST", "_query");
        request.addParameter("format", "csv");
        request.setOptions(runAsUserOptions("sequence_dls_fls"));
        request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("query", query).endObject()));
        return EntityUtils.toString(assertOK(client().performRequest(request)).getEntity());
    }

    /** A broader second-stage alias grant may expose its own source fields, but cannot widen the first stage's source or documents. */
    public void testStageSourceRetainsStageSpecificAliasDocumentAndFieldGrants() throws Exception {
        createSequenceSecurityEvents(true);
        createSequenceSecurityAliases();
        String user = "sequence-source-scoped";
        Request role = new Request("PUT", "_security/role/" + user);
        role.setJsonEntity("""
            {"indices":[
              {"names":["sequence-alias-a"],"privileges":["read"],
               "query":{"term":{"tenant":"allowed"}},
               "field_security":{"grant":["@timestamp","key","phase","payload","number"]}},
              {"names":["sequence-alias-b"],"privileges":["read"],"field_security":{"grant":["*"]}}
            ]}
            """);
        assertOK(adminClient().performRequest(role));
        try {
            Request createUser = new Request("PUT", "_security/user/" + user);
            createUser.setJsonEntity(
                Strings.toString(
                    JsonXContent.contentBuilder()
                        .startObject()
                        .field("roles", List.of(user))
                        .field("password", randomAlphaOfLength(32))
                        .endObject()
                )
            );
            assertOK(adminClient().performRequest(createUser));
            try {
                Map<String, Object> result = retentionQuery(user, """
                    SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
                    (FROM sequence-alias-a METADATA _id, _index, _source | KEEP key, _id, _index, _source),
                    (FROM sequence-alias-b METADATA _id, _index, _source | KEEP key, _id, _index, _source)
                    | KEEP key | LIMIT 10 | RETURN MATCHES
                    """);
                Map<?, ?> firstSource = requireType(Map.class, retainedRoleValues(result, 1, "source").getFirst());
                Map<?, ?> secondSource = requireType(Map.class, retainedRoleValues(result, 2, "source").getFirst());
                assertRetainedPair(result, 1, firstSource, secondSource);
                assertEquals("shared", requireType(List.class, requireType(List.class, result.get("values")).getFirst()).getFirst());
                assertEquals(Set.of("@timestamp", "key", "phase", "payload", "number"), firstSource.keySet());
                assertFalse(firstSource.containsKey("secret"));
                assertFalse(firstSource.containsKey("tenant"));
                assertEquals("secret-B", secondSource.get("secret"));
                assertEquals("allowed", secondSource.get("tenant"));
                Request direct = new Request("GET", "sequence-security-events/_search");
                direct.setOptions(runAsUserOptions(user));
                ResponseException denied = expectThrows(ResponseException.class, () -> client().performRequest(direct));
                assertEquals(HttpStatus.SC_FORBIDDEN, denied.getResponse().getStatusLine().getStatusCode());
            } finally {
                assertOK(adminClient().performRequest(new Request("DELETE", "_security/user/" + user)));
            }
        } finally {
            assertOK(adminClient().performRequest(new Request("DELETE", "_security/role/" + user)));
        }
    }

    private static <T> T requireType(Class<T> type, Object value) {
        assertTrue("expected " + type.getSimpleName(), type.isInstance(value));
        return type.cast(value);
    }

    /** Hidden predecessors must never enter matching, and typed evidence fetches must retain the caller's field permissions. */
    public void testSequenceRetainsDocumentAndFieldSecurityDuringEvidenceReplay() throws Exception {
        createSequenceSecurityEvents();
        String stages = """
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM sequence-security-events | WHERE phase=="A"),
            (FROM sequence-security-events | WHERE phase=="B")
            """;
        Map<String, Object> allowed = entityAsMap(
            runESQLCommand(
                "sequence_dls_fls",
                stages + " | EVAL member2_payload = MEMBER(2, payload) | KEEP key, payload, number, member2_payload | LIMIT 10"
            )
        );
        assertEquals(List.of(List.of("shared", "visible-A", 9007199254740993L, "visible-B")), allowed.get("values"));
        Map<String, Object> unrestricted = entityAsMap(runESQLCommand("test-admin", stages + " | STATS count(*) | LIMIT 1"));
        assertEquals(List.of(List.of(2)), unrestricted.get("values"));
        ResponseException denied = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("sequence_dls_fls", stages + " | KEEP secret | LIMIT 1")
        );
        assertEquals(HttpStatus.SC_BAD_REQUEST, denied.getResponse().getStatusLine().getStatusCode());
        assertThat(EntityUtils.toString(denied.getResponse().getEntity()), containsString("Unknown column [secret]"));
    }

    /** Alias grants must authorize every PIT page without granting access to its concrete index. */
    public void testSequenceRetainsAliasOnlyDocumentAndFieldPermissions() throws Exception {
        createSequenceSecurityEvents();
        createSequenceSecurityAliases();
        String stages = """
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM sequence-alias-a),
            (FROM sequence-alias-*,-sequence-alias-a)
            """;
        Map<String, Object> allowed = entityAsMap(
            runESQLCommand(
                "sequence_alias_only",
                stages + " | EVAL member2_payload = MEMBER(2, payload) | KEEP key, payload, number, member2_payload | LIMIT 10"
            )
        );
        assertEquals(List.of(List.of("shared", "visible-A", 9007199254740993L, "visible-B")), allowed.get("values"));
        ResponseException denied = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("sequence_alias_only", stages + " | KEEP secret | LIMIT 1")
        );
        assertEquals(HttpStatus.SC_BAD_REQUEST, denied.getResponse().getStatusLine().getStatusCode());
        assertThat(EntityUtils.toString(denied.getResponse().getEntity()), containsString("Unknown column [secret]"));
        Request direct = new Request("GET", "sequence-security-events/_search");
        direct.setOptions(runAsUserOptions("sequence_alias_only"));
        ResponseException forbidden = expectThrows(ResponseException.class, () -> client().performRequest(direct));
        assertEquals(HttpStatus.SC_FORBIDDEN, forbidden.getResponse().getStatusLine().getStatusCode());
    }

    /** A branch-local exclusion must also exclude that alias's broader document permissions when opening the shared PIT. */
    public void testSequenceIndexExclusionsDoNotBroadenAliasDocumentPermissions() throws Exception {
        createSequenceSecurityEvents();
        createSequenceSecurityAliases();
        Map<String, Object> allowed = entityAsMap(runESQLCommand("sequence_alias_exclusions", """
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM sequence-alias-*,-sequence-alias-b,-sequence-alias-excluded),
            (FROM sequence-alias-b)
            | EVAL member2_payload = MEMBER(2, payload)
            | KEEP key, payload, number, member2_payload | LIMIT 10
            """));
        assertEquals(List.of(List.of("shared", "visible-A", 9007199254740993L, "visible-B")), allowed.get("values"));
    }

    /** Each stage must retain its own document grants even when another stage uses a broader alias on the same index. */
    public void testSequenceStagesRetainIndependentAliasDocumentPermissions() throws Exception {
        createSequenceSecurityEvents();
        createSequenceSecurityAliases();
        // The broader alias also reaches an out-of-scope physical index. Each data node already holds a shard of the
        // first index, so a node query batch could otherwise merge that alias back into the first stage's permissions.
        createSequenceSecurityAdditionalIndex();
        Map<String, Object> ordinary = entityAsMap(runESQLCommand("sequence_alias_exclusions", """
            FROM (FROM sequence-alias-a METADATA _id, _index, _source | EVAL branch="A"),
                 (FROM sequence-alias-b,sequence-alias-excluded | WHERE phase=="B" | EVAL branch="B")
            | WHERE branch=="A" | KEEP payload | LIMIT 10
            """));
        assertEquals(List.of(List.of("visible-A")), ordinary.get("values"));
        Map<String, Object> allowed = entityAsMap(runESQLCommand("sequence_alias_exclusions", """
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM sequence-alias-a),
            (FROM sequence-alias-b,sequence-alias-excluded | WHERE phase=="B")
            | EVAL member2_payload = MEMBER(2, payload)
            | KEEP key, payload, number, member2_payload | LIMIT 10
            """));
        assertEquals(List.of(List.of("shared", "visible-A", 9007199254740993L, "visible-B")), allowed.get("values"));
    }

    /** PIT can-match may omit an existing source index when its alias is empty; that stage still yields zero matches. */
    public void testSequenceAllowsAuthorizedSourceOmittedFromSnapshot() throws Exception {
        createSequenceSecurityEvents();
        createSequenceSecurityAliases();
        createSequenceSecurityAdditionalIndex();
        Request event = new Request("PUT", "sequence-security-other/_doc/B");
        event.addParameter("refresh", "true");
        event.setJsonEntity("""
            {"@timestamp":"2026-09-25T21:14:51.389000002Z","key":"shared","phase":"B","payload":"visible-B","tenant":"allowed","number":8}
            """);
        assertOK(client().performRequest(event));
        Request aliases = new Request("POST", "_aliases");
        aliases.setJsonEntity("""
            {"actions":[
              {"add":{"index":"sequence-security-events","alias":"sequence-alias-a","filter":{"match_none":{}}}},
              {"remove":{"index":"sequence-security-events","alias":"sequence-alias-b"}},
              {"add":{"index":"sequence-security-other","alias":"sequence-alias-b","filter":{"term":{"phase":"B"}}}}
            ]}
            """);
        assertOK(client().performRequest(aliases));
        Map<String, Object> allowed = entityAsMap(runESQLCommand("sequence_alias_only", """
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM sequence-alias-a), (FROM sequence-alias-b)
            | EVAL member2_payload = MEMBER(2, payload)
            | KEEP key, payload, member2_payload | LIMIT 10
            """));
        assertEquals(List.of(), allowed.get("values"));
    }

    private void createSequenceSecurityAdditionalIndex() throws IOException {
        Request create = new Request("PUT", "sequence-security-other");
        create.setJsonEntity("""
            {"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"_source":{"enabled":false},"properties":{
              "@timestamp":{"type":"date_nanos"},"key":{"type":"keyword"},"phase":{"type":"keyword"},
              "payload":{"type":"keyword"},"tenant":{"type":"keyword"},"secret":{"type":"keyword"},"number":{"type":"long"}}}}
            """);
        assertOK(client().performRequest(create));
        Request event = new Request("PUT", "sequence-security-other/_doc/other");
        event.addParameter("refresh", "true");
        event.setJsonEntity("""
            {"@timestamp":"2026-09-25T21:14:51.389000000Z","key":"unused","phase":"A",\
            "payload":"other","tenant":"hidden","secret":"other","number":0}
            """);
        assertOK(client().performRequest(event));
        Request alias = new Request("POST", "_aliases");
        alias.setJsonEntity("""
            {"actions":[{"add":{"index":"sequence-security-other","alias":"sequence-alias-excluded","filter":{"term":{"phase":"A"}}}}]}
            """);
        assertOK(client().performRequest(alias));
    }

    private void createSequenceSecurityAliases() throws IOException {
        Request aliases = new Request("POST", "_aliases");
        aliases.setJsonEntity("""
            {"actions":[
              {"add":{"index":"sequence-security-events","alias":"sequence-alias-a","filter":{"term":{"phase":"A"}}}},
              {"add":{"index":"sequence-security-events","alias":"sequence-alias-b","filter":{"term":{"phase":"B"}}}},
              {"add":{"index":"sequence-security-events","alias":"sequence-alias-excluded","filter":{"term":{"phase":"A"}}}}
            ]}
            """);
        assertOK(client().performRequest(aliases));
    }

    private void createSequenceSecurityEvents() throws IOException {
        createSequenceSecurityEvents(false);
    }

    private void createSequenceSecurityEvents(boolean sourceEnabled) throws IOException {
        Request create = new Request("PUT", "sequence-security-events");
        create.setJsonEntity("""
            {"settings":{"number_of_shards":2,"number_of_replicas":0,"routing.allocation.total_shards_per_node":1},\
            "mappings":{"_source":{"enabled":%s},"properties":{
              "@timestamp":{"type":"date_nanos"},"key":{"type":"keyword"},"phase":{"type":"keyword"},
              "payload":{"type":"keyword"},"tenant":{"type":"keyword"},"secret":{"type":"keyword"},"number":{"type":"long"}}}}
            """.formatted(sourceEnabled));
        assertOK(client().performRequest(create));
        Request bulk = new Request("POST", "_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity("""
            {"index":{"_index":"sequence-security-events","_id":"visible-A"}}
            {"@timestamp":"2026-09-25T21:14:51.389000000Z","key":"shared","phase":"A",\
            "payload":"visible-A","tenant":"allowed","secret":"secret-A","number":9007199254740993}
            {"index":{"_index":"sequence-security-events","_id":"hidden-A"}}
            {"@timestamp":"2026-09-25T21:14:51.389000001Z","key":"shared","phase":"A",\
            "payload":"hidden-A","tenant":"hidden","secret":"hidden-secret","number":7}
            {"index":{"_index":"sequence-security-events","_id":"visible-B"}}
            {"@timestamp":"2026-09-25T21:14:51.389000002Z","key":"shared","phase":"B",\
            "payload":"visible-B","tenant":"allowed","secret":"secret-B","number":8}
            {"index":{"_index":"sequence-security-events","_id":"orphan-A"}}
            {"@timestamp":"2026-09-25T21:14:51.389000003Z","key":"orphan","phase":"A",\
            "payload":"orphan-A","tenant":"hidden","secret":"orphan-secret","number":9}
            {"index":{"_index":"sequence-security-events","_id":"orphan-B"}}
            {"@timestamp":"2026-09-25T21:14:51.389000004Z","key":"orphan","phase":"B",\
            "payload":"orphan-B","tenant":"allowed","secret":"orphan-secret","number":10}
            """);
        Map<String, Object> indexed = entityAsMap(client().performRequest(bulk));
        assertEquals(false, indexed.get("errors"));
    }

}
