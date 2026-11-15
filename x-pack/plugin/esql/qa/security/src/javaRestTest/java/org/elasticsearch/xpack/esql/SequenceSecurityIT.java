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
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
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
            SEQUENCE BY %s WITHIN 30s PRIMARY 1
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

    /** Provenance identifies only the authorized matched documents after stage and outer edits. */
    public void testProvenancePreservesDocumentAndFieldSecurity() throws Exception {
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
                | KEEP payload, other_payload, _id, _index, sequence_provenance | LIMIT 10
                """.formatted(other));
            assertProvenancePair(result);
            List<?> row = requireType(List.class, requireType(List.class, result.get("values")).getFirst());
            assertEquals(primary == 1 ? "visible-A-stage-outer" : "visible-B-stage-outer", row.get(0));
            assertEquals(other == 1 ? "visible-A-stage" : "visible-B-stage", row.get(1));
            assertEquals("edited-id", row.get(2));
            assertEquals("edited-index", row.get(3));
            assertEquals(9007199254740993L, first.get("number"));
        }
    }

    /** Controls prove forbidden fields exist, but neither primary nor explicit member access can read them. */
    public void testSequenceProvenanceDoesNotExposeForbiddenFields() throws Exception {
        createSequenceSecurityEvents(true);
        assertRetentionFieldDenied("sequence_dls_fls", "FROM sequence-security-events | KEEP secret | LIMIT 10");
        for (int primary : List.of(1, 2)) {
            for (String expression : List.of("secret", "MEMBER(" + (3 - primary) + ", secret)")) {
                String query = retentionStages(primary, false)
                    + " | EVAL leaked = "
                    + expression
                    + " | KEEP leaked,sequence_provenance | LIMIT 10";
                Map<String, Object> unrestricted = retentionQuery("test-admin", query);
                List<?> rows = requireType(List.class, unrestricted.get("values"));
                assertEquals(2, rows.size());
                assertEquals(Set.of("hidden-A", "orphan-A"), new java.util.HashSet<>(provenanceRoleValues(unrestricted, 1, "id")));
                for (Object value : rows)
                    assertNotNull(requireType(List.class, value).getFirst());
                assertRetentionFieldDenied("sequence_dls_fls", query);
            }
        }
    }

    /** Selecting a different primary must not merge either stage's alias, document or field grants. */
    public void testSequenceProvenanceKeepsStageAliasGrantsIndependent() throws Exception {
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
                        SEQUENCE BY key WITHIN 30s PRIMARY %s
                        (FROM sequence-alias-a),
                        (FROM sequence-alias-b,sequence-alias-excluded | WHERE phase=="B")
                        | EVAL broad_secret = %s | KEEP payload, broad_secret, sequence_provenance | LIMIT 10
                        """.formatted(primary, broadSecret);
                    Map<String, Object> result = retentionQuery(user, query);
                    assertProvenancePair(result);
                    List<?> row = requireType(List.class, requireType(List.class, result.get("values")).getFirst());
                    assertEquals(primary == 1 ? "visible-A" : "visible-B", row.get(0));
                    assertEquals("secret-B", row.get(1));
                }
                // ES|QL deliberately reports unauthorized indices as unknown, unlike the search API's 403.
                for (String query : List.of(
                    "FROM sequence-security-events | KEEP payload | LIMIT 10",
                    retentionStages(2, false) + " | KEEP payload | LIMIT 10"
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
        String stageEdit = edit
            ? " | EVAL payload = CONCAT(payload, \"-stage\"), _id=\"edited-id\", _index=\"edited-index\" | KEEP key,payload,number,_id,_index"
            : "";
        return """
            SEQUENCE BY key WITHIN 30s PRIMARY %s
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

    /** Read each identity by its own stage tag; multivalue order and public column labels carry no role metadata. */
    private static List<?> provenanceRoleValues(Map<String, Object> result, int stage, String role) {
        if (role.equals("id") == false && role.equals("index") == false) throw new IllegalArgumentException("unsupported provenance role");
        List<Object> values = new ArrayList<>();
        for (Object cell : ordinaryColumnValues(result, "sequence_provenance")) {
            Map<Integer, Map<String, Object>> members = new HashMap<>();
            for (Object item : requireType(List.class, cell)) {
                try (
                    var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, requireType(String.class, item))
                ) {
                    Map<String, Object> member = parser.map();
                    assertEquals(Set.of("stage", "index", "id"), member.keySet());
                    int position = requireType(Integer.class, member.get("stage"));
                    requireType(String.class, member.get("id"));
                    requireType(String.class, member.get("index"));
                    assertNull("one entry per stage", members.putIfAbsent(position, member));
                } catch (IOException failure) {
                    throw new AssertionError("invalid provenance JSON", failure);
                }
            }
            assertEquals(Set.of(1, 2), members.keySet());
            values.add(members.get(stage).get(role));
        }
        return values;
    }

    private static List<?> ordinaryColumnValues(Map<String, Object> result, String name) {
        List<?> columns = requireType(List.class, result.get("columns"));
        int position = -1;
        for (int column = 0; column < columns.size(); column++) {
            if (name.equals(requireType(Map.class, columns.get(column)).get("name"))) position = column;
        }
        assertTrue("ordinary output column " + name + " must be present", position >= 0);
        final int resolved = position;
        List<?> rows = requireType(List.class, result.get("values"));
        return rows.stream().map(row -> requireType(List.class, row).get(resolved)).toList();
    }

    private static void assertProvenancePair(Map<String, Object> result) {
        assertEquals(1, requireType(List.class, result.get("values")).size());
        assertEquals(List.of("visible-A"), provenanceRoleValues(result, 1, "id"));
        assertEquals(List.of("visible-B"), provenanceRoleValues(result, 2, "id"));
        for (int stage : List.of(1, 2)) {
            assertEquals(List.of("sequence-security-events"), provenanceRoleValues(result, stage, "index"));
        }
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

    /** Explicit ordinary SOURCE and MEMBER ids retain DLS/FLS through stage and downstream projection. */
    public void testStageSourceRetainsSecurityThroughProjection() throws Exception {
        createSequenceSecurityEvents(true);
        Map<?, ?> firstSource = ordinaryRetentionSource("sequence_dls_fls", "sequence-security-events", "visible-A");
        Map<?, ?> secondSource = ordinaryRetentionSource("sequence_dls_fls", "sequence-security-events", "visible-B");
        Set<String> granted = Set.of("@timestamp", "key", "phase", "payload", "number");
        assertEquals(granted, firstSource.keySet());
        assertEquals(granted, secondSource.keySet());
        assertEquals("visible-A", firstSource.get("payload"));
        assertEquals("visible-B", secondSource.get("payload"));
        assertEquals(9007199254740993L, firstSource.get("number"));
        assertFalse(firstSource.containsKey("secret"));
        assertFalse(firstSource.containsKey("tenant"));
        for (int primary : List.of(1, 2)) {
            Map<String, Object> result = retentionQuery("sequence_dls_fls", """
                SEQUENCE BY key WITHIN 30s PRIMARY %s
                (FROM sequence-security-events METADATA _id,_index,_source | WHERE phase=="A" | KEEP key,_id,_index,_source),
                (FROM sequence-security-events METADATA _id,_index,_source | WHERE phase=="B" | KEEP key,_id,_index,_source)
                | EVAL other_id=MEMBER(%s,_id)
                | KEEP key,_source,_id,_index,other_id,sequence_provenance | LIMIT 10
                """.formatted(primary, 3 - primary));
            assertProvenancePair(result);
            List<?> row = requireType(List.class, requireType(List.class, result.get("values")).getFirst());
            assertEquals("shared", row.get(0));
            assertEquals(primary == 1 ? firstSource : secondSource, row.get(1));
            assertEquals(primary == 1 ? "visible-A" : "visible-B", row.get(2));
            assertEquals("sequence-security-events", row.get(3));
            assertEquals(primary == 1 ? "visible-B" : "visible-A", row.get(4));
        }
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
        for (int primary : List.of(1, 2)) {
            String stages = """
                SEQUENCE BY key WITHIN 30s PRIMARY %s
                (FROM sequence-security-events METADATA _id, _index, _source | WHERE phase=="A"),
                (FROM sequence-security-events METADATA _id, _index, _source | WHERE phase=="B")
                """.formatted(primary);
            Map<String, Object> columns = retentionQuery("sequence_dls_fls", stages + " | KEEP key,_source,sequence_provenance | LIMIT 10");
            assertProvenancePair(columns);
            assertEquals(List.of(primary == 1 ? firstSource : secondSource), ordinaryColumnValues(columns, "_source"));
            assertEquals("shared", requireType(List.class, requireType(List.class, columns.get("values")).getFirst()).getFirst());
        }
    }

    /** CSV acceptance must match ordinary source rendering; it is a display format rather than JSON evidence. */
    public void testStageSourceCsvMatchesOrdinarySourceRendering() throws Exception {
        createSequenceSecurityEvents(true);
        String ordinary = runSourceCsv("""
            FROM sequence-security-events METADATA _id, _source
            | WHERE _id == "visible-A" | KEEP _source | LIMIT 10
            """);
        String sequence = runSourceCsv("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
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
                Map<?, ?> firstSource = ordinaryRetentionSource(user, "sequence-alias-a", "visible-A");
                Map<?, ?> secondSource = ordinaryRetentionSource(user, "sequence-alias-b", "visible-B");
                assertEquals(Set.of("@timestamp", "key", "phase", "payload", "number"), firstSource.keySet());
                assertFalse(firstSource.containsKey("secret"));
                assertFalse(firstSource.containsKey("tenant"));
                assertEquals("secret-B", secondSource.get("secret"));
                assertEquals("allowed", secondSource.get("tenant"));
                for (int primary : List.of(1, 2)) {
                    Map<String, Object> result = retentionQuery(user, """
                        SEQUENCE BY key WITHIN 30s PRIMARY %s
                        (FROM sequence-alias-a METADATA _id,_index,_source | KEEP key,_id,_index,_source),
                        (FROM sequence-alias-b METADATA _id,_index,_source | KEEP key,_id,_index,_source)
                        | EVAL other_id=MEMBER(%s,_id) | KEEP key,_source,_id,other_id,sequence_provenance | LIMIT 10
                        """.formatted(primary, 3 - primary));
                    assertProvenancePair(result);
                    assertEquals(List.of(primary == 1 ? firstSource : secondSource), ordinaryColumnValues(result, "_source"));
                    List<?> row = requireType(List.class, requireType(List.class, result.get("values")).getFirst());
                    assertEquals("shared", row.get(0));
                    assertEquals(primary == 1 ? "visible-A" : "visible-B", row.get(2));
                    assertEquals(primary == 1 ? "visible-B" : "visible-A", row.get(3));
                }
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

    /** Direct late reads must preserve the same restricted values and original sources as ordinary authorized reads. */
    public void testDirectLateFieldsAndSourcesPreserveDocumentAndFieldSecurity() throws Exception {
        createSequenceSecurityEvents(true);
        createSequenceSecurityAliases();
        for (String user : List.of("sequence_dls_fls", "sequence_alias_only")) {
            String first = user.equals("sequence_alias_only") ? "sequence-alias-a" : "sequence-security-events";
            String second = user.equals("sequence_alias_only") ? "sequence-alias-b" : "sequence-security-events";
            Map<?, ?> firstSource = ordinaryRetentionSource(user, first, "visible-A");
            Map<?, ?> secondSource = ordinaryRetentionSource(user, second, "visible-B");
            assertEquals(Set.of("@timestamp", "key", "phase", "payload", "number"), firstSource.keySet());
            assertEquals(firstSource.keySet(), secondSource.keySet());
            assertEquals(9007199254740993L, firstSource.get("number"));
            assertEquals(
                List.of(List.of("visible-A")),
                retentionQuery(user, "FROM " + first + " METADATA _id | WHERE phase==\"A\" | KEEP _id | SORT _id | LIMIT 10").get("values")
            );
            assertEquals(
                List.of(List.of("orphan-B"), List.of("visible-B")),
                retentionQuery(user, "FROM " + second + " METADATA _id | WHERE phase==\"B\" | KEEP _id | SORT _id | LIMIT 10").get("values")
            );
            String stages = """
                SEQUENCE BY key WITHIN 30s PRIMARY %s
                (FROM %s METADATA _source | WHERE phase=="A" | KEEP key,payload,number,_source),
                (FROM %s METADATA _source | WHERE phase=="B" | KEEP key,payload,number,_source)
                | EVAL other_payload=MEMBER(%s,payload), other_number=MEMBER(%s,number)
                | KEEP key,payload,number,other_payload,other_number,_source,sequence_provenance | LIMIT 10
                """;
            for (int primary : List.of(1, 2)) {
                int other = 3 - primary;
                Map<String, Object> result = profiledRetentionQuery(user, stages.formatted(primary, first, second, other, other));
                assertProvenancePair(result);
                Map<?, ?> selected = primary == 1 ? firstSource : secondSource;
                Map<?, ?> member = other == 1 ? firstSource : secondSource;
                List<?> row = requireType(List.class, requireType(List.class, result.get("values")).getFirst());
                assertEquals(
                    List.of("shared", selected.get("payload"), selected.get("number"), member.get("payload"), member.get("number")),
                    row.subList(0, 5)
                );
                assertEquals(List.of(selected), ordinaryColumnValues(result, "_source"));
                assertDirectSequenceProfile(result, 2, 2, 1);
            }
            Map<String, Object> unrestricted = retentionQuery("test-admin", stages.formatted(1, first, second, 2, 2));
            assertEquals(Set.of("hidden-A", "orphan-A"), new java.util.HashSet<>(provenanceRoleValues(unrestricted, 1, "id")));
            for (Object source : ordinaryColumnValues(unrestricted, "_source")) {
                assertNotNull(requireType(Map.class, source).get("secret"));
                assertNotNull(requireType(Map.class, source).get("tenant"));
            }
        }
    }

    /** Already authorized key and physical ID values can be retained without fetching payloads or widening field access. */
    public void testFullyRetainedIdsPreserveSecurityWithoutPayloadSearch() throws Exception {
        createSequenceSecurityEvents(true);
        String query = """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-security-events METADATA _id,_index | WHERE phase=="A" | KEEP key,_id,_index),
            (FROM sequence-security-events METADATA _id,_index | WHERE phase=="B" | KEEP key,_id,_index)
            | EVAL other_id=MEMBER(2,_id), other_index=MEMBER(2,_index)
            | KEEP key,_id,_index,other_id,other_index | LIMIT 10
            """;
        Map<String, Object> restricted = profiledRetentionQuery("sequence_dls_fls", query);
        assertEquals(
            List.of(List.of("shared", "visible-A", "sequence-security-events", "visible-B", "sequence-security-events")),
            restricted.get("values")
        );
        assertDirectSequenceProfile(restricted, 0, 0, 1);
        Map<String, Object> ordinary = retentionQuery("sequence_dls_fls", """
            FROM sequence-security-events METADATA _id
            | KEEP _id | SORT _id | LIMIT 10
            """);
        assertEquals(List.of(List.of("orphan-B"), List.of("visible-A"), List.of("visible-B")), ordinary.get("values"));
        Map<String, Object> unrestricted = profiledRetentionQuery("test-admin", query);
        List<?> rows = requireType(List.class, unrestricted.get("values"));
        assertEquals(
            Set.of("hidden-A", "orphan-A"),
            rows.stream().map(row -> requireType(List.class, row).get(1)).collect(java.util.stream.Collectors.toSet())
        );
        assertDirectSequenceProfile(unrestricted, 0, 0, 2);
        assertRetentionFieldDenied("sequence_dls_fls", "FROM sequence-security-events | KEEP secret | LIMIT 10");
        String forbidden = """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-security-events METADATA _id | WHERE phase=="A" | EVAL _id=secret | KEEP key,_id),
            (FROM sequence-security-events METADATA _id | WHERE phase=="B" | EVAL _id=secret | KEEP key,_id)
            | EVAL other_id=MEMBER(2,_id) | KEEP _id,other_id | LIMIT 10
            """;
        Map<String, Object> allowed = retentionQuery("test-admin", forbidden);
        assertEquals(2, requireType(List.class, allowed.get("values")).size());
        assertRetentionFieldDenied("sequence_dls_fls", forbidden);
    }

    private Map<String, Object> profiledRetentionQuery(String user, String query) throws IOException {
        Request request = new Request("POST", "_query");
        request.addParameter("drop_null_columns", "false");
        request.addParameter("allow_partial_results", "false");
        request.setOptions(runAsUserOptions(user));
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .field("query", query)
                    .field("profile", true)
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

    private static void assertDirectSequenceProfile(Map<String, Object> result, long payloadSearches, long payloadRows, long emittedRows) {
        Map<?, ?> profile = requireType(Map.class, result.get("profile"));
        List<Map<?, ?>> statuses = new ArrayList<>();
        for (Object driver : requireType(List.class, profile.get("drivers"))) {
            for (Object operator : requireType(List.class, requireType(Map.class, driver).get("operators"))) {
                Object status = requireType(Map.class, operator).get("status");
                if (status instanceof Map<?, ?> values && values.containsKey("payload_search_requests")) {
                    statuses.add(values);
                }
            }
        }
        assertEquals("exactly one native sequence source must report its actual work", 1, statuses.size());
        Map<?, ?> status = statuses.getFirst();
        assertEquals(2L, requireType(Number.class, status.get("candidate_search_requests")).longValue());
        assertEquals(
            "only the two candidate stage drivers may run",
            2L,
            requireType(Number.class, status.get("stage_drivers")).longValue()
        );
        assertEquals(payloadSearches, requireType(Number.class, status.get("payload_search_requests")).longValue());
        assertEquals(payloadRows, requireType(Number.class, status.get("payload_rows_fetched")).longValue());
        assertEquals(emittedRows, requireType(Number.class, status.get("rows_emitted")).longValue());
    }

    /** Hidden predecessors must never enter matching, and typed evidence fetches must retain the caller's field permissions. */
    public void testSequenceRetainsDocumentAndFieldSecurityDuringEvidenceReplay() throws Exception {
        createSequenceSecurityEvents();
        String stages = """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
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
            SEQUENCE BY key WITHIN 30s PRIMARY 1
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

    /** A previously authorized PIT must recheck current read grants even when field discovery remains authorized. */
    public void testPitAndSequenceSearchReauthorizeAfterReadGrantRevocation() throws Exception {
        createSequenceSecurityEvents(true);
        createSequenceSecurityAliases();
        String user = "sequence-current-grants";
        Request role = new Request("PUT", "_security/role/" + user);
        role.setJsonEntity("""
            {"indices":[{"names":["sequence-security-events","sequence-alias-a","sequence-alias-b"],
             "privileges":["read","view_index_metadata"]}]}
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
                String query = """
                    SEQUENCE BY key WITHIN 30s PRIMARY 1
                    (FROM sequence-alias-a | WHERE tenant=="allowed"),
                    (FROM sequence-alias-b | WHERE tenant=="allowed")
                    | EVAL other_payload = MEMBER(2, payload) | KEEP key, payload, other_payload | LIMIT 10
                    """;
                Map<String, Object> authorized = entityAsMap(runESQLCommand(user, query));
                assertEquals(List.of(List.of("shared", "visible-A", "visible-B")), authorized.get("values"));
                assertEquals(Boolean.FALSE, authorized.get("is_partial"));
                Request open = new Request("POST", "sequence-alias-a,sequence-alias-b/_pit");
                open.addParameter("keep_alive", "2m");
                open.setOptions(runAsUserOptions(user));
                String pit = requireType(String.class, entityAsMap(assertOK(client().performRequest(open))).get("id"));
                try {
                    Request authorizedSearch = new Request("POST", "_search");
                    authorizedSearch.setOptions(runAsUserOptions(user));
                    authorizedSearch.setJsonEntity(
                        Strings.toString(
                            JsonXContent.contentBuilder().startObject().field("size", 0).field("pit", Map.of("id", pit)).endObject()
                        )
                    );
                    Map<String, Object> searched = entityAsMap(assertOK(client().performRequest(authorizedSearch)));
                    assertEquals(0, requireType(Map.class, searched.get("_shards")).get("failed"));
                    if (searched.get("pit_id") != null) {
                        pit = requireType(String.class, searched.get("pit_id"));
                    }
                    Request revokeRead = new Request("PUT", "_security/role/" + user);
                    revokeRead.setJsonEntity("""
                        {"indices":[{"names":["sequence-security-events","sequence-alias-a","sequence-alias-b"],
                         "privileges":["view_index_metadata"]}]}
                        """);
                    assertOK(adminClient().performRequest(revokeRead));
                    Request fieldCaps = new Request("GET", "sequence-alias-a/_field_caps");
                    fieldCaps.addParameter("fields", "@timestamp,key,phase,payload,tenant");
                    fieldCaps.setOptions(runAsUserOptions(user));
                    Map<?, ?> fields = requireType(Map.class, entityAsMap(assertOK(client().performRequest(fieldCaps))).get("fields"));
                    assertTrue("field discovery must pass before testing search authorization", fields.containsKey("key"));

                    Request search = new Request("POST", "_search");
                    search.setOptions(runAsUserOptions(user));
                    search.setJsonEntity(
                        Strings.toString(
                            JsonXContent.contentBuilder().startObject().field("size", 0).field("pit", Map.of("id", pit)).endObject()
                        )
                    );
                    assertBusy(() -> {
                        ResponseException denied = expectThrows(ResponseException.class, () -> client().performRequest(search));
                        assertEquals(HttpStatus.SC_FORBIDDEN, denied.getResponse().getStatusLine().getStatusCode());
                        assertThat(EntityUtils.toString(denied.getResponse().getEntity()), containsString("indices:data/read/search"));
                    });
                    ResponseException denied = expectThrows(ResponseException.class, () -> runESQLCommand(user, query));
                    assertEquals(HttpStatus.SC_FORBIDDEN, denied.getResponse().getStatusLine().getStatusCode());
                    assertThat(EntityUtils.toString(denied.getResponse().getEntity()), containsString("indices:data/read/"));
                } finally {
                    Request close = new Request("DELETE", "_pit");
                    close.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("id", pit).endObject()));
                    assertEquals(Boolean.TRUE, entityAsMap(assertOK(adminClient().performRequest(close))).get("succeeded"));
                }
            } finally {
                assertOK(adminClient().performRequest(new Request("DELETE", "_security/user/" + user)));
            }
        } finally {
            assertOK(adminClient().performRequest(new Request("DELETE", "_security/role/" + user)));
        }
    }

    /** A branch-local exclusion must also exclude that alias's broader document permissions when opening the shared PIT. */
    public void testSequenceIndexExclusionsDoNotBroadenAliasDocumentPermissions() throws Exception {
        createSequenceSecurityEvents();
        createSequenceSecurityAliases();
        Map<String, Object> allowed = entityAsMap(runESQLCommand("sequence_alias_exclusions", """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
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
            SEQUENCE BY key WITHIN 30s PRIMARY 1
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
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-alias-a), (FROM sequence-alias-b)
            | EVAL member2_payload = MEMBER(2, payload)
            | KEEP key, payload, member2_payload | LIMIT 10
            """));
        assertEquals(List.of(), allowed.get("values"));
    }

    /** An entirely empty authorized alias scope retains its schema and recovers the same authorized matches. */
    public void testNarrowedPitAllowsAllAuthorizedAliasesToBeEmpty() throws Exception {
        createSequenceSecurityEvents(true);
        createSequenceSecurityAliases();
        String user = "sequence_alias_only";
        Map<?, ?> first = ordinaryRetentionSource(user, "sequence-alias-a", "visible-A");
        Map<?, ?> second = ordinaryRetentionSource(user, "sequence-alias-b", "visible-B");
        assertEquals(Set.of("@timestamp", "key", "phase", "payload", "number"), first.keySet());
        assertEquals(first.keySet(), second.keySet());
        setSequenceSecurityAliasesEmpty();
        try {
            Map<String, Object> ordinary = retentionQuery(user, """
                FROM sequence-alias-a,sequence-alias-b | KEEP key,payload | LIMIT 10
                """);
            assertEquals(List.of(), ordinary.get("values"));
            for (int primary : List.of(1, 2)) {
                assertEmptyAliasSequence(retentionQuery(user, emptyAliasSequenceQuery(primary)));
            }
            assertSequenceSearchReadDenied(user, "sequence-security-events");
        } finally {
            createSequenceSecurityAliases();
        }
        for (int primary : List.of(1, 2)) {
            Map<String, Object> recovered = retentionQuery(
                user,
                emptyAliasSequenceQuery(primary).replace("other_payload | LIMIT", "other_payload,sequence_provenance | LIMIT")
            );
            assertProvenancePair(recovered);
            List<?> row = requireType(List.class, requireType(List.class, recovered.get("values")).getFirst());
            assertEquals(
                List.of("shared", primary == 1 ? "visible-A" : "visible-B", primary == 1 ? "visible-B" : "visible-A"),
                row.subList(0, 3)
            );
        }
    }

    /** Zero-hit native work must still enforce current read grants when field discovery remains authorized. */
    public void testNarrowedEmptyPitRechecksCurrentReadPrivileges() throws Exception {
        createSequenceSecurityEvents(true);
        createSequenceSecurityAliases();
        String user = "sequence-empty-current-grants";
        String readRole = """
            {"indices":[{"names":["sequence-alias-a","sequence-alias-b"],
              "privileges":["read","view_index_metadata"],"query":{"term":{"tenant":"allowed"}},
              "field_security":{"grant":["@timestamp","key","phase","payload","number"]}}]}
            """;
        Request role = new Request("PUT", "_security/role/" + user);
        role.setJsonEntity(readRole);
        assertOK(adminClient().performRequest(role));
        try {
            createSequenceSecurityUser(user);
            try {
                setSequenceSecurityAliasesEmpty();
                String query = emptyAliasSequenceQuery(1);
                assertEmptyAliasSequence(retentionQuery(user, query));
                role.setJsonEntity("""
                    {"indices":[{"names":["sequence-alias-a","sequence-alias-b"],
                      "privileges":["view_index_metadata"]}]}
                    """);
                assertOK(adminClient().performRequest(role));
                Request fieldCaps = new Request("GET", "sequence-alias-a,sequence-alias-b/_field_caps");
                fieldCaps.addParameter("fields", "@timestamp,key,phase,payload");
                fieldCaps.setOptions(runAsUserOptions(user));
                Map<?, ?> fields = requireType(Map.class, entityAsMap(assertOK(client().performRequest(fieldCaps))).get("fields"));
                assertTrue("field discovery must remain authorized", fields.containsKey("key"));
                assertBusy(() -> assertSequenceSearchReadDenied(user, "sequence-alias-a"));
                ResponseException denied = expectThrows(ResponseException.class, () -> retentionQuery(user, query));
                assertEquals(HttpStatus.SC_FORBIDDEN, denied.getResponse().getStatusLine().getStatusCode());
                assertThat(EntityUtils.toString(denied.getResponse().getEntity()), containsString("indices:data/read/"));

                role.setJsonEntity(readRole);
                assertOK(adminClient().performRequest(role));
                assertBusy(() -> {
                    try {
                        assertEmptyAliasSequence(retentionQuery(user, query));
                    } catch (ResponseException failure) {
                        if (failure.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_FORBIDDEN) {
                            assertThat(EntityUtils.toString(failure.getResponse().getEntity()), containsString("indices:data/read/"));
                            throw new AssertionError("restored read grant must become visible", failure);
                        }
                        throw failure;
                    }
                });
                createSequenceSecurityAliases();
                Map<?, ?> first = ordinaryRetentionSource(user, "sequence-alias-a", "visible-A");
                Map<?, ?> second = ordinaryRetentionSource(user, "sequence-alias-b", "visible-B");
                assertEquals(Set.of("@timestamp", "key", "phase", "payload", "number"), first.keySet());
                assertEquals(first.keySet(), second.keySet());
                assertProvenancePair(
                    retentionQuery(user, query.replace("other_payload | LIMIT", "other_payload,sequence_provenance | LIMIT"))
                );
            } finally {
                try {
                    createSequenceSecurityAliases();
                } finally {
                    assertOK(adminClient().performRequest(new Request("DELETE", "_security/user/" + user)));
                }
            }
        } finally {
            assertOK(adminClient().performRequest(new Request("DELETE", "_security/role/" + user)));
        }
    }

    /** Routed duplicate IDs and broader excluded grants preserve each stage's identity, values and field mask. */
    public void testNarrowedPitKeepsRoutedAliasAndExcludedGrantsIndependent() throws Exception {
        String index = "sequence-security-routed-events";
        List<String> routes = createRoutedSequenceSecurityEvents(index);
        String user = "sequence-pit-routed-grants";
        Request role = new Request("PUT", "_security/role/" + user);
        role.setJsonEntity("""
            {"indices":[
              {"names":["sequence-pit-routed-a"],"privileges":["read","view_index_metadata"],
               "query":{"term":{"tenant":"allowed"}},
               "field_security":{"grant":["@timestamp","key","phase","payload","number"]}},
              {"names":["sequence-pit-routed-b","sequence-pit-routed-excluded"],
               "privileges":["read","view_index_metadata"],"field_security":{"grant":["*"]}}
            ]}
            """);
        assertOK(adminClient().performRequest(role));
        try {
            createSequenceSecurityUser(user);
            try {
                assertEquals(
                    sequenceSecurityRoutingShard("test-admin", index, routes.get(0)),
                    sequenceSecurityRoutingShard(user, "sequence-pit-routed-a", null)
                );
                assertEquals(
                    sequenceSecurityRoutingShard("test-admin", index, routes.get(1)),
                    sequenceSecurityRoutingShard(user, "sequence-pit-routed-b", null)
                );
                Map<String, Object> duplicateIds = retentionQuery("test-admin", """
                    FROM sequence-security-routed-events METADATA _id
                    | WHERE _id=="visible-A" | KEEP _id,payload | SORT payload | LIMIT 10
                    """);
                assertEquals(List.of(List.of("visible-A", "routed-away-A"), List.of("visible-A", "visible-A")), duplicateIds.get("values"));
                assertEquals(List.of(List.of("visible-A")), retentionQuery(user, """
                    FROM sequence-pit-routed-a | KEEP payload | LIMIT 10
                    """).get("values"));
                assertEquals(List.of(List.of("visible-B")), retentionQuery(user, """
                    FROM sequence-pit-routed-b | KEEP payload | LIMIT 10
                    """).get("values"));
                Map<String, Object> broad = retentionQuery(user, """
                    FROM sequence-pit-routed-excluded | KEEP payload | SORT payload | LIMIT 10
                    """);
                assertEquals(
                    List.of(
                        List.of("hidden-A"),
                        List.of("routed-away-A"),
                        List.of("routed-away-B"),
                        List.of("visible-A"),
                        List.of("visible-B")
                    ),
                    broad.get("values")
                );
                Map<?, ?> first = ordinaryRetentionSource(user, "sequence-pit-routed-a", "visible-A");
                Map<?, ?> second = ordinaryRetentionSource(user, "sequence-pit-routed-b", "visible-B");
                assertEquals(Set.of("@timestamp", "key", "phase", "payload", "number"), first.keySet());
                assertEquals(9007199254740993L, first.get("number"));
                assertEquals("secret-B", second.get("secret"));
                assertEquals("allowed", second.get("tenant"));
                assertRetentionFieldDenied(user, "FROM sequence-pit-routed-a | KEEP secret | LIMIT 10");
                assertSequenceSearchReadDenied(user, index);
                for (int primary : List.of(1, 2)) {
                    String query = """
                        SEQUENCE BY key WITHIN 30s PRIMARY %s
                        (FROM sequence-pit-routed-*,-sequence-pit-routed-b,-sequence-pit-routed-excluded),
                        (FROM sequence-pit-routed-b,sequence-pit-routed-excluded | WHERE phase=="B")
                        | EVAL other_payload=MEMBER(%s,payload), broad_secret=%s
                        | KEEP key,payload,other_payload,broad_secret,sequence_provenance | LIMIT 10
                        """.formatted(primary, 3 - primary, primary == 2 ? "secret" : "MEMBER(2,secret)");
                    Map<String, Object> result = profiledRetentionQuery(user, query);
                    assertEquals(List.of("visible-A"), provenanceRoleValues(result, 1, "id"));
                    assertEquals(List.of("visible-B"), provenanceRoleValues(result, 2, "id"));
                    for (int stage : List.of(1, 2)) {
                        assertEquals(List.of(index), provenanceRoleValues(result, stage, "index"));
                    }
                    List<?> row = requireType(List.class, requireType(List.class, result.get("values")).getFirst());
                    assertEquals(
                        List.of("routed", primary == 1 ? "visible-A" : "visible-B", primary == 1 ? "visible-B" : "visible-A", "secret-B"),
                        row.subList(0, 4)
                    );
                    assertDirectSequenceProfile(result, 2, 2, 1);
                    Map<String, Object> ids = profiledRetentionQuery(user, """
                        SEQUENCE BY key WITHIN 30s PRIMARY %s
                        (FROM sequence-pit-routed-*,-sequence-pit-routed-b,-sequence-pit-routed-excluded
                         METADATA _id,_index | KEEP key,_id,_index),
                        (FROM sequence-pit-routed-b,sequence-pit-routed-excluded METADATA _id,_index
                         | WHERE phase=="B" | KEEP key,_id,_index)
                        | EVAL other_id=MEMBER(%s,_id), other_index=MEMBER(%s,_index)
                        | KEEP key,_id,_index,other_id,other_index | LIMIT 10
                        """.formatted(primary, 3 - primary, 3 - primary));
                    assertEquals(
                        List.of(
                            List.of(
                                "routed",
                                primary == 1 ? "visible-A" : "visible-B",
                                index,
                                primary == 1 ? "visible-B" : "visible-A",
                                index
                            )
                        ),
                        ids.get("values")
                    );
                    assertDirectSequenceProfile(ids, 0, 0, 1);
                }
            } finally {
                assertOK(adminClient().performRequest(new Request("DELETE", "_security/user/" + user)));
            }
        } finally {
            assertOK(adminClient().performRequest(new Request("DELETE", "_security/role/" + user)));
        }
    }

    private static String emptyAliasSequenceQuery(int primary) {
        return """
            SEQUENCE BY key WITHIN 30s PRIMARY %s
            (FROM sequence-alias-a), (FROM sequence-alias-b)
            | EVAL other_payload=MEMBER(%s,payload) | KEEP key,payload,other_payload | LIMIT 10
            """.formatted(primary, 3 - primary);
    }

    private static void assertEmptyAliasSequence(Map<String, Object> result) {
        assertEquals(Boolean.FALSE, result.get("is_partial"));
        assertEquals(List.of(), result.get("values"));
        List<?> columns = requireType(List.class, result.get("columns"));
        assertEquals(List.of("key", "payload", "other_payload"), columns.stream().map(c -> requireType(Map.class, c).get("name")).toList());
        assertEquals(List.of("keyword", "keyword", "keyword"), columns.stream().map(c -> requireType(Map.class, c).get("type")).toList());
    }

    private void setSequenceSecurityAliasesEmpty() throws IOException {
        Request aliases = new Request("POST", "_aliases");
        aliases.setJsonEntity("""
            {"actions":[
              {"add":{"index":"sequence-security-events","alias":"sequence-alias-a","filter":{"match_none":{}}}},
              {"add":{"index":"sequence-security-events","alias":"sequence-alias-b","filter":{"match_none":{}}}}
            ]}
            """);
        assertOK(adminClient().performRequest(aliases));
    }

    private void createSequenceSecurityUser(String user) throws IOException {
        Request request = new Request("PUT", "_security/user/" + user);
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .field("roles", List.of(user))
                    .field("password", randomAlphaOfLength(32))
                    .endObject()
            )
        );
        assertOK(adminClient().performRequest(request));
    }

    private void assertSequenceSearchReadDenied(String user, String expression) throws IOException {
        Request search = new Request("GET", expression + "/_search");
        search.setOptions(runAsUserOptions(user));
        ResponseException denied = expectThrows(ResponseException.class, () -> client().performRequest(search));
        assertEquals(HttpStatus.SC_FORBIDDEN, denied.getResponse().getStatusLine().getStatusCode());
        assertThat(EntityUtils.toString(denied.getResponse().getEntity()), containsString("indices:data/read/search"));
    }

    /** Real routing resolution prevents a duplicate-ID control from accidentally landing both copies on one shard. */
    private List<String> createRoutedSequenceSecurityEvents(String index) throws IOException {
        Request create = new Request("PUT", index);
        create.setJsonEntity("""
            {"settings":{"number_of_shards":2,"number_of_replicas":0,"routing.allocation.total_shards_per_node":1},
             "mappings":{"properties":{
               "@timestamp":{"type":"date_nanos"},"key":{"type":"keyword"},"phase":{"type":"keyword"},
               "payload":{"type":"keyword"},"tenant":{"type":"keyword"},"secret":{"type":"keyword"},"number":{"type":"long"}}}}
            """);
        assertOK(adminClient().performRequest(create));
        String first = "route-0";
        int firstShard = sequenceSecurityRoutingShard("test-admin", index, first);
        String second = null;
        for (int i = 1; i <= 32; i++) {
            String candidate = "route-" + i;
            if (sequenceSecurityRoutingShard("test-admin", index, candidate) != firstShard) {
                second = candidate;
                break;
            }
        }
        assertNotNull("bounded real routing controls must reach distinct physical shards", second);
        indexRoutedSequenceSecurityEvent(index, first, "visible-A", "A", "visible-A", "allowed", "secret-A", 0, 9007199254740993L);
        indexRoutedSequenceSecurityEvent(index, second, "visible-A", "A", "routed-away-A", "allowed", "away-A", 1, 11);
        indexRoutedSequenceSecurityEvent(index, first, "hidden-A", "A", "hidden-A", "hidden", "hidden-secret", 2, 12);
        indexRoutedSequenceSecurityEvent(index, second, "visible-B", "B", "visible-B", "allowed", "secret-B", 3, 8);
        indexRoutedSequenceSecurityEvent(index, first, "visible-B", "B", "routed-away-B", "allowed", "away-B", 4, 14);
        assertOK(adminClient().performRequest(new Request("POST", index + "/_refresh")));
        Request aliases = new Request("POST", "_aliases");
        aliases.setJsonEntity("""
            {"actions":[
              {"add":{"index":"%s","alias":"sequence-pit-routed-a","search_routing":"%s","filter":{"term":{"phase":"A"}}}},
              {"add":{"index":"%s","alias":"sequence-pit-routed-b","search_routing":"%s","filter":{"term":{"phase":"B"}}}},
              {"add":{"index":"%s","alias":"sequence-pit-routed-excluded","filter":{"match_all":{}}}}
            ]}
            """.formatted(index, first, index, second, index));
        assertOK(adminClient().performRequest(aliases));
        return List.of(first, second);
    }

    private void indexRoutedSequenceSecurityEvent(
        String index,
        String routing,
        String id,
        String phase,
        String payload,
        String tenant,
        String secret,
        int nanos,
        long number
    ) throws IOException {
        Request request = new Request("PUT", index + "/_doc/" + id);
        request.addParameter("routing", routing);
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .field("@timestamp", "2026-09-25T21:14:51.38900000" + nanos + "Z")
                    .field("key", "routed")
                    .field("phase", phase)
                    .field("payload", payload)
                    .field("tenant", tenant)
                    .field("secret", secret)
                    .field("number", number)
                    .endObject()
            )
        );
        assertOK(adminClient().performRequest(request));
    }

    private int sequenceSecurityRoutingShard(String user, String expression, String routing) throws IOException {
        Request request = new Request("GET", expression + "/_search_shards");
        request.setOptions(runAsUserOptions(user));
        if (routing != null) {
            request.addParameter("routing", routing);
        }
        List<?> groups = requireType(List.class, entityAsMap(assertOK(client().performRequest(request))).get("shards"));
        assertEquals("a routed source must identify one physical shard", 1, groups.size());
        List<?> copies = requireType(List.class, groups.getFirst());
        assertFalse(copies.isEmpty());
        return requireType(Number.class, requireType(Map.class, copies.getFirst()).get("shard")).intValue();
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

    /** A role naming only the data stream must read matched originals from all of its backing indices. */
    public void testSequenceDataStreamNameGrantsAcrossRollover() throws Exception {
        assertSequenceDataStreamNameGrants(false);
    }

    /** Parent data-stream grants retain DLS/FLS for matching and explicit ordinary SOURCE output. */
    public void testSequenceDataStreamNameGrantsPreserveDlsFls() throws Exception {
        assertSequenceDataStreamNameGrants(true);
    }

    private void assertSequenceDataStreamNameGrants(boolean filtered) throws Exception {
        String stream = "sequence-security-stream";
        String deniedIndex = "sequence-stream-denied-events";
        String user = filtered ? "sequence-stream-filtered-reader" : "sequence-stream-reader";
        List<String> backings = createSequenceDataStreamEvents(stream, deniedIndex, filtered);
        try {
            Request role = new Request("PUT", "_security/role/" + user);
            role.setJsonEntity("""
                {"indices":[{"names":["%s"],"privileges":["read"]%s}]}
                """.formatted(stream, filtered ? """
                ,"query":{"term":{"tenant":"allowed"}},
                "field_security":{"grant":["@timestamp","key","phase","payload","number"]}
                """ : ""));
            assertOK(adminClient().performRequest(role));
            try {
                createSequenceSecurityUser(user);
                try {
                    Map<?, ?> savedRole = requireType(
                        Map.class,
                        entityAsMap(adminClient().performRequest(new Request("GET", "_security/role/" + user))).get(user)
                    );
                    List<?> grants = requireType(List.class, savedRole.get("indices"));
                    assertEquals(1, grants.size());
                    assertEquals(List.of(stream), requireType(Map.class, grants.getFirst()).get("names"));

                    if (filtered) {
                        Map<String, Object> admin = retentionQuery("test-admin", sequenceDataStreamStages(stream, 1) + """
                            | EVAL end_payload=MEMBER(2,payload) | KEEP key,payload,end_payload | SORT key | LIMIT 10
                            """);
                        assertEquals(
                            List.of(
                                List.of("new", "new-A", "new-B"),
                                List.of("old", "hidden-A", "old-B"),
                                List.of("orphan", "hidden-orphan-A", "orphan-B")
                            ),
                            admin.get("values")
                        );
                    }
                    for (int primary : List.of(1, 2)) {
                        int other = primary == 1 ? 2 : 1;
                        String stages = sequenceDataStreamStages(stream, primary);
                        Map<String, Object> result = retentionQuery(user, stages + """
                            | EVAL other_payload=MEMBER(%s,payload), other_id=MEMBER(%s,_id), other_index=MEMBER(%s,_index)
                            | KEEP key,payload,_id,_index,other_payload,other_id,other_index | SORT key | LIMIT 10
                            """.formatted(other, other, other));
                        assertEquals(
                            List.of(
                                sequenceDataStreamRow("new", "new-A", backings.get(1), "new-B", backings.get(2), primary),
                                sequenceDataStreamRow("old", "old-A", backings.get(0), "old-B", backings.get(1), primary)
                            ),
                            result.get("values")
                        );

                        Map<String, Object> originals = retentionQuery(
                            user,
                            stages + " | KEEP key,_source,sequence_provenance | SORT key | LIMIT 10"
                        );
                        List<?> retainedRows = requireType(List.class, originals.get("values"));
                        assertEquals(
                            List.of("new", "old"),
                            retainedRows.stream().map(row -> requireType(List.class, row).getFirst()).toList()
                        );
                        assertEquals(List.of("shared-start", "shared-start"), provenanceRoleValues(originals, 1, "id"));
                        assertEquals(List.of("shared-end", "shared-end"), provenanceRoleValues(originals, 2, "id"));
                        assertEquals(List.of(backings.get(1), backings.get(0)), provenanceRoleValues(originals, 1, "index"));
                        assertEquals(List.of(backings.get(2), backings.get(1)), provenanceRoleValues(originals, 2, "index"));
                        assertEquals(
                            primary == 1
                                ? List.of(
                                    sequenceDataStreamSource("new", "A", "new-A", "allowed", 10_000, filtered),
                                    sequenceDataStreamSource("old", "A", "old-A", "allowed", 0, filtered)
                                )
                                : List.of(
                                    sequenceDataStreamSource("new", "B", "new-B", "allowed", 11_000, filtered),
                                    sequenceDataStreamSource("old", "B", "old-B", "allowed", 1_000, filtered)
                                ),
                            ordinaryColumnValues(originals, "_source")
                        );
                        if (filtered) {
                            assertRetentionFieldDenied(
                                user,
                                stages + " | EVAL hidden=MEMBER(" + other + ",secret) | KEEP hidden | LIMIT 10"
                            );
                        }
                    }
                    // Backing-index access is inherited from a data-stream grant. An unrelated existing index is the denial control.
                    assertOK(adminClient().performRequest(new Request("GET", deniedIndex + "/_search")));
                    assertSequenceSearchReadDenied(user, deniedIndex);
                    ResponseException denied = expectThrows(ResponseException.class, () -> retentionQuery(user, """
                        SEQUENCE BY key WITHIN 30s PRIMARY 1
                        (FROM %s | WHERE phase=="A"), (FROM %s | WHERE phase=="B")
                        | KEEP payload | LIMIT 10
                        """.formatted(stream, deniedIndex)));
                    assertEquals(HttpStatus.SC_BAD_REQUEST, denied.getResponse().getStatusLine().getStatusCode());
                    assertThat(
                        EntityUtils.toString(denied.getResponse().getEntity()),
                        containsString("Unknown index [" + deniedIndex + "]")
                    );
                } finally {
                    assertOK(adminClient().performRequest(new Request("DELETE", "_security/user/" + user)));
                }
            } finally {
                assertOK(adminClient().performRequest(new Request("DELETE", "_security/role/" + user)));
            }
        } finally {
            try {
                // Check before deleting backing indices, since deleting them could hide a leaked PIT reader.
                assertRetentionResourcesReleased();
                assertBusy(() -> {
                    Request tasks = new Request("GET", "_tasks");
                    tasks.addParameter("actions", "indices:data/read/esql*,indices:data/read/search*");
                    Map<?, ?> nodes = requireType(Map.class, entityAsMap(adminClient().performRequest(tasks)).get("nodes"));
                    assertTrue(nodes.toString(), nodes.isEmpty());
                });
            } finally {
                assertOK(adminClient().performRequest(new Request("DELETE", "_data_stream/" + stream)));
                assertOK(adminClient().performRequest(new Request("DELETE", deniedIndex)));
                assertOK(adminClient().performRequest(new Request("DELETE", "_index_template/" + stream + "-template")));
            }
        }
    }

    private static String sequenceDataStreamStages(String stream, int primary) {
        return """
            SEQUENCE BY key WITHIN 30s PRIMARY %s
            (FROM %s METADATA _id,_index,_source | WHERE phase=="A"),
            (FROM %s METADATA _id,_index,_source | WHERE phase=="B")
            """.formatted(primary, stream, stream);
    }

    private static List<Object> sequenceDataStreamRow(
        String key,
        String startPayload,
        String startIndex,
        String endPayload,
        String endIndex,
        int primary
    ) {
        return primary == 1
            ? List.of(key, startPayload, "shared-start", startIndex, endPayload, "shared-end", endIndex)
            : List.of(key, endPayload, "shared-end", endIndex, startPayload, "shared-start", startIndex);
    }

    private static Map<String, Object> sequenceDataStreamSource(
        String key,
        String phase,
        String payload,
        String tenant,
        int millis,
        boolean filtered
    ) {
        Map<String, Object> source = new HashMap<>(
            Map.of(
                "@timestamp",
                Instant.parse("2026-09-25T21:14:51.389Z").plusMillis(millis).toString(),
                "key",
                key,
                "phase",
                phase,
                "payload",
                payload,
                "number",
                9007199254740993L
            )
        );
        if (filtered == false) {
            source.put("tenant", tenant);
            source.put("secret", payload + "-secret");
        }
        return source;
    }

    private List<String> createSequenceDataStreamEvents(String stream, String deniedIndex, boolean hiddenEvents) throws IOException {
        Request template = new Request("PUT", "_index_template/" + stream + "-template");
        template.setJsonEntity("""
            {"index_patterns":["%s"],"data_stream":{},"template":{
              "settings":{"index.mode":"standard","number_of_shards":2,"number_of_replicas":0,
                "routing.allocation.total_shards_per_node":1},
              "mappings":{"dynamic":"strict","properties":{
                "@timestamp":{"type":"date_nanos"},"key":{"type":"keyword"},"phase":{"type":"keyword"},
                "payload":{"type":"keyword"},"tenant":{"type":"keyword"},"secret":{"type":"keyword"},"number":{"type":"long"}}}}}
            """.formatted(stream));
        assertOK(adminClient().performRequest(template));
        assertOK(adminClient().performRequest(new Request("PUT", "_data_stream/" + stream)));
        indexSequenceDataStreamEvent(stream, "shared-start", "old", "A", "old-A", "allowed", 0);
        if (hiddenEvents) {
            indexSequenceDataStreamEvent(stream, "hidden-A", "old", "A", "hidden-A", "hidden", 500);
            indexSequenceDataStreamEvent(stream, "hidden-orphan-A", "orphan", "A", "hidden-orphan-A", "hidden", 12_000);
        }
        Map<String, Object> second = entityAsMap(assertOK(adminClient().performRequest(new Request("POST", stream + "/_rollover"))));
        assertEquals(Boolean.TRUE, second.get("rolled_over"));
        indexSequenceDataStreamEvent(stream, "shared-end", "old", "B", "old-B", "allowed", 1_000);
        indexSequenceDataStreamEvent(stream, "shared-start", "new", "A", "new-A", "allowed", 10_000);
        Map<String, Object> third = entityAsMap(assertOK(adminClient().performRequest(new Request("POST", stream + "/_rollover"))));
        assertEquals(Boolean.TRUE, third.get("rolled_over"));
        indexSequenceDataStreamEvent(stream, "shared-end", "new", "B", "new-B", "allowed", 11_000);
        if (hiddenEvents) {
            indexSequenceDataStreamEvent(stream, "orphan-B", "orphan", "B", "orphan-B", "allowed", 13_000);
        }
        assertOK(adminClient().performRequest(new Request("POST", stream + "/_refresh")));
        Map<String, Object> data = entityAsMap(assertOK(adminClient().performRequest(new Request("GET", "_data_stream/" + stream))));
        List<?> actual = requireType(
            List.class,
            requireType(Map.class, requireType(List.class, data.get("data_streams")).getFirst()).get("indices")
        );
        List<String> backings = actual.stream()
            .map(index -> requireType(String.class, requireType(Map.class, index).get("index_name")))
            .toList();
        assertEquals(3, backings.size());
        assertEquals(second.get("old_index"), backings.get(0));
        assertEquals(second.get("new_index"), backings.get(1));
        assertEquals(third.get("new_index"), backings.get(2));

        Request denied = new Request("PUT", deniedIndex);
        denied.setJsonEntity("""
            {"mappings":{"properties":{"@timestamp":{"type":"date_nanos"},"key":{"type":"keyword"},
              "phase":{"type":"keyword"},"payload":{"type":"keyword"},"tenant":{"type":"keyword"},
              "secret":{"type":"keyword"},"number":{"type":"long"}}}}
            """);
        assertOK(adminClient().performRequest(denied));
        indexSequenceDataStreamEvent(deniedIndex, "denied-B", "old", "B", "denied-B", "allowed", 2_000);
        assertOK(adminClient().performRequest(new Request("POST", deniedIndex + "/_refresh")));
        return backings;
    }

    private void indexSequenceDataStreamEvent(String stream, String id, String key, String phase, String payload, String tenant, int millis)
        throws IOException {
        Request event = new Request("PUT", stream + "/_create/" + id);
        event.setJsonEntity(
            Strings.toString(JsonXContent.contentBuilder().map(sequenceDataStreamSource(key, phase, payload, tenant, millis, false)))
        );
        assertOK(adminClient().performRequest(event));
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
