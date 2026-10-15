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

    /** Original source metadata retains DLS/FLS after stage and downstream projection. */
    public void testStageSourceRetainsSecurityThroughProjection() throws Exception {
        assumeTrue("requires native SEQUENCE", hasCapabilities(adminClient(), List.of("sequence_command")));
        createSequenceSecurityEvents(true);
        Map<String, Object> result = entityAsMap(runESQLCommand("sequence_dls_fls", """
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM sequence-security-events METADATA _id, _index, _source | WHERE phase=="A" | KEEP key, _id, _index, _source),
            (FROM sequence-security-events METADATA _id, _index, _source | WHERE phase=="B" | KEEP key, _id, _index, _source)
            | KEEP key, stage1._id, stage1._index, stage1._source, stage2._id, stage2._index, stage2._source | LIMIT 10
            """));
        List<?> row = singleSequenceRow(result);
        assertEquals("visible-A", row.get(1));
        assertEquals("visible-B", row.get(4));
        assertEquals("sequence-security-events", row.get(2));
        assertEquals("sequence-security-events", row.get(5));
        Map<?, ?> firstSource = requireType(Map.class, row.get(3));
        Map<?, ?> secondSource = requireType(Map.class, row.get(6));
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
        assumeTrue("requires native SEQUENCE", hasCapabilities(adminClient(), List.of("sequence_command")));
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
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM sequence-security-events METADATA _id, _index, _source | WHERE phase=="A"),
            (FROM sequence-security-events METADATA _id, _index, _source | WHERE phase=="B")
            """;
        Map<String, Object> columns = entityAsMap(runESQLCommand("sequence_dls_fls", stages + """
            | KEEP key, stage1._id, stage1._index, stage1._source, stage2._id, stage2._index, stage2._source | LIMIT 10
            """));
        assertEquals(
            List.of(
                List.of(
                    "shared",
                    "visible-A",
                    "sequence-security-events",
                    firstSource,
                    "visible-B",
                    "sequence-security-events",
                    secondSource
                )
            ),
            columns.get("values")
        );
    }

    /** CSV acceptance must match ordinary source rendering; it is a display format rather than JSON evidence. */
    public void testStageSourceCsvMatchesOrdinarySourceRendering() throws Exception {
        assumeTrue("requires native SEQUENCE", hasCapabilities(adminClient(), List.of("sequence_command")));
        createSequenceSecurityEvents(true);
        String ordinary = runSourceCsv("""
            FROM sequence-security-events METADATA _id, _source
            | WHERE _id == "visible-A" | KEEP _source | LIMIT 10
            """);
        String sequence = runSourceCsv("""
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM sequence-security-events METADATA _source | WHERE phase=="A"),
            (FROM sequence-security-events METADATA _source | WHERE phase=="B")
            | KEEP stage1._source | RENAME stage1._source AS _source | LIMIT 10
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
        assumeTrue("requires native SEQUENCE", hasCapabilities(adminClient(), List.of("sequence_command")));
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
                Map<String, Object> result = entityAsMap(runESQLCommand(user, """
                    SEQUENCE BY key WITH {"maxspan":"30s"}
                    (FROM sequence-alias-a METADATA _id, _index, _source | KEEP key, _id, _index, _source),
                    (FROM sequence-alias-b METADATA _id, _index, _source | KEEP key, _id, _index, _source)
                    | KEEP key, stage1._id, stage1._index, stage1._source, stage2._id, stage2._index, stage2._source | LIMIT 10
                    """));
                List<?> row = singleSequenceRow(result);
                assertEquals("visible-A", row.get(1));
                assertEquals("visible-B", row.get(4));
                assertEquals("sequence-security-events", row.get(2));
                assertEquals("sequence-security-events", row.get(5));
                Map<?, ?> firstSource = requireType(Map.class, row.get(3));
                Map<?, ?> secondSource = requireType(Map.class, row.get(6));
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

    private static List<?> singleSequenceRow(Map<String, Object> result) {
        List<?> rows = requireType(List.class, result.get("values"));
        assertEquals("hidden predecessors and orphan keys must not complete extra chains", 1, rows.size());
        List<?> row = requireType(List.class, rows.getFirst());
        assertEquals(7, row.size());
        assertEquals("shared", row.getFirst());
        return row;
    }

    private static <T> T requireType(Class<T> type, Object value) {
        assertTrue("expected " + type.getSimpleName(), type.isInstance(value));
        return type.cast(value);
    }

    /** Hidden predecessors must never enter matching, and typed evidence fetches must retain the caller's field permissions. */
    public void testSequenceRetainsDocumentAndFieldSecurityDuringEvidenceReplay() throws Exception {
        assumeTrue("requires native SEQUENCE", hasCapabilities(adminClient(), List.of("sequence_command")));
        createSequenceSecurityEvents();
        String stages = """
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM sequence-security-events | WHERE phase=="A"),
            (FROM sequence-security-events | WHERE phase=="B")
            """;
        Map<String, Object> allowed = entityAsMap(
            runESQLCommand("sequence_dls_fls", stages + " | KEEP key, stage1.payload, stage1.number, stage2.payload | LIMIT 10")
        );
        assertEquals(List.of(List.of("shared", "visible-A", 9007199254740993L, "visible-B")), allowed.get("values"));
        Map<String, Object> unrestricted = entityAsMap(runESQLCommand("test-admin", stages + " | STATS count(*) | LIMIT 1"));
        assertEquals(List.of(List.of(2)), unrestricted.get("values"));
        ResponseException denied = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("sequence_dls_fls", stages + " | KEEP stage1.secret | LIMIT 1")
        );
        assertEquals(HttpStatus.SC_BAD_REQUEST, denied.getResponse().getStatusLine().getStatusCode());
        assertThat(EntityUtils.toString(denied.getResponse().getEntity()), containsString("Unknown column [stage1.secret]"));
    }

    /** Alias grants must authorize every PIT page without granting access to its concrete index. */
    public void testSequenceRetainsAliasOnlyDocumentAndFieldPermissions() throws Exception {
        assumeTrue("requires native SEQUENCE", hasCapabilities(adminClient(), List.of("sequence_command")));
        createSequenceSecurityEvents();
        createSequenceSecurityAliases();
        String stages = """
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM sequence-alias-a),
            (FROM sequence-alias-*,-sequence-alias-a)
            """;
        Map<String, Object> allowed = entityAsMap(
            runESQLCommand("sequence_alias_only", stages + " | KEEP key, stage1.payload, stage1.number, stage2.payload | LIMIT 10")
        );
        assertEquals(List.of(List.of("shared", "visible-A", 9007199254740993L, "visible-B")), allowed.get("values"));
        ResponseException denied = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("sequence_alias_only", stages + " | KEEP stage1.secret | LIMIT 1")
        );
        assertEquals(HttpStatus.SC_BAD_REQUEST, denied.getResponse().getStatusLine().getStatusCode());
        assertThat(EntityUtils.toString(denied.getResponse().getEntity()), containsString("Unknown column [stage1.secret]"));
        Request direct = new Request("GET", "sequence-security-events/_search");
        direct.setOptions(runAsUserOptions("sequence_alias_only"));
        ResponseException forbidden = expectThrows(ResponseException.class, () -> client().performRequest(direct));
        assertEquals(HttpStatus.SC_FORBIDDEN, forbidden.getResponse().getStatusLine().getStatusCode());
    }

    /** A branch-local exclusion must also exclude that alias's broader document permissions when opening the shared PIT. */
    public void testSequenceIndexExclusionsDoNotBroadenAliasDocumentPermissions() throws Exception {
        assumeTrue("requires native SEQUENCE", hasCapabilities(adminClient(), List.of("sequence_command")));
        createSequenceSecurityEvents();
        createSequenceSecurityAliases();
        Map<String, Object> allowed = entityAsMap(runESQLCommand("sequence_alias_exclusions", """
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM sequence-alias-*,-sequence-alias-b,-sequence-alias-excluded),
            (FROM sequence-alias-b)
            | KEEP key, stage1.payload, stage1.number, stage2.payload | LIMIT 10
            """));
        assertEquals(List.of(List.of("shared", "visible-A", 9007199254740993L, "visible-B")), allowed.get("values"));
    }

    /** Each stage must retain its own document grants even when another stage uses a broader alias on the same index. */
    public void testSequenceStagesRetainIndependentAliasDocumentPermissions() throws Exception {
        assumeTrue("requires native SEQUENCE", hasCapabilities(adminClient(), List.of("sequence_command")));
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
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM sequence-alias-a),
            (FROM sequence-alias-b,sequence-alias-excluded | WHERE phase=="B")
            | KEEP key, stage1.payload, stage1.number, stage2.payload | LIMIT 10
            """));
        assertEquals(List.of(List.of("shared", "visible-A", 9007199254740993L, "visible-B")), allowed.get("values"));
    }

    /** PIT can-match may omit an existing source index when its alias is empty; that stage still yields zero matches. */
    public void testSequenceAllowsAuthorizedSourceOmittedFromSnapshot() throws Exception {
        assumeTrue("requires native SEQUENCE", hasCapabilities(adminClient(), List.of("sequence_command")));
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
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM sequence-alias-a), (FROM sequence-alias-b)
            | KEEP key, stage1.payload, stage2.payload | LIMIT 10
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
