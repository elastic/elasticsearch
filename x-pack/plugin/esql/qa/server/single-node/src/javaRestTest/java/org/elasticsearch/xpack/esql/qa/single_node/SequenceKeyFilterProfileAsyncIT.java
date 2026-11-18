/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.SEQUENCE_COMMAND;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SequenceKeyFilterProfileAsyncIT extends ESRestTestCase {
    private static final String PRIVATE_HEADER = "X-Elastic-Internal-Esql-Sequence-Key-Filter";
    private static final String INDEX = "sequence-async-profile";
    private static final String QUERY = """
        SEQUENCE BY key WITHIN 30s PRIMARY 2
          (FROM sequence-async-profile | WHERE role == "start"),
          (FROM sequence-async-profile METADATA _id | WHERE role == "end")
        | EVAL invalid = TO_LONG(_id)
        | KEEP _id,invalid
        | LIMIT 10
        """;

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testStoredAsyncGetPreservesProfileAndWarnings() throws Exception {
        assumeTrue("requires native SEQUENCE", hasCapabilities(adminClient(), List.of(SEQUENCE_COMMAND.capabilityName())));
        seed();
        var sync = query(
            "/_query",
            Map.of("query", QUERY, "profile", true, "accept_pragma_risks", true, "pragma", Map.of("sequence_key_filter", true))
        );
        Map<?, ?> expected = checkProfile(sync);
        var submit = query(
            "/_query/async",
            Map.of(
                "query",
                QUERY,
                "profile",
                true,
                "accept_pragma_risks",
                true,
                "pragma",
                Map.of("sequence_key_filter", true),
                "keep_on_completion",
                true,
                "wait_for_completion_timeout",
                "30s"
            )
        );
        String id = submit.getHeader("X-Elasticsearch-Async-Id");
        assertNotNull(id);
        try (Closeable ignored = () -> assertOK(client().performRequest(new Request("DELETE", "/_query/async/" + id)))) {
            assertEquals("?0", submit.getHeader("X-Elasticsearch-Async-Is-Running"));
            // keep_on_completion acknowledges the final submit only after async indexing and task unregistration.
            assertEquals(expected, checkProfile(submit));
            var get = new Request("GET", "/_query/async/" + id);
            assertEquals(expected, checkProfile(perform(get)));
        }
    }

    private static void seed() throws IOException {
        var create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("""
            {
              "settings":{"index.mode":"standard","number_of_shards":1,"number_of_replicas":0},
              "mappings":{"properties":{"@timestamp":{"type":"date"},"key":{"type":"keyword"},"role":{"type":"keyword"}}}
            }
            """);
        assertOK(client().performRequest(create));
        var seed = new Request("POST", "/" + INDEX + "/_bulk?refresh=true");
        seed.setJsonEntity("""
            {"index":{"_id":"start"}}
            {"@timestamp":"2026-01-01T00:00:00Z","key":"match","role":"start"}
            {"index":{"_id":"end"}}
            {"@timestamp":"2026-01-01T00:00:01Z","key":"match","role":"end"}
            {"index":{"_id":"noise"}}
            {"@timestamp":"2026-01-01T00:00:01Z","key":"unrelated","role":"end"}
            """);
        assertEquals(false, entityAsMap(client().performRequest(seed)).get("errors"));
    }

    private static Response query(String path, Map<String, Object> body) throws IOException {
        var request = new Request("POST", path);
        try (var json = JsonXContent.contentBuilder()) {
            request.setJsonEntity(Strings.toString(json.map(body)));
        }
        return perform(request);
    }

    private static Response perform(Request request) throws IOException {
        request.setOptions(request.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        Response response = client().performRequest(request);
        assertOK(response);
        return response;
    }

    private static Map<?, ?> checkProfile(Response response) throws IOException {
        assertNull(response.getHeader(PRIVATE_HEADER));
        var warnings = Arrays.stream(response.getHeaders())
            .filter(header -> "Warning".equalsIgnoreCase(header.getName()))
            .map(header -> header.getValue())
            .toList();
        assertThat(warnings.toString(), containsString("Cannot parse number [end]"));
        Map<String, Object> body = entityAsMap(response);
        assertEquals(List.of(Arrays.asList("end", null)), body.get("values"));
        Map<?, ?> profile = (Map<?, ?>) body.get("profile");
        assertNotNull(profile);
        Map<?, ?> stats = (Map<?, ?>) profile.get("sequence_key_filter");
        assertNotNull(stats);
        assertEquals(1L, ((Number) stats.get("built_searches")).longValue());
        assertEquals(1L, ((Number) stats.get("applied_shard_searches")).longValue());
        assertEquals(1L, ((Number) ((Map<?, ?>) stats.get("index_modes")).get("standard")).longValue());
        return stats;
    }
}
