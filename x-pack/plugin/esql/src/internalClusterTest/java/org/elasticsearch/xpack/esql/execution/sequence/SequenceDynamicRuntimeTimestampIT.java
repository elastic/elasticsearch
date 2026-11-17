/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.junit.After;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/** Dynamic runtime mappings are source-time origins even when no runtime field was declared explicitly. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceDynamicRuntimeTimestampIT extends AbstractSequenceIntegTestCase {
    /** Source time is refused when dynamic mapping, rather than an explicit runtime declaration, created it. */
    public void testDynamicallyCreatedRuntimeTimestampIsRefused() {
        String index = "sequence-dynamic-runtime-time";
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(1, 0))
                .setMapping(
                    Map.of("dynamic", "runtime", "properties", Map.of("key", Map.of("type", "keyword"), "phase", Map.of("type", "keyword")))
                )
        );
        ensureGreen(index);
        prepareIndex(index).setId("a").setSource("@timestamp", "2026-01-01T00:00:00Z", "key", "k", "phase", "A").get();
        prepareIndex(index).setId("b").setSource("@timestamp", "2026-01-01T00:00:01Z", "key", "k", "phase", "B").get();
        // An acknowledged later mapping update is a barrier for the dynamically introduced timestamp mapping.
        assertAcked(
            indicesAdmin().preparePutMapping(index).setSource(Map.of("properties", Map.of("mapping_barrier", Map.of("type", "keyword"))))
        );
        refresh(index);
        Map<String, Object> mapping = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, index)
            .get()
            .mappings()
            .get(index)
            .sourceAsMap();
        if (mapping.get("runtime") instanceof Map<?, ?> fields && fields.get("@timestamp") instanceof Map<?, ?> timestamp) {
            assertEquals("date", timestamp.get("type"));
        } else {
            fail("dynamic runtime mapping must create a runtime timestamp");
        }
        try (var response = run("FROM " + index + " | STATS n=COUNT(@timestamp)")) {
            assertEquals(List.of(List.of(2L)), getValuesList(response));
        }
        String query = """
            SEQUENCE BY key WITHIN 10s PRIMARY 1
            (FROM sequence-dynamic-runtime-time | WHERE phase=="A"),
            (FROM sequence-dynamic-runtime-time | WHERE phase=="B")
            | KEEP key | LIMIT 10
            """;
        for (boolean partial : List.of(false, true)) {
            Exception failure = expectThrows(Exception.class, () -> {
                try (var response = run(syncEsqlQueryRequest(query).allowPartialResults(partial))) {
                    getValuesList(response);
                }
            });
            assertThat(ExceptionsHelper.stackTrace(failure), containsString("SEQUENCE does not support a runtime [@timestamp]"));
        }
    }

    /** Refusal must not leave native PIT readers or retained blocks alive. */
    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }
}
