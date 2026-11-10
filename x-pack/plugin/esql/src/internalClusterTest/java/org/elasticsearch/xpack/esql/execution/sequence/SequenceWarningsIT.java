/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/** Real searches and compute evaluators must share warning allowances across candidate pages and payload replay. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceWarningsIT extends AbstractEsqlIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");

    public void testWarningsRemainBoundedAcrossCandidatePages() {
        createIndices();
        var bulk = client().prepareBulk();
        for (int event = 0; event < 700; event++) {
            bulk.add(
                prepareIndex("sequence-warning-a").setId("A" + event)
                    .setSource("@timestamp", BASE.plusMillis(event).toString(), "key", "shared", "payload", "bad-" + event)
            );
        }
        bulk.add(
            prepareIndex("sequence-warning-b").setId("B")
                .setSource("@timestamp", BASE.plusMillis(701).toString(), "key", "shared", "payload", "completion")
        );
        assertFalse(bulk.get().hasFailures());
        refresh("sequence-warning-a", "sequence-warning-b");
        String query = """
            sequence by key with {"maxspan":"30s"}
            (from sequence-warning-a | where COALESCE(TO_LONG(payload), -1)==-1 | keep key, payload),
            (from sequence-warning-b | keep key, payload)
            | keep stage1.payload, stage2.payload
            | limit 1
            """;
        Observed observed = runWithWarnings(query);
        assertEquals(List.of(List.of("bad-699", "completion")), observed.values());
        assertEquals(20, observed.warnings().stream().filter(w -> w.contains("Cannot parse number [bad-")).count());
        assertEquals(1, observed.warnings().stream().filter(w -> w.contains("Only first 20 failures recorded.")).count());
        // The allowance belongs to this query, so a later query reports its own failures.
        Observed nextQuery = runWithWarnings(query);
        assertEquals(observed.values(), nextQuery.values());
        assertEquals(observed.warnings(), nextQuery.warnings());
    }

    public void testWarningsRemainBoundedAcrossPayloadReplayAndSeparateExpressions() {
        createIndices();
        var bulk = client().prepareBulk();
        List<List<Object>> expected = new ArrayList<>();
        for (int event = 0; event < 40; event++) {
            String key = "key-" + (100 + event);
            String left = "left-" + event;
            String right = "right-" + event;
            bulk.add(
                prepareIndex("sequence-warning-a").setId("A" + event)
                    .setSource("@timestamp", BASE.plusMillis(event).toString(), "key", key, "payload", left)
            );
            bulk.add(
                prepareIndex("sequence-warning-b").setId("B" + event)
                    .setSource("@timestamp", BASE.plusMillis(1000 + event).toString(), "key", key, "payload", right)
            );
            expected.add(List.of(key, left, right));
        }
        assertFalse(bulk.get().hasFailures());
        refresh("sequence-warning-a", "sequence-warning-b");
        Observed observed = runWithWarnings("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-warning-a | where COALESCE(TO_LONG(payload), -1)==-1 | keep key, payload),
            (from sequence-warning-b | where COALESCE(TO_LONG(payload), -1)==-1 | keep key, payload)
            | keep key, stage1.payload, stage2.payload
            | sort key
            | limit 100
            """);
        assertEquals(expected, observed.values());
        assertEquals(20, observed.warnings().stream().filter(w -> w.contains("Cannot parse number [left-")).count());
        assertEquals(20, observed.warnings().stream().filter(w -> w.contains("Cannot parse number [right-")).count());
        assertEquals(2, observed.warnings().stream().filter(w -> w.contains("Only first 20 failures recorded.")).count());
    }

    private Observed runWithWarnings(String query) {
        var coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        var future = new PlainActionFuture<Observed>();
        var request = syncEsqlQueryRequest(query).pragmas(new QueryPragmas(Settings.builder().put("page_size", 17).build()));
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            var threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
                .getThreadPool()
                .getThreadContext();
            // Compare ES|QL expression warnings only; node-setting deprecations depend on the coordinator.
            var warnings = threadContext.getResponseHeaders()
                .getOrDefault("Warning", List.of())
                .stream()
                .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, false))
                .filter(warning -> warning.startsWith("Line "))
                .toList();
            future.onResponse(new Observed(getValuesList(response), warnings));
        }, future::onFailure));
        return future.actionGet(DEFAULT_REQUEST_TIMEOUT);
    }

    private void createIndices() {
        for (String index : List.of("sequence-warning-a", "sequence-warning-b")) {
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(
                        Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .setMapping("@timestamp", "type=date", "key", "type=keyword", "payload", "type=keyword")
            );
        }
    }

    @After
    public void checkSequenceResourcesReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }

    /** Captures warnings on the coordinator callback thread alongside values before the response is released. */
    private record Observed(List<List<Object>> values, List<String> warnings) {}
}
