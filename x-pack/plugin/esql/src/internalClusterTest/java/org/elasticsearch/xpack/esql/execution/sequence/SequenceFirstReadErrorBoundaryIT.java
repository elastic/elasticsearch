/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.junit.After;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;

/** Same baseline-success oracle is run before and after first-read retention to expose a supported-input boundary. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceFirstReadErrorBoundaryIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389000001Z");
    private static final String GOOD = "sequence-e5-boundary-first-good";
    private static final String BAD = "sequence-e5-boundary-first-unmatched";
    private static final String FINAL = "sequence-e5-boundary-final";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.class);
    }

    public void testDeterministicUnmatchedRuntimeOutputPreservesBaselineSuccess() {
        for (String index : List.of(GOOD, BAD, FINAL)) {
            var properties = Map.of(
                "@timestamp",
                Map.of("type", "date_nanos"),
                "key",
                Map.of("type", "keyword"),
                "failure",
                Map.of("type", "long")
            );
            Map<String, Object> mapping = index.equals(BAD)
                ? Map.of(
                    "properties",
                    properties,
                    "runtime",
                    Map.of("failure", Map.of("type", "long", "script", Map.of("lang", "sequence_failure", "source", "failure")))
                )
                : Map.of("properties", properties);
            assertAcked(indicesAdmin().prepareCreate(index).setSettings(indexSettings(1, 0)).setMapping(mapping));
        }
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex(GOOD).setId("good-start").setSource("@timestamp", BASE.toString(), "key", "good", "failure", 7L))
                .add(prepareIndex(BAD).setId("unmatched-start").setSource("@timestamp", BASE.plusMillis(1).toString(), "key", "unmatched"))
                .add(prepareIndex(FINAL).setId("good-final").setSource("@timestamp", BASE.plusMillis(2).toString(), "key", "good"))
                .get()
                .hasFailures()
        );
        refresh(GOOD, BAD, FINAL);
        // The existing fixture registers this script in its deterministic map and reports that flag through the real runtime factory.
        Exception fixtureFailure = expectThrows(Exception.class, () -> {
            try (var response = run("FROM sequence-e5-boundary-first-unmatched | KEEP failure | LIMIT 10")) {
                getValuesList(response);
            }
        });
        assertThat(ExceptionsHelper.stackTrace(fixtureFailure), containsString("sequence mapped loading failure"));

        try (var response = run("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-e5-boundary-first-* | KEEP key,failure),
            (FROM sequence-e5-boundary-final | KEEP key)
            | KEEP key,failure | LIMIT 10
            """)) {
            assertEquals(List.of(List.of("good", 7L)), getValuesList(response));
        }
    }

    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }
}
