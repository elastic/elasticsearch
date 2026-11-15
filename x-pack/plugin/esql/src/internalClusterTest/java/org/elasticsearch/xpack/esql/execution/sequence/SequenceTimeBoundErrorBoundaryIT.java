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

/** Same accepted-input success oracle before and after range simplification, independently of retention enablement. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceTimeBoundErrorBoundaryIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389000001Z");
    private static final String GOOD = "sequence-boundary-start-good";
    private static final String BAD = "sequence-boundary-unreachable-tail";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.class);
    }

    public void testDeterministicUnreachableRuntimeKeyPreservesBaselineSuccess() {
        for (String index : List.of(GOOD, BAD)) {
            var properties = Map.of("@timestamp", Map.of("type", "date_nanos"), "failure", Map.of("type", "long"));
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
                .add(prepareIndex(GOOD).setId("good-start").setSource("@timestamp", BASE.toString(), "failure", 7L))
                .add(prepareIndex(BAD).setId("unreachable-final").setSource("@timestamp", BASE.plusSeconds(31).toString()))
                .get()
                .hasFailures()
        );
        refresh(GOOD, BAD);
        // This is the existing real deterministic runtime factory; the ordinary query proves the loader is active.
        Exception fixtureFailure = expectThrows(Exception.class, () -> {
            try (var response = run("FROM sequence-boundary-unreachable-tail | KEEP failure | LIMIT 10")) {
                getValuesList(response);
            }
        });
        assertThat(ExceptionsHelper.stackTrace(fixtureFailure), containsString("sequence mapped loading failure"));
        try (var response = run("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-boundary-start-good | EVAL key=failure | KEEP key),
            (FROM sequence-boundary-unreachable-tail | EVAL key=failure | KEEP key)
            | KEEP key | LIMIT 10
            """)) {
            assertEquals(List.of(), getValuesList(response));
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
