/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * The automaton that {@code BinaryDvConfirmedQuery} builds is retained by the query and used to confirm matches for as long as the
 * query runs, so it must stay charged to the request breaker until the search context releases its query construction memory.
 * {@link SearchOperationListener#onQueryPhase} runs once the query has been fully executed but before its search context is closed,
 * which makes it the latest point at which the charge can still be observed.
 */
public class WildcardAutomatonBreakerIT extends ESIntegTestCase {

    /** Distinguishes the pattern from a plain substring so that determinizing it yields a sizeable automaton. */
    private static final String GAP = "y".repeat(5);

    /** Request breaker usage, per node, as seen after the query phase executed but before its context was released. */
    private static final AtomicLong usedAfterQueryExecuted = new AtomicLong(-1);
    private static final AtomicLong constructionMemoryAfterQueryExecuted = new AtomicLong(-1);

    public static class BreakerObservingPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
                    usedAfterQueryExecuted.set(searchContext.circuitBreaker().getUsed());
                    constructionMemoryAfterQueryExecuted.set(searchContext.getSearchExecutionContext().getQueryConstructionMemoryUsed());
                }
            });
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // The test cluster randomly swaps the request breaker for a noop one, which never reports usage.
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), CircuitBreaker.Type.MEMORY)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(Wildcard.class, BreakerObservingPlugin.class);
    }

    public void testAutomatonStaysChargedUntilSearchContextReleased() {
        // Async translog durability keeps the last unsynced translog buffer page charged to the request breaker (in tests) until the
        // next periodic sync, which would leak into the baseline.
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST))
                .setMapping("wildcard", "type=wildcard")
        );
        for (int i = 0; i < 20; i++) {
            prepareIndex("test").setSource("wildcard", "xxa" + GAP + "bzzc" + i).get();
        }
        indicesAdmin().prepareRefresh("test").get();

        final String wildcardPattern = "*a" + "?".repeat(GAP.length()) + "b*c*";
        assertAutomatonChargedForQueryLifetime(
            QueryBuilders.wildcardQuery("wildcard", wildcardPattern),
            AutomatonQueries.toWildcardByteRunAutomaton(new Term("wildcard", wildcardPattern), false, NoopCircuitBreaker.INSTANCE)
        );

        final String regexpPattern = ".*a.{" + GAP.length() + "}b.*c.*";
        assertAutomatonChargedForQueryLifetime(
            QueryBuilders.regexpQuery("wildcard", regexpPattern),
            AutomatonQueries.toRegexpByteRunAutomaton(
                "wildcard",
                regexpPattern,
                RegexpQueryBuilder.DEFAULT_FLAGS_VALUE,
                0,
                RegexpQueryBuilder.DEFAULT_MAX_DETERMINIZED_STATES,
                NoopCircuitBreaker.INSTANCE
            )
        );
    }

    /**
     * Runs {@code query} and checks that, once it has executed, the query construction memory still held covers at least the retained
     * size of {@code automaton}, the same automaton built independently. The fixed gap in the pattern makes the automaton large enough
     * that the rest of the query could not account for that much on its own.
     */
    private void assertAutomatonChargedForQueryLifetime(QueryBuilder query, ByteRunAutomaton automaton) {
        usedAfterQueryExecuted.set(-1);
        constructionMemoryAfterQueryExecuted.set(-1);
        final long automatonBytes = automaton.ramBytesUsed();
        final long baseline = requestBreakerUsed();
        assertThat(baseline, equalTo(0L));

        assertResponse(prepareSearch("test").setQuery(query), response -> {
            assertNoFailures(response);
            assertThat(response.getHits().getTotalHits().value(), equalTo(20L));
        });

        assertThat(constructionMemoryAfterQueryExecuted.get(), greaterThanOrEqualTo(automatonBytes));
        assertThat(usedAfterQueryExecuted.get(), greaterThanOrEqualTo(baseline + automatonBytes));
        // Releasing the search context hands everything back.
        assertThat(requestBreakerUsed(), equalTo(baseline));
    }

    private static long requestBreakerUsed() {
        long used = 0;
        for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
            used += service.getBreaker(CircuitBreaker.REQUEST).getUsed();
        }
        return used;
    }
}
