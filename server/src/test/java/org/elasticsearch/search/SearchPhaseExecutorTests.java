/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search;

import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

public class SearchPhaseExecutorTests extends ESTestCase {

    public void testExecutePhaseReportsExactlyOneOutcome() throws Exception {
        List<String> events = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();
        SearchOperationListener listener = new SearchOperationListener() {
            @Override
            public void onPreDfsPhase(SearchContext searchContext) {
                events.add("pre");
            }

            @Override
            public void onDfsPhase(SearchContext searchContext, long tookInNanos) {
                assertThat(tookInNanos, greaterThanOrEqualTo(0L));
                events.add("success");
            }

            @Override
            public void onFailedDfsPhase(SearchContext searchContext, Throwable e) {
                events.add("failure");
                failures.add(e);
            }

            @Override
            public void onPreQueryPhase(SearchContext searchContext) {
                onPreDfsPhase(searchContext);
            }

            @Override
            public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
                onDfsPhase(searchContext, tookInNanos);
            }

            @Override
            public void onFailedQueryPhase(SearchContext searchContext, Throwable e) {
                onFailedDfsPhase(searchContext, e);
            }

            @Override
            public void onPreFetchPhase(SearchContext searchContext) {
                onPreDfsPhase(searchContext);
            }

            @Override
            public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
                onDfsPhase(searchContext, tookInNanos);
            }

            @Override
            public void onFailedFetchPhase(SearchContext searchContext, Throwable e) {
                onFailedDfsPhase(searchContext, e);
            }
        };
        try (SearchContext ctx = new TestSearchContext((SearchExecutionContext) null)) {
            for (int phase : new int[] { 0, 1, 2 }) {
                events.clear();
                failures.clear();
                // an Error must reach the failure hook too, otherwise the shard-level current-phase gauges would never be decremented
                Throwable failure = randomFrom(new IOException("io"), new IllegalStateException("state"), new AssertionError("error"));
                CheckedRunnable<Exception> body = () -> {
                    if (failure instanceof Error error) {
                        throw error;
                    }
                    throw (Exception) failure;
                };
                Throwable thrown = expectThrows(Throwable.class, () -> execute(phase, listener, ctx, body));
                assertThat(thrown, sameInstance(failure));
                assertEquals(List.of("pre", "failure"), events);
                assertEquals(List.of(failure), failures);

                events.clear();
                execute(phase, listener, ctx, () -> {});
                assertEquals(List.of("pre", "success"), events);
            }
        }
    }

    private static void execute(int phase, SearchOperationListener listener, SearchContext ctx, CheckedRunnable<Exception> body)
        throws Exception {
        switch (phase) {
            case 0 -> SearchPhaseExecutor.executeDfsPhase(listener, ctx, body);
            case 1 -> SearchPhaseExecutor.executeQueryPhase(listener, ctx, body);
            case 2 -> SearchPhaseExecutor.executeFetchPhase(listener, ctx, body);
            default -> throw new AssertionError("unknown phase " + phase);
        }
    }
}
