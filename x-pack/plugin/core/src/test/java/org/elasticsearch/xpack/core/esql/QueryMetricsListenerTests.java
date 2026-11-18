/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class QueryMetricsListenerTests extends ESTestCase {

    /** A consumer that does not know about outcomes, like billing, is never called for a query that did not succeed. */
    public void testListenerThatIgnoresOutcomesOnlySeesSuccess() {
        List<Map<String, Long>> seen = new ArrayList<>();
        QueryMetricsListener listener = seen::add;
        Map<String, Long> succeeded = Map.of(QueryMetricsListener.CPU_NANOS, 1L);

        listener.onQueryCompleted(Outcome.FAILED, Map.of(QueryMetricsListener.CPU_NANOS, 2L));
        listener.onQueryCompleted(Outcome.CANCELLED, Map.of(QueryMetricsListener.CPU_NANOS, 3L));
        listener.onQueryCompleted(Outcome.SUCCESS, succeeded);

        assertThat(seen.size(), equalTo(1));
        assertThat(seen.get(0), sameInstance(succeeded));
    }

    public void testListenerThatKnowsOutcomesSeesAllOfThem() {
        List<Outcome> seen = new ArrayList<>();
        QueryMetricsListener listener = new QueryMetricsListener() {
            @Override
            public void onQueryCompleted(Map<String, Long> metrics) {
                fail("outcomes are delivered with the outcome");
            }

            @Override
            public void onQueryCompleted(Outcome outcome, Map<String, Long> metrics) {
                seen.add(outcome);
            }
        };

        for (Outcome outcome : Outcome.values()) {
            listener.onQueryCompleted(outcome, Map.of());
        }

        assertThat(seen, equalTo(List.of(Outcome.values())));
    }

    public void testCompositeOfNothingIsNoop() {
        assertThat(QueryMetricsListener.composite(List.of()), sameInstance(QueryMetricsListener.NOOP));
    }

    public void testCompositeCallsEveryListenerWithTheOutcomeAndRespectsEachOnesChoice() {
        List<Outcome> outcomeAware = new ArrayList<>();
        List<Outcome> successOnly = new ArrayList<>();
        QueryMetricsListener aware = new QueryMetricsListener() {
            @Override
            public void onQueryCompleted(Map<String, Long> metrics) {
                fail("outcomes are delivered with the outcome");
            }

            @Override
            public void onQueryCompleted(Outcome outcome, Map<String, Long> metrics) {
                outcomeAware.add(outcome);
            }
        };
        QueryMetricsListener unaware = metrics -> successOnly.add(Outcome.SUCCESS);
        QueryMetricsListener composite = QueryMetricsListener.composite(List.of(aware, unaware));

        composite.onQueryCompleted(Outcome.FAILED, Map.of());
        composite.onQueryCompleted(Outcome.SUCCESS, Map.of());
        composite.onQueryCompleted(Outcome.CANCELLED, Map.of());

        assertThat(outcomeAware, equalTo(List.of(Outcome.FAILED, Outcome.SUCCESS, Outcome.CANCELLED)));
        assertThat(successOnly, equalTo(List.of(Outcome.SUCCESS)));
    }

    /** What a consumer written before outcomes existed looks like: a class that implements the one method it was given. */
    private static final class SuccessSumListener implements QueryMetricsListener {
        long sum;

        @Override
        public void onQueryCompleted(Map<String, Long> metrics) {
            sum += metrics.get(QueryMetricsListener.CPU_NANOS);
        }
    }

    public void testListenerWrittenBeforeOutcomesCountsOnlySuccessThroughTheComposite() {
        SuccessSumListener old = new SuccessSumListener();
        QueryMetricsListener composite = QueryMetricsListener.composite(List.of(old));

        composite.onQueryCompleted(Outcome.FAILED, Map.of(QueryMetricsListener.CPU_NANOS, 100L));
        composite.onQueryCompleted(Outcome.SUCCESS, Map.of(QueryMetricsListener.CPU_NANOS, 7L));
        composite.onQueryCompleted(Outcome.CANCELLED, Map.of(QueryMetricsListener.CPU_NANOS, 1000L));

        assertThat(old.sum, equalTo(7L));
    }

    /** A listener that throws, such as one that only monitors, must not keep the ones after it, such as billing, from being called. */
    public void testCompositeCallsTheOtherListenersWhenOneThrowsAndThrowsTheFirstFailure() {
        List<String> called = new ArrayList<>();
        QueryMetricsListener first = metrics -> called.add("first");
        IllegalStateException brokenFailure = new IllegalStateException("broken");
        QueryMetricsListener broken = metrics -> { throw brokenFailure; };
        IllegalArgumentException anotherFailure = new IllegalArgumentException("another");
        QueryMetricsListener another = metrics -> { throw anotherFailure; };
        QueryMetricsListener last = metrics -> called.add("last");
        QueryMetricsListener composite = QueryMetricsListener.composite(List.of(first, broken, another, last));

        IllegalStateException thrown = expectThrows(
            IllegalStateException.class,
            () -> composite.onQueryCompleted(Outcome.SUCCESS, Map.of())
        );

        assertThat(thrown, sameInstance(brokenFailure));
        assertThat(thrown.getSuppressed(), equalTo(new Throwable[] { anotherFailure }));
        assertThat(called, equalTo(List.of("first", "last")));
    }

    public void testCompositeIsNotAffectedByLaterChangesToItsList() {
        List<QueryMetricsListener> listeners = new ArrayList<>();
        List<String> called = new ArrayList<>();
        listeners.add(metrics -> called.add("first"));
        QueryMetricsListener composite = QueryMetricsListener.composite(listeners);
        listeners.add(metrics -> called.add("added later"));

        composite.onQueryCompleted(Outcome.SUCCESS, Map.of());

        assertThat(called, equalTo(List.of("first")));
    }

    /** The composite also stands in for listeners called the old way. */
    public void testCompositeCalledWithoutOutcomeIsASuccess() {
        List<Outcome> seen = new ArrayList<>();
        QueryMetricsListener aware = new QueryMetricsListener() {
            @Override
            public void onQueryCompleted(Map<String, Long> metrics) {}

            @Override
            public void onQueryCompleted(Outcome outcome, Map<String, Long> metrics) {
                seen.add(outcome);
            }
        };
        QueryMetricsListener.composite(List.of(aware)).onQueryCompleted(Map.of());
        assertThat(seen, equalTo(List.of(Outcome.SUCCESS)));
    }
}
