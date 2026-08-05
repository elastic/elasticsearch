/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class OrderedGroupedActionListenerTests extends ESTestCase {

    public void testDeliversResultsInIterationOrder() {
        // Tasks complete synchronously inside the loop in iteration order; baseline ergonomic case.
        final AtomicReference<List<String>> result = new AtomicReference<>();
        OrderedGroupedActionListener.<Integer, String>forEach(
            List.of(1, 2, 3),
            (value, listener) -> listener.onResponse("item-" + value),
            ActionTestUtils.assertNoFailureListener(result::set)
        );
        assertThat(result.get(), contains("item-1", "item-2", "item-3"));
    }

    public void testPreservesIterationOrderWhenTasksCompleteOutOfOrder() {
        // Defer each task's completion via the test-controlled map and fire them out of iteration order.
        // The delegate must still receive results in the iteration order of the original collection.
        final Map<Integer, ActionListener<String>> deferred = new HashMap<>();
        final AtomicReference<List<String>> result = new AtomicReference<>();
        OrderedGroupedActionListener.<Integer, String>forEach(
            List.of(1, 2, 3),
            deferred::put,
            ActionTestUtils.assertNoFailureListener(result::set)
        );

        deferred.get(3).onResponse("third");
        deferred.get(1).onResponse("first");
        deferred.get(2).onResponse("second");

        assertThat(result.get(), contains("first", "second", "third"));
    }

    public void testEmptyCollectionFiresDelegateImmediatelyWithEmptyList() {
        final AtomicReference<List<String>> result = new AtomicReference<>();
        final AtomicInteger taskInvocations = new AtomicInteger();
        OrderedGroupedActionListener.<Integer, String>forEach(List.of(), (value, listener) -> {
            taskInvocations.incrementAndGet();
            listener.onResponse("unused");
        }, ActionTestUtils.assertNoFailureListener(result::set));
        assertThat(result.get(), equalTo(List.of()));
        assertThat(taskInvocations.get(), equalTo(0));
    }

    public void testFirstFailureWinsAndSubsequentAreSuppressed() {
        // Defer all tasks so we can fire failures in a controlled order and assert the suppression chain.
        final Map<Integer, ActionListener<String>> deferred = new HashMap<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        OrderedGroupedActionListener.<Integer, String>forEach(List.of(1, 2, 3), deferred::put, ActionListener.wrap(v -> {
            throw new AssertionError("expected failure, got " + v);
        }, failure::set));

        final Exception first = new RuntimeException("first");
        final Exception second = new RuntimeException("second");
        final Exception third = new RuntimeException("third");
        deferred.get(1).onFailure(first);
        deferred.get(2).onFailure(second);
        deferred.get(3).onFailure(third);

        assertThat(failure.get(), sameInstance(first));
        assertThat(List.of(failure.get().getSuppressed()), contains(sameInstance(second), sameInstance(third)));
    }

    public void testMixedSuccessAndFailureStillReportsFailure() {
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final Exception boom = new RuntimeException("boom");
        OrderedGroupedActionListener.<Integer, String>forEach(List.of(1, 2, 3), (value, listener) -> {
            if (value == 2) {
                listener.onFailure(boom);
            } else {
                listener.onResponse("item-" + value);
            }
        }, ActionListener.wrap(v -> { throw new AssertionError("expected failure, got " + v); }, failure::set));
        assertThat(failure.get(), sameInstance(boom));
    }

    public void testInvokesDelegateExactlyOnce() {
        final AtomicInteger delegateInvocations = new AtomicInteger();
        OrderedGroupedActionListener.<Integer, String>forEach(
            List.of(1, 2, 3),
            (value, listener) -> listener.onResponse("item-" + value),
            ActionListener.wrap(v -> delegateInvocations.incrementAndGet(), e -> delegateInvocations.incrementAndGet())
        );
        assertThat(delegateInvocations.get(), equalTo(1));
    }

    public void testSelfSuppressionGuardWhenSameExceptionRefiredTwice() {
        // Defensive: if a caller hands the same exception instance back through two child listeners,
        // we must not call addSuppressed(e, e) — that would throw IllegalArgumentException.
        final Map<Integer, ActionListener<String>> deferred = new HashMap<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        OrderedGroupedActionListener.forEach(List.of(1, 2), deferred::put, ActionListener.wrap(v -> {
            throw new AssertionError("expected failure, got " + v);
        }, failure::set));

        final Exception shared = new RuntimeException("shared");
        deferred.get(1).onFailure(shared);
        deferred.get(2).onFailure(shared);

        assertThat(failure.get(), sameInstance(shared));
        assertThat(failure.get().getSuppressed().length, equalTo(0));
    }
}
