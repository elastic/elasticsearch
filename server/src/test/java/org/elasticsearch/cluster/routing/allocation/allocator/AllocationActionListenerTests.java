/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class AllocationActionListenerTests extends ESTestCase {

    public void testShouldDelegateWhenBothComplete() {
        var completed = new AtomicBoolean(false);
        var listener = new AllocationActionListener<AcknowledgedResponse>(
            ActionListener.wrap(ignore -> completed.set(true), exception -> { throw new AssertionError("Should not fail in test"); }),
            createEmptyThreadContext()
        );

        listener.clusterStateUpdate().onResponse(AcknowledgedResponse.TRUE);
        listener.reroute().onResponse(null);

        assertThat(completed.get(), equalTo(true));
    }

    public void testShouldNotDelegateWhenOnlyOneComplete() {
        var completed = new AtomicBoolean(false);
        var listener = new AllocationActionListener<AcknowledgedResponse>(
            ActionListener.wrap(ignore -> completed.set(true), exception -> { throw new AssertionError("Should not fail in test"); }),
            createEmptyThreadContext()
        );

        if (randomBoolean()) {
            listener.clusterStateUpdate().onResponse(AcknowledgedResponse.TRUE);
        } else {
            listener.reroute().onResponse(null);
        }

        assertThat(completed.get(), equalTo(false));
    }

    public void testShouldDelegateFailureImmediately() {
        var completed = new AtomicBoolean(false);
        var listener = new AllocationActionListener<AcknowledgedResponse>(
            ActionListener.wrap(ignore -> { throw new AssertionError("Should not complete in test"); }, exception -> completed.set(true)),
            createEmptyThreadContext()
        );

        if (randomBoolean()) {
            listener.clusterStateUpdate().onFailure(new RuntimeException());
        } else {
            listener.reroute().onFailure(new RuntimeException());
        }

        assertThat(completed.get(), equalTo(true));
    }

    public void testShouldExecuteWithCorrectContext() {

        var threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader("header", "root");

        var result = new AtomicReference<String>();
        var listener = new AllocationActionListener<>(
            ActionListener.wrap(
                ignore -> result.set(threadContext.getHeader("header")),
                exception -> { throw new AssertionError("Should not fail in test"); }
            ),
            threadContext
        );

        executeInRandomOrder(
            threadContext,
            List.of(
                new Tuple<>("clusterStateUpdate", () -> listener.clusterStateUpdate().onResponse(AcknowledgedResponse.TRUE)),
                new Tuple<>("reroute", () -> listener.reroute().onResponse(null))
            )
        );

        assertThat(result.get(), equalTo("root"));
    }

    private static void executeInRandomOrder(ThreadContext context, List<Tuple<String, Runnable>> actions) {
        for (var action : shuffledList(actions)) {
            try (var ignored = context.stashContext()) {
                context.putHeader("header", action.v1());
                action.v2().run();
            }
        }
    }

    private static ThreadContext createEmptyThreadContext() {
        return new ThreadContext(Settings.EMPTY);
    }
}
