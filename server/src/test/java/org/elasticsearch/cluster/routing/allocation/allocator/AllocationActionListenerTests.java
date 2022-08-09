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
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsInAnyOrder;
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

        var context = new ThreadContext(Settings.EMPTY);

        // should not be changed after the listener is created
        context.putHeader("header", "root");// ensure this is visible in the listener
        context.addResponseHeader("header", "1");

        var listener = new AllocationActionListener<>(ActionListener.wrap(ignore -> {
            assertThat(context.getResponseHeaders().get("header"), containsInAnyOrder("1", "3", "4"));
            assertThat(context.getHeader("header"), equalTo("root"));
        }, exception -> { throw new AssertionError("Should not fail in test"); }), context);

        // this header should be ignored as it is added after context is captured
        context.addResponseHeader("header", "2");

        executeInRandomOrder(context, () -> {
            context.addResponseHeader("header", "3");
            var csl = listener.clusterStateUpdate();
            context.addResponseHeader("header", "4");
            csl.onResponse(AcknowledgedResponse.TRUE);
        }, () -> {
            // reroute is executed for multiple changes so its headers should be ignored
            context.addResponseHeader("header", "5");
            var reroute = listener.reroute();
            context.addResponseHeader("header", "6");
            reroute.onResponse(null);
        });
    }

    private static void executeInRandomOrder(ThreadContext context, Runnable... actions) {
        for (var action : shuffledList(List.of(actions))) {
            try (var ignored = context.stashContext()) {
                action.run();
            }
        }
    }

    private static ThreadContext createEmptyThreadContext() {
        return new ThreadContext(Settings.EMPTY);
    }
}
