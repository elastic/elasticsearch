/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class AllocationActionListenerTests extends ESTestCase {

    public void testShouldDelegateWhenBothComplete() {
        var completed = new AtomicBoolean(false);
        var listener = new AllocationActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoFailureListener(ignore -> completed.set(true)),
            createEmptyThreadContext()
        );

        listener.clusterStateUpdate().onResponse(AcknowledgedResponse.TRUE);
        listener.reroute().onResponse(null);

        assertThat(completed.get(), equalTo(true));
    }

    public void testShouldNotDelegateWhenOnlyOneComplete() {
        var completed = new AtomicBoolean(false);
        var listener = new AllocationActionListener<AcknowledgedResponse>(
            ActionTestUtils.assertNoFailureListener(ignore -> completed.set(true)),
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
        var listener = new AllocationActionListener<AcknowledgedResponse>(ActionListener.wrap(ignore -> {
            throw new AssertionError("Should not complete in test");
        }, exception -> completed.set(true)), createEmptyThreadContext());

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

        var header = new AtomicReference<String>();
        var responseHeaders = new AtomicReference<List<String>>();
        var listener = new AllocationActionListener<>(ActionTestUtils.assertNoFailureListener(ignore -> {
            header.set(context.getHeader("header"));
            responseHeaders.set(context.getResponseHeaders().get("header"));
        }), context);

        // this header should be ignored as it is added after context is captured
        context.addResponseHeader("header", "2");

        for (var action : shuffledList(List.<Runnable>of(() -> {
            // headers for clusterStateUpdate listener are captured
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
        }))) {
            try (var ignored = context.stashContext()) {
                action.run();
            }
        }

        assertThat(header.get(), equalTo("root"));
        assertThat(responseHeaders.get(), containsInAnyOrder("1", "3", "4"));
    }

    public void testShouldFailWithCorrectContext() {

        var context = new ThreadContext(Settings.EMPTY);

        // should not be changed after the listener is created
        context.putHeader("header", "root");// ensure this is visible in the listener
        context.addResponseHeader("header", "1");

        var header = new AtomicReference<String>();
        var responseHeaders = new AtomicReference<List<String>>();
        var listener = new AllocationActionListener<>(
            ActionListener.wrap(ignore -> { throw new AssertionError("Should not fail in test"); }, exception -> {
                header.set(context.getHeader("header"));
                responseHeaders.set(context.getResponseHeaders().get("header"));

            }),
            context
        );

        // this header should be ignored as it is added after context is captured
        context.addResponseHeader("header", "2");

        if (randomBoolean()) {
            try (var ignored = context.stashContext()) {
                context.addResponseHeader("header", "3");
                var csl = listener.clusterStateUpdate();
                context.addResponseHeader("header", "4");
                csl.onFailure(new RuntimeException("cluster-state-update-failed"));
            }

            assertThat(header.get(), equalTo("root"));
            assertThat(responseHeaders.get(), containsInAnyOrder("1", "3", "4"));
        } else {
            try (var ignored = context.stashContext()) {
                context.addResponseHeader("header", "5");
                var reroute = listener.reroute();
                context.addResponseHeader("header", "6");
                reroute.onFailure(new RuntimeException("reroute-failed"));
            }

            assertThat(header.get(), equalTo("root"));
            assertThat(responseHeaders.get(), containsInAnyOrder("1"));
        }
    }

    private static ThreadContext createEmptyThreadContext() {
        return new ThreadContext(Settings.EMPTY);
    }
}
