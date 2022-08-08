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
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

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

    public void testShouldExecuteWithCorrectContext() throws Exception {

        var queue = new DeterministicTaskQueue();
        var pool = queue.getThreadPool();
        pool.getThreadContext().addResponseHeader("header", "root");

        var completed = new AtomicReference<String>();
        var listener = new AllocationActionListener<AcknowledgedResponse>(
            ActionListener.wrap(
                ignore -> completed.set(pool.getThreadContext().getResponseHeaders().get("header").get(0)),
                exception -> { throw new AssertionError("Should not fail in test"); }
            ),
            pool.getThreadContext()
        );

        pool.generic().execute(() -> {
            pool.getThreadContext().addResponseHeader("header", "clusterStateUpdate");
            listener.clusterStateUpdate().onResponse(AcknowledgedResponse.TRUE);
        });
        pool.generic().execute(() -> {
            pool.getThreadContext().addResponseHeader("header", "reroute");
            listener.reroute().onResponse(null);
        });

        queue.runAllTasks();
        assertThat(completed.get(), equalTo("root"));
    }

    private ThreadContext createEmptyThreadContext() {
        return new ThreadContext(Settings.EMPTY);
    }
}
