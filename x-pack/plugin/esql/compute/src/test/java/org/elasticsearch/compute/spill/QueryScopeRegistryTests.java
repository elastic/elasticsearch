/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The registry hands out leases bound to the entry it found or created, never on a scope that is closing, replaces a
 * closing entry with a fresh scope, and gives nothing when spilling is off for the query.
 */
public class QueryScopeRegistryTests extends ComputeTestCase {

    private QueryScopeRegistry registry(BlockFactory node, List<QueryScope> created) {
        return new QueryScopeRegistry((queryId, onClosing) -> {
            QueryScope scope = QueryScope.create(queryId, node, ByteSizeValue.ofMb(1).getBytes(), 1024, null, onClosing);
            synchronized (created) {
                created.add(scope);
            }
            return scope;
        });
    }

    public void testAcquireCreatesReusesAndReplacesAfterTheLastRelease() {
        List<QueryScope> created = new ArrayList<>();
        QueryScopeRegistry registry = registry(blockFactory(), created);
        QueryScope.Lease first = registry.acquire("q");
        QueryScope.Lease second = registry.acquire("q");
        assertThat(second.scope(), sameInstance(first.scope()));
        assertThat(registry.size(), equalTo(1));
        QueryScope.Lease resolved = registry.resolve("q");
        assertThat("a resolver sees the live scope", resolved.scope(), sameInstance(first.scope()));
        assertThat(registry.resolve("other"), nullValue());
        first.close();
        assertThat(registry.size(), equalTo(1));
        // the second compute's lease and the resolver's lease go; the scope closes and leaves the map
        second.close();
        assertThat(registry.size(), equalTo(1));
        resolved.close();
        assertThat(registry.size(), equalTo(0));
        assertThat(registry.resolve("q"), nullValue());
        // a later compute of the same query, after a gap, gets a fresh scope, never the closed one
        QueryScope.Lease third = registry.acquire("q");
        assertThat(third.scope(), not(sameInstance(first.scope())));
        assertTrue(first.scope().isClosing());
        third.close();
        assertThat(registry.size(), equalTo(0));
        assertThat(created.size(), equalTo(2));
    }

    public void testDisabledQueriesGetNoScope() {
        QueryScopeRegistry registry = new QueryScopeRegistry((queryId, onClosing) -> null);
        assertThat(registry.acquire("q"), nullValue());
        assertThat(registry.resolve("q"), nullValue());
        assertThat(registry.size(), equalTo(0));
    }

    public void testConcurrentAcquireAndReleaseNeverHandsOutAClosingScope() throws Exception {
        List<QueryScope> created = new ArrayList<>();
        QueryScopeRegistry registry = registry(blockFactory(), created);
        int threads = between(2, 8);
        int rounds = between(200, 2000);
        CyclicBarrier start = new CyclicBarrier(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger closingSeen = new AtomicInteger();
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                    for (int r = 0; r < rounds; r++) {
                        QueryScope.Lease lease = registry.acquire("q");
                        if (lease.scope().isClosing()) {
                            closingSeen.incrementAndGet();
                        }
                        QueryScope.Lease resolved = registry.resolve("q");
                        if (resolved != null) {
                            resolved.close();
                        }
                        lease.close();
                    }
                } catch (Throwable e) {
                    failure.set(e);
                }
            }, "registry-" + i);
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertThat(failure.get(), nullValue());
        assertThat("a lease is never bound to a closing scope", closingSeen.get(), equalTo(0));
        assertThat(registry.size(), equalTo(0));
        for (QueryScope scope : created) {
            PlainActionFuture<Void> done = new PlainActionFuture<>();
            scope.addCloseListener(done);
            assertTrue("every scope created was closed: " + scope, done.isDone());
        }
    }
}
