/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.QueryScopeRegistry;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class SpillStatsTests extends ESTestCase {
    /**
     * The figures come from the store and the registry as they stand when asked: a scope open counts, a closed one no
     * longer; the store's limit and use as the store reports them.
     */
    public void testFiguresFollowTheStoreAndTheRegistry() throws Exception {
        long limit = between(1, 1_000_000);
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.INLINE,
            null,
            limit,
            "test.spill",
            new Random(randomLong())
        );
        QueryScopeRegistry registry = new QueryScopeRegistry(
            (queryId, onClosing) -> QueryScope.create(
                queryId,
                TestBlockFactory.getNonBreakingInstance(),
                1024,
                0,
                store,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                onClosing
            )
        );
        SpillStats stats = new SpillStats(store, registry);
        Counters before = new Counters();
        stats.populate(before);
        assertThat(before.get(SpillStats.SCOPES), equalTo(0L));
        assertThat(before.get(SpillStats.BYTES_LIMIT), equalTo(limit));
        assertThat(before.get(SpillStats.BYTES_IN_USE), equalTo(0L));
        assertThat(before.get(SpillStats.CLEANUP_DEBT_BYTES), equalTo(0L));

        QueryScope.Lease lease = registry.acquire("q");
        Counters open = new Counters();
        stats.populate(open);
        assertThat(open.get(SpillStats.SCOPES), equalTo(1L));

        PlainActionFuture<Void> closed = new PlainActionFuture<>();
        lease.scope().addCloseListener(closed);
        lease.close();
        closed.get(10, TimeUnit.SECONDS);
        Counters after = new Counters();
        stats.populate(after);
        assertThat(after.get(SpillStats.SCOPES), equalTo(0L));
        PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
        store.shutdown(shutdown);
        shutdown.get(10, TimeUnit.SECONDS);
    }

    /**
     * Without a store there is nothing to report, so the stats carry no spill entry at all.
     */
    public void testWithoutAStoreNothingIsReported() {
        SpillStats stats = new SpillStats(null, new QueryScopeRegistry((queryId, onClosing) -> null));
        Counters counters = new Counters();
        stats.populate(counters);
        assertThat(counters.toNestedMap(), not(hasKey("spill")));
    }
}
