/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.spill.spi.SpillScope;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * One query on one node: its memory account and, when spilling is on, its spilled data. The scope owns the
 * {@link QueryBreaker}, a root view of it for the operations that outlive any driver, a block factory and a
 * {@link BigArrays} over that root view, and the lazily created {@link SpillScope}.
 * <p>
 * The scope lives as long as any lease on it does: the coordinator's root lease spans the whole query, a compute lease
 * spans one compute on this node, an exchange lease spans a sink handler or a remote sink, a response lease spans the
 * reading of a result. On every transfer the destination lease is acquired before the source is released. When the last
 * lease goes, the scope closes asynchronously: its spilled data is deleted and, once no backend worker touches it any
 * more, {@link #addCloseListener} subscribers run. A scope that has started closing hands out no further leases; the
 * registry then creates a fresh one for the same query, which is correct because nothing spilled has to survive the gap.
 */
public final class QueryScope {

    private static final Logger logger = LogManager.getLogger(QueryScope.class);

    private final String queryId;
    private final QueryBreaker breaker;
    private final QueryBreaker.View rootView;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;
    @Nullable
    private final SpillStore store;
    private final long minSpillBytes;
    private final Consumer<QueryScope> onClosing;
    private final AtomicInteger leases = new AtomicInteger();
    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private final SubscribableListener<Void> closed = new SubscribableListener<>();
    @Nullable
    private SpillScope spillScope;

    private QueryScope(
        String queryId,
        BlockFactory nodeFactory,
        long budgetBytes,
        long minSpillBytes,
        @Nullable SpillStore store,
        Consumer<QueryScope> onClosing
    ) {
        this.queryId = queryId;
        this.breaker = new QueryBreaker(nodeFactory.breaker(), budgetBytes);
        this.rootView = breaker.newView();
        this.bigArrays = nodeFactory.bigArrays().withBreakerService(rootView.service());
        this.blockFactory = BlockFactory.builder(bigArrays)
            .breaker(rootView)
            .maxPrimitiveArraySize(nodeFactory.maxPrimitiveArrayBytes())
            .bytesRefRamOverestimateThreshold(nodeFactory.bytesRefRamOverestimateThreshold())
            .bytesRefRamOverestimateFactor(nodeFactory.bytesRefRamOverestimateFactor())
            .build();
        this.store = store;
        this.minSpillBytes = minSpillBytes;
        this.onClosing = onClosing;
    }

    /**
     * A scope with no lease yet; the caller takes the first one.
     *
     * @param nodeFactory the node's block factory, whose breaker is the request breaker; the scope's factory sits one
     *                    level below it, so that a block passed to another driver releases against the query's account
     * @param budgetBytes the soft limit of {@link QueryBreaker}
     * @param minSpillBytes the own-state minimum below which an operator never spills, see {@link #shouldSpill}
     * @param store the backend, or null when spilling is off, in which case the scope only accounts
     * @param onClosing called once, when the last lease is released and before the asynchronous close starts; the
     *                  registry uses it to drop the entry, so that a new scope may be created while this one closes
     */
    public static QueryScope create(
        String queryId,
        BlockFactory nodeFactory,
        long budgetBytes,
        long minSpillBytes,
        @Nullable SpillStore store,
        Consumer<QueryScope> onClosing
    ) {
        return new QueryScope(queryId, nodeFactory, budgetBytes, minSpillBytes, store, onClosing);
    }

    public String queryId() {
        return queryId;
    }

    public QueryBreaker breaker() {
        return breaker;
    }

    /**
     * The view the query's own operations charge: codec tasks, restores, writers and indexes, anything that may complete
     * on another thread or after a driver has closed.
     */
    public QueryBreaker.View rootView() {
        return rootView;
    }

    /**
     * The query's block factory over the root view; the parent of every driver factory of the query.
     */
    public BlockFactory blockFactory() {
        return blockFactory;
    }

    public BigArrays bigArrays() {
        return bigArrays;
    }

    public boolean spillEnabled() {
        return store != null;
    }

    public long budgetBytes() {
        return breaker.budget();
    }

    /**
     * The one policy, read at an operator's safe point: true when the query's total plus the estimate would cross the
     * budget and the operator holds at least the own-state minimum, so that a crossing does not spill every small
     * operator of the query at once; a smaller operator that trips the breaker fails as it does today.
     */
    public boolean shouldSpill(long ownStateBytes, long estimateBytes) {
        return store != null && ownStateBytes >= minSpillBytes && breaker.wouldCrossBudget(estimateBytes);
    }

    /**
     * The backend scope, created on the first call. The backend charges its own buffers to the root view.
     */
    public synchronized SpillScope spillScope() {
        if (store == null) {
            throw new IllegalStateException("spilling is disabled for query [" + queryId + "]");
        }
        if (spillScope == null) {
            spillScope = store.newScope(queryId, rootView);
        }
        return spillScope;
    }

    /**
     * What a driver allocates through: its own view of the query breaker, a local breaker over it, a {@link BigArrays}
     * over it and a child of the query's block factory over both, so that blocks and arrays consume one reservation.
     * Closed when the driver closes; its blocks may still be released afterwards, against the view.
     */
    public DriverAllocation newDriverAllocation(LocalCircuitBreaker.SizeSettings settings) {
        QueryBreaker.View view = breaker.newView();
        LocalCircuitBreaker local = new LocalCircuitBreaker(view, settings.overReservedBytes(), settings.maxOverReservedBytes());
        BigArrays arrays = bigArrays.withBreakerService(view.service());
        return new DriverAllocation(view, local, arrays, blockFactory.newChildFactory(local, arrays));
    }

    public record DriverAllocation(QueryBreaker.View view, LocalCircuitBreaker localBreaker, BigArrays bigArrays, BlockFactory blockFactory)
        implements
            Releasable {
        @Override
        public void close() {
            localBreaker.close();
            view.close();
        }
    }

    /**
     * A new lease, or null once the scope has started closing. Never blocks.
     */
    @Nullable
    public Lease tryAcquire() {
        while (true) {
            int current = leases.get();
            if (current < 0) {
                return null;
            }
            if (leases.compareAndSet(current, current + 1)) {
                return new Lease();
            }
        }
    }

    public int leaseCount() {
        return Math.max(0, leases.get());
    }

    public boolean isClosing() {
        return leases.get() < 0;
    }

    /**
     * Runs when the scope has finished closing: its spilled data is deleted and no backend worker touches it any more.
     * Runs inline if that already happened.
     */
    public void addCloseListener(ActionListener<Void> listener) {
        closed.addListener(listener);
    }

    private void release() {
        int remaining = leases.decrementAndGet();
        assert remaining >= 0 : "lease released twice on " + this;
        if (remaining == 0 && leases.compareAndSet(0, -1)) {
            close();
        }
    }

    private void close() {
        if (closeStarted.compareAndSet(false, true) == false) {
            return;
        }
        ActionListener<Void> done = ActionListener.running(() -> {
            rootView.close();
            if (breaker.used() != 0) {
                logger.warn("query [{}] closed with [{}] bytes still accounted on this node", queryId, breaker.used());
            }
            closed.onResponse(null);
        });
        try {
            onClosing.accept(this);
            SpillScope scope;
            synchronized (this) {
                scope = spillScope;
            }
            if (scope == null) {
                done.onResponse(null);
            } else {
                scope.close(ActionListener.wrap(ignored -> done.onResponse(null), e -> {
                    logger.warn(() -> "failed to delete the spilled data of query [" + queryId + "]", e);
                    done.onResponse(null);
                }));
            }
        } catch (RuntimeException e) {
            // a close listener waits for this scope whatever the hook or the backend did
            logger.warn(() -> "failed to close the scope of query [" + queryId + "]", e);
            if (closed.isDone() == false) {
                done.onResponse(null);
            }
        }
    }

    @Override
    public String toString() {
        return "QueryScope[" + queryId + ", leases=" + leases.get() + ", " + breaker + "]";
    }

    /**
     * One holder's claim on the scope. Idempotent: closing it twice releases once.
     */
    public final class Lease implements Releasable {
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease() {}

        public QueryScope scope() {
            return QueryScope.this;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                release();
            }
        }
    }
}
