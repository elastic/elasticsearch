/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.DelegatingCircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * One query's share of the request breaker on one node, and the reservation credit each of its drivers holds.
 * <p>
 * Every charge goes to the parent, the request breaker, so the node still sees and still trips on every byte; in addition
 * the query's own total is counted here, so a spill client can read a soft signal, {@link #wouldCrossBudget}, before the
 * parent's hard limit is hit. This breaker never trips on its own. It is thread-safe: drivers, their workers and the spill's
 * codec tasks charge and release from many threads, and a release may arrive after the driver that allocated the bytes has
 * closed.
 * <p>
 * Drivers do not charge this breaker directly. Each gets a {@link View}, which carries that driver's reservation credit: a
 * reservation is charged to the parent once, and the allocations that follow consume the credit before anything further
 * reaches the parent. The driver's {@link org.elasticsearch.compute.data.LocalCircuitBreaker} and its
 * {@link org.elasticsearch.common.util.BigArrays} both sit on the view, so blocks and arrays consume the one reservation;
 * the local breaker's refills consume it in over-reserve chunks, a bounded over-estimate. Releases always go to the shared
 * counter and never back into a credit, so the local breaker's trimming cannot hand a reservation back.
 */
public final class QueryBreaker implements DelegatingCircuitBreaker {

    private final CircuitBreaker parent;
    private volatile long budgetBytes;
    private final AtomicLong used = new AtomicLong();

    /**
     * @param parent the request breaker, which keeps the hard limit
     * @param budgetBytes the soft limit this query may use on this node before its operators are asked to spill
     */
    public QueryBreaker(CircuitBreaker parent, long budgetBytes) {
        this.parent = parent;
        this.budgetBytes = budgetBytes;
    }

    /**
     * Bytes this query holds on this node, reservations included.
     */
    public long used() {
        return used.get();
    }

    public long budget() {
        return budgetBytes;
    }

    /**
     * Replaces the budget for the rest of the query: the test pragma's value, applied by every compute of the query on
     * the node before any of its drivers runs.
     */
    public void setBudget(long budgetBytes) {
        this.budgetBytes = budgetBytes;
    }

    /**
     * The soft signal: true when charging {@code estimate} more would take the query past its budget. Reading it never
     * changes any count; the caller decides what to do about it.
     */
    public boolean wouldCrossBudget(long estimate) {
        return used.get() + estimate > budgetBytes;
    }

    /**
     * A new view for one driver, or for the query's own operations. Views are cheap and are never pooled.
     */
    public View newView() {
        return new View();
    }

    @Override
    public CircuitBreaker parentBreaker() {
        return parent;
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        parent.circuitBreak(fieldName, bytesNeeded);
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        parent.addEstimateBytesAndMaybeBreak(bytes, label);
        used.addAndGet(bytes);
    }

    @Override
    public void addWithoutBreaking(long bytes) {
        parent.addWithoutBreaking(bytes);
        used.addAndGet(bytes);
    }

    /**
     * This query's bytes, not the parent's: the request breaker's total is read from the parent.
     */
    @Override
    public long getUsed() {
        return used.get();
    }

    @Override
    public long getLimit() {
        return parent.getLimit();
    }

    @Override
    public double getOverhead() {
        return parent.getOverhead();
    }

    @Override
    public long getTrippedCount() {
        return parent.getTrippedCount();
    }

    @Override
    public String getName() {
        return parent.getName();
    }

    @Override
    public Durability getDurability() {
        return parent.getDurability();
    }

    /**
     * The limit belongs to the request breaker; a query must not move it.
     */
    @Override
    public void setLimitAndOverhead(long limit, double overhead) {
        throw new UnsupportedOperationException("the limit of a query breaker is its parent's");
    }

    @Override
    public String toString() {
        return "QueryBreaker[used=" + used.get() + ", budget=" + budgetBytes + "]";
    }

    /**
     * One driver's window onto the query breaker, carrying that driver's reservation credit.
     * <p>
     * {@link #reserve} charges the parent once; every later charge through the view, positive, consumes the credit first and
     * sends only the excess on; a release, negative, goes to the query and the parent and never refills the credit.
     * {@link #releaseReserve} returns what was not consumed. The view is as thread-safe as the breaker above it, so a
     * block allocated through it may be released from another thread, or after the driver has closed; closing the view
     * only returns its unconsumed credit.
     */
    public final class View implements DelegatingCircuitBreaker, Releasable {

        private final AtomicLong credit = new AtomicLong();
        private volatile boolean closed;
        private final CircuitBreakerService service = new CircuitBreakerService() {
            @Override
            public CircuitBreaker getBreaker(String name) {
                return View.this;
            }

            @Override
            public AllCircuitBreakerStats stats() {
                throw new UnsupportedOperationException("a query breaker view has no stats");
            }

            @Override
            public CircuitBreakerStats stats(String name) {
                throw new UnsupportedOperationException("a query breaker view has no stats");
            }
        };

        private View() {}

        /**
         * Charge {@code bytes} to the parent now, as credit for the allocations that follow. A failure leaves the credit
         * untouched, so the caller's state is still consistent and it may spill first and reserve again.
         */
        public void reserve(long bytes, String label) throws CircuitBreakingException {
            if (bytes <= 0) {
                return;
            }
            QueryBreaker.this.addEstimateBytesAndMaybeBreak(bytes, label);
            credit.addAndGet(bytes);
            if (closed) {
                releaseReserve();
            }
        }

        /**
         * Return the credit that was not consumed. The result is what went back to the parent.
         */
        public long releaseReserve() {
            long unconsumed = credit.getAndSet(0);
            if (unconsumed > 0) {
                QueryBreaker.this.addWithoutBreaking(-unconsumed);
            }
            return unconsumed;
        }

        /**
         * Reserved bytes not yet consumed by an allocation.
         */
        public long credit() {
            return credit.get();
        }

        /**
         * A breaker service answering this view for any name, for
         * {@link org.elasticsearch.common.util.BigArrays#withBreakerService}, so that the driver's arrays consume the same
         * reservation as its blocks.
         */
        public CircuitBreakerService service() {
            return service;
        }

        public QueryBreaker queryBreaker() {
            return QueryBreaker.this;
        }

        @Override
        public CircuitBreaker parentBreaker() {
            return QueryBreaker.this;
        }

        /**
         * Two views of one query keep one account: a block charged through one may be released through the other.
         */
        @Override
        public boolean sharesAccountWith(CircuitBreaker other) {
            return other == this || (other instanceof View view && view.queryBreaker() == QueryBreaker.this);
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            QueryBreaker.this.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (bytes <= 0) {
                addWithoutBreaking(bytes);
                return;
            }
            long consumed = consume(bytes);
            long excess = bytes - consumed;
            if (excess > 0) {
                try {
                    QueryBreaker.this.addEstimateBytesAndMaybeBreak(excess, label);
                } catch (CircuitBreakingException e) {
                    credit.addAndGet(consumed);
                    if (closed) {
                        // the view closed while this charge was in flight; its close may have missed the restored credit
                        releaseReserve();
                    }
                    throw e;
                }
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            if (bytes > 0) {
                long excess = bytes - consume(bytes);
                if (excess > 0) {
                    QueryBreaker.this.addWithoutBreaking(excess);
                }
            } else if (bytes < 0) {
                QueryBreaker.this.addWithoutBreaking(bytes);
            }
        }

        private long consume(long bytes) {
            while (true) {
                long current = credit.get();
                long take = Math.min(current, bytes);
                if (take <= 0) {
                    return 0;
                }
                if (credit.compareAndSet(current, current - take)) {
                    return take;
                }
            }
        }

        @Override
        public long getUsed() {
            return QueryBreaker.this.getUsed();
        }

        @Override
        public long getLimit() {
            return QueryBreaker.this.getLimit();
        }

        @Override
        public double getOverhead() {
            return QueryBreaker.this.getOverhead();
        }

        @Override
        public long getTrippedCount() {
            return QueryBreaker.this.getTrippedCount();
        }

        @Override
        public String getName() {
            return QueryBreaker.this.getName();
        }

        @Override
        public Durability getDurability() {
            return QueryBreaker.this.getDurability();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            QueryBreaker.this.setLimitAndOverhead(limit, overhead);
        }

        /**
         * Returns the unconsumed credit. Charges may still arrive afterwards, from a worker that outlived the driver or a
         * late restore, and they are accounted correctly: a closed view holds no credit, so they go straight to the query.
         */
        @Override
        public void close() {
            closed = true;
            releaseReserve();
        }

        @Override
        public String toString() {
            return "QueryBreaker.View[credit=" + credit.get() + ", " + QueryBreaker.this + "]";
        }
    }
}
