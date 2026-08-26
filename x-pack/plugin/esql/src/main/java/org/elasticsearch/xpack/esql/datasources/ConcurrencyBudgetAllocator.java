/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages per-source-node concurrency budgets across active external-source operators. Each
 * registered source plan node receives a fair share of the total concurrency budget
 * ({@code max(MIN_PERMITS_PER_QUERY, totalBudget / activeCount)}), dynamically rebalanced as
 * operators register and deregister. When the total budget is 0 (concurrency limiting disabled),
 * all registrations return unlimited budgets.
 *
 * <p>Note: a single ESQL query with multiple external-file sources (joins, unions, LOOKUP, etc.)
 * will register separate budgets for each source plan node. This is intentional — each source
 * operates independently and may target different files/prefixes.
 */
class ConcurrencyBudgetAllocator {

    private static final Logger logger = LogManager.getLogger(ConcurrencyBudgetAllocator.class);

    static final int MIN_PERMITS_PER_QUERY = 2;

    private final int totalBudget;
    private final long acquireTimeoutMs;
    private final Set<QueryConcurrencyBudget> activeBudgets = ConcurrentHashMap.newKeySet();

    ConcurrencyBudgetAllocator(int totalBudget, long acquireTimeoutMs) {
        this.totalBudget = totalBudget;
        this.acquireTimeoutMs = acquireTimeoutMs;
    }

    ConcurrencyBudgetAllocator(int totalBudget) {
        this(totalBudget, 60_000L);
    }

    /**
     * Registers a new source-node and returns its concurrency budget. Existing budgets are
     * rebalanced to accommodate the new entrant.
     */
    QueryConcurrencyBudget register() {
        if (totalBudget <= 0) {
            return QueryConcurrencyBudget.UNLIMITED;
        }
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(MIN_PERMITS_PER_QUERY, acquireTimeoutMs, this);
        activeBudgets.add(budget);
        rebalance();
        int count = activeBudgets.size();
        logger.debug("Registered query budget, active sources [{}], per-source permits [{}]", count, computePerQuery(count));
        return budget;
    }

    /**
     * Removes a query's budget and rebalances remaining active budgets.
     */
    void deregister(QueryConcurrencyBudget budget) {
        if (activeBudgets.remove(budget)) {
            rebalance();
            int count = activeBudgets.size();
            logger.debug("Deregistered query budget, active sources [{}], per-source permits [{}]", count, computePerQuery(count));
        }
    }

    /**
     * Rebalances permits across all active budgets. Iteration over the {@code ConcurrentHashMap}-backed
     * set is weakly consistent — a concurrent register/deregister may or may not be visible. This is
     * intentional: every register/deregister call invokes rebalance at the end, so convergence is
     * guaranteed within one additional registration cycle.
     */
    private void rebalance() {
        int count = activeBudgets.size();
        if (count == 0) {
            return;
        }
        int perQuery = computePerQuery(count);
        for (QueryConcurrencyBudget budget : activeBudgets) {
            if (budget.isClosed() == false) {
                budget.updateMaxPermits(perQuery);
            }
        }
    }

    private int computePerQuery(int activeCount) {
        if (activeCount <= 0) {
            return totalBudget;
        }
        return Math.max(MIN_PERMITS_PER_QUERY, totalBudget / activeCount);
    }

    int activeQueryCount() {
        return activeBudgets.size();
    }

    int totalBudget() {
        return totalBudget;
    }
}
