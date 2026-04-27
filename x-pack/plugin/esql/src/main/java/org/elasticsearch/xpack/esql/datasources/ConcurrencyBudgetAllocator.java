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
 * Manages per-query concurrency budgets across active queries. Each registered query receives a
 * fair share of the total concurrency budget ({@code max(MIN_PERMITS_PER_QUERY, totalBudget / activeQueries)}),
 * dynamically rebalanced as queries register and deregister. When the total budget is 0
 * (concurrency limiting disabled), all registrations return unlimited budgets.
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
     * Registers a new query and returns its concurrency budget. Existing budgets are rebalanced
     * to accommodate the new query.
     */
    QueryConcurrencyBudget register() {
        if (totalBudget <= 0) {
            return QueryConcurrencyBudget.UNLIMITED;
        }
        int perQuery = computePerQuery(activeBudgets.size() + 1);
        QueryConcurrencyBudget budget = new QueryConcurrencyBudget(perQuery, acquireTimeoutMs, this);
        activeBudgets.add(budget);
        rebalance();
        logger.debug("Registered query budget, active queries [{}], per-query permits [{}]", activeBudgets.size(), perQuery);
        return budget;
    }

    /**
     * Removes a query's budget and rebalances remaining active budgets.
     */
    void deregister(QueryConcurrencyBudget budget) {
        if (activeBudgets.remove(budget)) {
            rebalance();
            int count = activeBudgets.size();
            logger.debug("Deregistered query budget, active queries [{}], per-query permits [{}]", count, computePerQuery(count));
        }
    }

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
