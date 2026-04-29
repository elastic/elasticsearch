/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

public class ConcurrencyBudgetAllocatorTests extends ESTestCase {

    public void testSingleQueryGetsFullBudget() {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(50);
        QueryConcurrencyBudget budget = allocator.register();
        assertEquals(50, budget.maxPermits());
        assertEquals(1, allocator.activeQueryCount());
        budget.close();
    }

    public void testTwoQueriesGetHalf() {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(50);
        QueryConcurrencyBudget b1 = allocator.register();
        QueryConcurrencyBudget b2 = allocator.register();

        assertEquals(2, allocator.activeQueryCount());
        assertEquals(25, b1.maxPermits());
        assertEquals(25, b2.maxPermits());

        b1.close();
        b2.close();
    }

    public void testFiveQueriesGetTenEach() {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(50);
        QueryConcurrencyBudget[] budgets = new QueryConcurrencyBudget[5];
        for (int i = 0; i < 5; i++) {
            budgets[i] = allocator.register();
        }
        assertEquals(5, allocator.activeQueryCount());
        for (QueryConcurrencyBudget b : budgets) {
            assertEquals(10, b.maxPermits());
        }
        for (QueryConcurrencyBudget b : budgets) {
            b.close();
        }
    }

    public void testMinPermitsFloor() {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(10);
        QueryConcurrencyBudget[] budgets = new QueryConcurrencyBudget[50];
        for (int i = 0; i < 50; i++) {
            budgets[i] = allocator.register();
        }
        assertEquals(50, allocator.activeQueryCount());
        for (QueryConcurrencyBudget b : budgets) {
            assertEquals(ConcurrencyBudgetAllocator.MIN_PERMITS_PER_QUERY, b.maxPermits());
        }
        for (QueryConcurrencyBudget b : budgets) {
            b.close();
        }
    }

    public void testRebalanceOnDeregister() {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(50);
        QueryConcurrencyBudget b1 = allocator.register();
        QueryConcurrencyBudget b2 = allocator.register();
        assertEquals(25, b1.maxPermits());
        assertEquals(25, b2.maxPermits());

        b1.close();
        assertEquals(1, allocator.activeQueryCount());
        assertEquals(50, b2.maxPermits());

        b2.close();
    }

    public void testDisabledWithZeroBudget() {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(0);
        QueryConcurrencyBudget budget = allocator.register();
        assertSame(QueryConcurrencyBudget.UNLIMITED, budget);
        assertFalse(budget.isEnabled());
        assertEquals(0, allocator.activeQueryCount());
    }

    public void testRegisterDeregisterCycle() {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(50);

        QueryConcurrencyBudget b1 = allocator.register();
        assertEquals(50, b1.maxPermits());
        assertEquals(1, allocator.activeQueryCount());

        QueryConcurrencyBudget b2 = allocator.register();
        assertEquals(25, b1.maxPermits());
        assertEquals(25, b2.maxPermits());

        QueryConcurrencyBudget b3 = allocator.register();
        assertEquals(16, b1.maxPermits());
        assertEquals(16, b2.maxPermits());
        assertEquals(16, b3.maxPermits());

        b2.close();
        assertEquals(25, b1.maxPermits());
        assertEquals(25, b3.maxPermits());

        b1.close();
        assertEquals(50, b3.maxPermits());

        b3.close();
        assertEquals(0, allocator.activeQueryCount());
    }

    public void testDoubleDeregister() {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(50);
        QueryConcurrencyBudget budget = allocator.register();
        assertEquals(1, allocator.activeQueryCount());

        budget.close();
        assertEquals(0, allocator.activeQueryCount());

        // Double close should be safe
        budget.close();
        assertEquals(0, allocator.activeQueryCount());
    }

    public void testTotalBudget() {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(42);
        assertEquals(42, allocator.totalBudget());
    }
}
