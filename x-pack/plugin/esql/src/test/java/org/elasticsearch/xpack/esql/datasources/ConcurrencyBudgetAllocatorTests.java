/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.logging.log4j.Level;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

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

    public void testSchemeScopedGateName() {
        ConcurrencyBudgetAllocator unnamed = new ConcurrencyBudgetAllocator(10);
        assertEquals("budget", unnamed.name());
        ConcurrencyBudgetAllocator s3 = new ConcurrencyBudgetAllocator(10, 50L, null, "s3");
        assertEquals("budget/s3", s3.name());
    }

    @TestLogging(
        value = "org.elasticsearch.xpack.esql.datasources.ConcurrencyBudgetAllocator:DEBUG",
        reason = "register/deregister DEBUG names the query id"
    )
    public void testRegisterLogsDistinctQueryIdsThenDeregisters() {
        TaskId first = new TaskId("coord", 11);
        TaskId second = new TaskId("coord", 22);
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(50);
        QueryConcurrencyBudget b1;
        QueryConcurrencyBudget b2;
        try (MockLog mockLog = MockLog.capture(ConcurrencyBudgetAllocator.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "register first",
                    ConcurrencyBudgetAllocator.class.getCanonicalName(),
                    Level.DEBUG,
                    "*Registered query budget [coord:11]*"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "register second",
                    ConcurrencyBudgetAllocator.class.getCanonicalName(),
                    Level.DEBUG,
                    "*Registered query budget [coord:22]*"
                )
            );
            b1 = allocator.register(first);
            b2 = allocator.register(second);
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(first, b1.queryId());
        assertEquals(second, b2.queryId());
        assertEquals(2, allocator.activeQueryCount());

        try (MockLog mockLog = MockLog.capture(ConcurrencyBudgetAllocator.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "deregister first",
                    ConcurrencyBudgetAllocator.class.getCanonicalName(),
                    Level.DEBUG,
                    "*Deregistered query budget [coord:11]*"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "deregister second",
                    ConcurrencyBudgetAllocator.class.getCanonicalName(),
                    Level.DEBUG,
                    "*Deregistered query budget [coord:22]*"
                )
            );
            b1.close();
            b2.close();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(0, allocator.activeQueryCount());
    }

    public void testBudgetWaiterLabelIncludesQueryId() throws Exception {
        List<String> waiters = new ArrayList<>();
        AdmissionTracker tracker = new AdmissionTracker() {
            @Override
            public Wait waitStarted(String gate, String waiter) {
                waiters.add(waiter);
                return NOOP_WAIT;
            }
        };
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(2, 50L, tracker, "s3");
        QueryConcurrencyBudget budget = allocator.register(new TaskId("coord", 7));
        budget.acquire();
        budget.acquire();
        expectThrows(TimeoutException.class, budget::acquire);
        assertEquals(1, waiters.size());
        assertEquals("coord:7/" + Thread.currentThread().getName(), waiters.get(0));
        budget.release();
        budget.release();
        budget.close();
        assertEquals(0, allocator.activeQueryCount());
    }

    public void testBindStampsQueryIdOnLease() {
        ConcurrencyBudgetAllocator allocator = new ConcurrencyBudgetAllocator(10);
        QueryConcurrencyBudget budget = allocator.register(new TaskId("coord", 9));
        RowGroupIo lease = new RowGroupIo();
        budget.bind(lease);
        assertEquals(new TaskId("coord", 9), lease.queryId());
        budget.bind(lease);
        assertEquals(new TaskId("coord", 9), lease.queryId());
        budget.close();
    }

    public void testWaiterLabelOmitsUnsetAndCapsLength() {
        assertEquals("lease#1", QueryConcurrencyBudget.waiterLabel(null, "lease#1"));
        assertEquals("lease#1", QueryConcurrencyBudget.waiterLabel(TaskId.EMPTY_TASK_ID, "lease#1"));
        String id = QueryConcurrencyBudget.waiterLabel(new TaskId("coord", 3), "lease#1");
        assertEquals("coord:3/lease#1", id);
        TaskId longId = new TaskId("n".repeat(60), 1L);
        String capped = QueryConcurrencyBudget.waiterLabel(longId, "lease#1");
        assertEquals(QueryConcurrencyBudget.QUERY_ID_LABEL_MAX + "/lease#1".length(), capped.length());
        assertTrue(capped.contains(":1/lease#1"));
        assertFalse(capped.contains(longId.toString()));
    }
}
