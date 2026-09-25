/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.tracking;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.test.ESTestCase;

public class BreakerPageBudgetTests extends ESTestCase {

    public void testItGivesBackEverythingItTook() {
        final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        final BreakerPageBudget budget = new BreakerPageBudget(breaker);

        long taken = 0;
        for (int i = 0; i < between(1, 10); i++) {
            final long bytes = between(1, 4096);
            budget.charge(bytes);
            taken += bytes;
        }
        assertEquals("the breaker holds what the page took", taken, breaker.getUsed());

        budget.close();
        assertEquals("closing the budget gives all of it back", 0L, breaker.getUsed());
    }

    public void testABudgetThatWasNeverChargedGivesNothingBack() {
        final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        new BreakerPageBudget(breaker).close();
        assertEquals(0L, breaker.getUsed());
    }

    /**
     * A page the breaker has no room for is refused, and what earlier pages took is still given back: the
     * refusal leaves the budget holding what it holds rather than losing track of it.
     */
    public void testARefusalDoesNotLoseWhatWasAlreadyTaken() {
        final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofKb(8));
        final BreakerPageBudget budget = new BreakerPageBudget(breaker);

        budget.charge(1024);
        expectThrows(CircuitBreakingException.class, () -> budget.charge(ByteSizeValue.ofMb(1).getBytes()));
        assertEquals("a refused charge is not held", 1024L, breaker.getUsed());

        budget.close();
        assertEquals(0L, breaker.getUsed());
    }

    public void testClosingTwiceGivesBackNothingMore() {
        final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        final BreakerPageBudget budget = new BreakerPageBudget(breaker);
        budget.charge(2048);
        budget.close();
        budget.close();
        assertEquals(0L, breaker.getUsed());
    }

    /** Charging a budget that has given its bytes back would account them to nothing, so it is refused. */
    public void testAReleasedBudgetIsNotChargedAgain() {
        assumeTrue("the guard is an assertion", Assertions.ENABLED);
        final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        final BreakerPageBudget budget = new BreakerPageBudget(breaker);
        budget.charge(512);
        budget.close();
        expectThrows(AssertionError.class, () -> budget.charge(512));
        assertEquals(0L, breaker.getUsed());
    }
}
