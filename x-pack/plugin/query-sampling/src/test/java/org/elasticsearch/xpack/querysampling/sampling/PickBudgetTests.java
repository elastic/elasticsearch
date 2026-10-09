/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class PickBudgetTests extends ESTestCase {

    private final AtomicLong now = new AtomicLong();
    private final PickBudget budget = new PickBudget(now::get);

    public void testThereIsNoLimitUntilOneIsSet() {
        for (int i = 0; i < 1000; i++) {
            assertTrue(budget.available());
            budget.take();
        }
    }

    public void testAZeroLimitIsNoLimit() {
        budget.perHour(0);

        for (int i = 0; i < 1000; i++) {
            assertTrue(budget.available());
            budget.take();
        }
    }

    public void testTheBucketStartsFullAndIsEmptiedByPicks() {
        budget.perHour(3600); // one a second, and a minute's worth fits in the bucket

        for (int i = 0; i < 60; i++) {
            assertTrue("pick " + i, budget.available());
            budget.take();
        }
        assertFalse(budget.available());
    }

    public void testTokensComeBackAtTheRateOfTheLimit() {
        budget.perHour(3600);
        for (int i = 0; i < 60; i++) {
            budget.take();
        }
        assertFalse(budget.available());

        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(1500));
        assertTrue("one token after a second and a half", budget.available());
        budget.take();
        assertFalse("and not two", budget.available());
    }

    public void testAQuietSpellDoesNotStoreUpMoreThanABurst() {
        budget.perHour(3600);

        now.addAndGet(TimeUnit.HOURS.toNanos(1));
        int picks = 0;
        while (budget.available() && picks < 1000) {
            budget.take();
            picks++;
        }

        assertThat(picks, equalTo(60));
    }

    public void testAVerySmallLimitStillAllowsOnePickAtATime() {
        budget.perHour(1);

        assertTrue(budget.available());
        budget.take();
        assertFalse(budget.available());

        now.addAndGet(TimeUnit.HOURS.toNanos(1));
        assertTrue("an hour later", budget.available());
    }

    public void testChangingTheLimitKeepsWhatIsInTheBucket() {
        budget.perHour(3600);
        for (int i = 0; i < 60; i++) {
            budget.take();
        }

        budget.perHour(7200);
        assertFalse("raising the limit does not refill the bucket", budget.available());

        budget.perHour(0);
        assertTrue("removing it lifts it", budget.available());

        budget.perHour(3600);
        assertTrue("a limit that is set again starts with a full bucket", budget.available());
    }
}
