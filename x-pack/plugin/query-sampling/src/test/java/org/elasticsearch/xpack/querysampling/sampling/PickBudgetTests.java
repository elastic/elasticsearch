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

    private final AtomicLong clock = new AtomicLong();
    private final PickBudget budget = new PickBudget(clock::get);

    private void passSeconds(long seconds) {
        clock.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
    }

    /** How many picks can be made right now, one after the other. */
    private int picksAvailable(int atMost) {
        int picks = 0;
        while (picks < atMost && budget.available()) {
            budget.take();
            picks++;
        }
        return picks;
    }

    public void testThereIsNoLimitUntilOneIsSetAndZeroRemovesIt() {
        assertThat(picksAvailable(1000), equalTo(1000));

        budget.perHour(3600);
        assertThat(picksAvailable(1000), equalTo(60));

        budget.perHour(0);
        assertThat("zero is no limit", picksAvailable(1000), equalTo(1000));
    }

    public void testANewLimitStartsWithAMinuteOfPicks() {
        budget.perHour(3600); // one a second

        assertThat(picksAvailable(1000), equalTo(60));
        assertFalse(budget.available());
    }

    public void testPicksComeBackAtTheRateOfTheLimit() {
        budget.perHour(3600);
        assertThat(picksAvailable(1000), equalTo(60));

        passSeconds(2);

        assertThat("two seconds, two picks", picksAvailable(1000), equalTo(2));
    }

    public void testAQuietSpellCannotBankMoreThanAMinuteOfPicks() {
        budget.perHour(3600);
        assertThat(picksAvailable(1000), equalTo(60));

        passSeconds(3600);

        assertThat(picksAvailable(1000), equalTo(60));
    }

    public void testALimitBelowOnePickAMinuteStillHoldsOnePick() {
        budget.perHour(30); // one every two minutes

        assertThat(picksAvailable(10), equalTo(1));
        passSeconds(119);
        assertFalse("not yet", budget.available());
        passSeconds(1);
        assertTrue("two minutes after the last", budget.available());
    }

    public void testChangingTheLimitKeepsThePicksThatAreLeftUpToTheNewMinute() {
        budget.perHour(3600);
        for (int i = 0; i < 10; i++) {
            budget.take();
        }

        budget.perHour(7200); // a minute is 120 now, but the budget does not refill because of it
        assertThat("50 were left", picksAvailable(1000), equalTo(50));

        budget.perHour(3600);
        passSeconds(3600);
        budget.perHour(60); // a minute holds one pick now
        assertThat(picksAvailable(1000), equalTo(1));
    }

    public void testALimitSetAfterNoneStartsFull() {
        budget.perHour(3600);
        picksAvailable(1000);
        budget.perHour(0);

        budget.perHour(3600);

        assertThat(picksAvailable(1000), equalTo(60));
    }
}
