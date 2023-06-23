/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;

public class AbstractProfileBreakdownTests extends ESTestCase {

    enum TestTimingTypes {
        ONE,
        TWO,
        THREE
    }

    static class TestProfileBreakdown extends AbstractProfileBreakdown<TestTimingTypes> {
        /**
         * Sole constructor
         */
        TestProfileBreakdown() {
            super(TestTimingTypes.class);
        }
    }

    public void testManagingDifferentTimers() {
        TestProfileBreakdown testBreakdown = new TestProfileBreakdown();
        Timer firstTimerOne = testBreakdown.getNewTimer(TestTimingTypes.ONE);
        runTimer(firstTimerOne);
        assertThat(firstTimerOne.getCount(), equalTo(1L));
        long firstTimerOneTime = firstTimerOne.getApproximateTiming();

        Timer secondTimerOne = testBreakdown.getNewTimer(TestTimingTypes.ONE);
        runTimer(secondTimerOne);
        // test that running second timer doesn't affect first timers count etc...
        assertThat(firstTimerOne.getCount(), equalTo(1L));
        assertThat(firstTimerOne.getApproximateTiming(), equalTo(firstTimerOneTime));
    }

    public void testGetBreakdownAndNodeTime() {
        TestProfileBreakdown testBreakdown = new TestProfileBreakdown();

        Timer firstTimerOne = testBreakdown.getNewTimer(TestTimingTypes.ONE);
        int firstTimerOneCount = randomIntBetween(10, 20);
        runTimerNTimes(firstTimerOne, firstTimerOneCount);

        Timer firstTimerTwo = testBreakdown.getNewTimer(TestTimingTypes.TWO);
        int firstTimerTwoCount = randomIntBetween(10, 20);
        runTimerNTimes(firstTimerTwo, firstTimerTwoCount);

        Timer secondTimerTwo = testBreakdown.getNewTimer(TestTimingTypes.TWO);
        int secondTimerTwoCount = randomIntBetween(10, 20);
        runTimerNTimes(secondTimerTwo, secondTimerTwoCount);

        // check behaviour if one timer type hasn't been created and used
        Map<String, Long> breakdownMap = testBreakdown.toBreakdownMap();
        assertThat(breakdownMap.size(), equalTo(6));
        assertThat(breakdownMap.get(TestTimingTypes.ONE.name()), equalTo(firstTimerOne.getApproximateTiming()));
        assertThat(breakdownMap.get(TestTimingTypes.ONE.name() + "_count"), equalTo(firstTimerOne.getCount()));
        assertThat(
            breakdownMap.get(TestTimingTypes.TWO.name()),
            equalTo(firstTimerTwo.getApproximateTiming() + secondTimerTwo.getApproximateTiming())
        );
        assertThat(breakdownMap.get(TestTimingTypes.TWO.name() + "_count"), equalTo(firstTimerTwo.getCount() + secondTimerTwo.getCount()));
        assertThat(breakdownMap.get(TestTimingTypes.THREE.name()), equalTo(0L));
        assertThat(breakdownMap.get(TestTimingTypes.THREE.name() + "_count"), equalTo(0L));

        Timer firstTimerThree = testBreakdown.getNewTimer(TestTimingTypes.THREE);
        int firstTimerThreeCount = randomIntBetween(10, 20);
        runTimerNTimes(firstTimerThree, firstTimerThreeCount);

        breakdownMap = testBreakdown.toBreakdownMap();
        assertThat(breakdownMap.size(), equalTo(6));
        assertThat(breakdownMap.get(TestTimingTypes.ONE.name()), equalTo(firstTimerOne.getApproximateTiming()));
        assertThat(breakdownMap.get(TestTimingTypes.ONE.name() + "_count"), equalTo(firstTimerOne.getCount()));
        assertThat(
            breakdownMap.get(TestTimingTypes.TWO.name()),
            equalTo(firstTimerTwo.getApproximateTiming() + secondTimerTwo.getApproximateTiming())
        );
        assertThat(breakdownMap.get(TestTimingTypes.TWO.name() + "_count"), equalTo(firstTimerTwo.getCount() + secondTimerTwo.getCount()));
        assertThat(breakdownMap.get(TestTimingTypes.THREE.name()), equalTo(firstTimerThree.getApproximateTiming()));
        assertThat(breakdownMap.get(TestTimingTypes.THREE.name() + "_count"), equalTo(firstTimerThree.getCount()));

        assertThat(
            testBreakdown.toNodeTime(),
            equalTo(
                firstTimerOne.getApproximateTiming() + firstTimerTwo.getApproximateTiming() + secondTimerTwo.getApproximateTiming()
                    + firstTimerThree.getApproximateTiming()
            )
        );
    }

    public void testMultiThreaded() throws InterruptedException {
        TestProfileBreakdown testBreakdown = new TestProfileBreakdown();
        Thread[] threads = new Thread[2000];
        final CountDownLatch latch = new CountDownLatch(1);
        int startsPerThread = between(1, 5);
        for (int t = 0; t < threads.length; t++) {
            final TestTimingTypes timingType = randomFrom(TestTimingTypes.values());
            threads[t] = new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Timer timer = testBreakdown.getNewTimer(timingType);
                for (int runs = 0; runs < startsPerThread; runs++) {
                    timer.start();
                    timer.stop();
                }
            });
            threads[t].start();
        }
        latch.countDown();
        for (Thread t : threads) {
            t.join();
        }
        Map<String, Long> breakdownMap = testBreakdown.toBreakdownMap();
        long totalCounter = breakdownMap.get(TestTimingTypes.ONE + "_count") + breakdownMap.get(TestTimingTypes.TWO + "_count")
            + breakdownMap.get(TestTimingTypes.THREE + "_count");
        assertEquals(threads.length * startsPerThread, totalCounter);
    }

    private void runTimerNTimes(Timer t, int n) {
        for (int i = 0; i < n; i++) {
            runTimer(t);
        }
    }

    private void runTimer(Timer t) {
        t.start();
        t.stop();
    }

}
