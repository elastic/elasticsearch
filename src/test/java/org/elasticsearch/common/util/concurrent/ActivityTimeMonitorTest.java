/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.ArrayList;
/**
 * Tests that multi-threaded activities with various timeout settings can 
 * run simultaneously and still trigger timely and appropriate timeout errors.
 */
public class ActivityTimeMonitorTest extends ElasticsearchTestCase{
    
    public static final int MAXIMUM_TIMING_DISCREPANCY_MS = 50;
    
    
    @Test
    public void testSingleTimeout() throws InterruptedException {
        checkTimeoutsFireCorrectly(new TimedActivity(500, 100, 6));
    }
    
    @Test
    public void testSingleNoTimeout() throws InterruptedException {

        checkTimeoutsFireCorrectly(new TimedActivity(500, 10, 5));
    }
    
    @Test
    public void testOneLongNoTimeoutOneShortTimeout() throws InterruptedException {

        TimedActivity goodLong = new TimedActivity(500, 10, 5);
        TimedActivity shortBad = new TimedActivity(100, 50, 5);
        checkTimeoutsFireCorrectly(goodLong,shortBad);
    }
    @Test
    public void testOneLongNoTimeoutManyShortTimeout() throws InterruptedException {

        ArrayList<TimedActivity> activities = new ArrayList<TimedActivity>();
        TimedActivity goodLong = new TimedActivity(500, 10, 5);
        int numShorts = 10;
        activities.add(goodLong);
        for (int i = 0; i < numShorts; i++) {
            activities.add(new TimedActivity(100, 50, 5));
        }
        checkTimeoutsFireCorrectly((TimedActivity[]) activities.toArray(new TimedActivity[activities.size()]));
    }
    @Test
    public void testOneLongTimeoutManyShortNoTimeout() throws InterruptedException {
        ArrayList<TimedActivity> activities = new ArrayList<TimedActivity>();
        TimedActivity goodLong = new TimedActivity(500, 5, 120);
        int numShorts = 10;
        activities.add(goodLong);
        for (int i = 0; i < numShorts; i++) {
            activities.add(new TimedActivity(100, 5, 5));
        }
        checkTimeoutsFireCorrectly((TimedActivity[]) activities.toArray(new TimedActivity[activities.size()]));
    }
    

    @Test 
    public void testTimerReset() throws InterruptedException {
        ActivityTimeMonitor atm = ActivityTimeMonitor.getOrCreateCurrentThreadMonitor();
        atm.start(500);
        Thread.sleep(100);
        atm.checkForTimeout();
        atm.stop();
        atm.start(1000); // extend the time allotted to this thread
        Thread.sleep(500);
        atm.checkForTimeout();
        atm.stop();
    }
    
    @Test
    public void testRandomThreads() throws InterruptedException {
        // Tests for a mix of threads - some of which are expected to timeout
        // while others not
        int numThreads = 100;
        int maxTimeToTake = 2000;
        int maxNumChecks = 50;
        int minTimeBetweenChecks = 5;
        int maxTimeBetweenChecks = 50;

        ArrayList<TimedActivity> activities = new ArrayList<TimedActivity>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            int maxTime = (int) (Math.random() * maxTimeToTake);
            int numChecks = 1 + (int) (Math.random() * maxNumChecks);
            int timeBetweenChecks = minTimeBetweenChecks + (int) (Math.random() * maxTimeBetweenChecks);
            activities.add(new TimedActivity(maxTime, timeBetweenChecks, numChecks));
        }
        checkTimeoutsFireCorrectly((TimedActivity[]) activities.toArray(new TimedActivity[activities.size()]));
        int numTimeouts = 0;
        for (TimedActivity timedActivity : activities) {
            if (timedActivity.didTimeout) {
                numTimeouts++;
            }
        }
        assertTrue("Invalid test parameters - failed to produce a timeout condition", numTimeouts > 0);
        assertEquals("All activities should be completed", 0, ActivityTimeMonitor.getCurrentNumActivities());
    }    
    
    public void checkTimeoutsFireCorrectly(TimedActivity... activities ) throws InterruptedException{
        Thread threads[] = new Thread[activities.length];
        for (int i = 0; i < activities.length; i++) {
            threads[i] = new Thread(activities[i]);
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        for (int i = 0; i < activities.length; i++) {
            activities[i].checkAssertions();
        }
    }
    
    
    static class TimedActivity implements Runnable{
        int maxTimePermitted;
        int checkForTimeoutEvery;
        int numberOfChecks;
        boolean didTimeout = false;
        private long timeTaken;

        public TimedActivity(int maxTimePermitted, int checkForTimeoutEvery, int numberOfChecks) {
            super();
            this.maxTimePermitted = maxTimePermitted;
            this.checkForTimeoutEvery = checkForTimeoutEvery;
            this.numberOfChecks = numberOfChecks;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            ActivityTimeMonitor.getOrCreateCurrentThreadMonitor().start(maxTimePermitted);
            try {
                try {
                    for (int i = 0; i < numberOfChecks; i++) {
                        Thread.sleep(checkForTimeoutEvery);
                        ActivityTimeMonitor.getOrCreateCurrentThreadMonitor().checkForTimeout();
                    }
                } catch (ActivityTimedOutException e) {
                    didTimeout = true;
                } catch (InterruptedException e) {
                    throw new RuntimeException();
                }
            } finally {
                ActivityTimeMonitor.getOrCreateCurrentThreadMonitor().stop();
                timeTaken = System.currentTimeMillis() - start;
            }

        }

        public void checkAssertions() {
            boolean timeoutExpected = timeTaken >= maxTimePermitted;
            long overrun = timeTaken - maxTimePermitted;
            if ((timeoutExpected) && (!didTimeout)) {
                if (overrun <= MAXIMUM_TIMING_DISCREPANCY_MS) {
                    // allow for minor timing discrepancies (<10ms) in heavily
                    // multi-threaded tests
                    // where a thread that overruns marginally escapes the
                    // scheduled overrun
                    // detection logic.
                    return;
                }
            }
            assertEquals("Took " + timeTaken + " but timeout set to " + maxTimePermitted, timeoutExpected, didTimeout);
            boolean wasTooSlowReportingOverrun = overrun > (checkForTimeoutEvery + MAXIMUM_TIMING_DISCREPANCY_MS);
            assertFalse("Too slow reporting timeout - overrun of " + overrun + " with check every " + checkForTimeoutEvery,
                    wasTooSlowReportingOverrun);

        }

    }

}
