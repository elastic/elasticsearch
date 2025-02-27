/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.test.ESTestCase;

/**
 * Tests the mechanics of using {@link FailPoints}.
 */
public class FailPointsTests extends ESTestCase {
    public static final String failPointName1;
    public static final String failPointName2;
    static {
        failPointName1 = "my-fail-point-1";
        failPointName2 = "my-fail-point-2";
        FailPoints.register(failPointName1);
        FailPoints.register(failPointName2);
    }

    /**
     * Exercises the FailPoints API.
     */
    public void testFailPoints() {
        assertTrue(FailPoints.ENABLED);

        // Check that fail points are false by default.
        assertFalse(FailPoints.isActive(failPointName1));
        assertFalse(FailPoints.isActive(failPointName2));

        // Check that a fail point activates and deactivates.
        FailPoints.activate(failPointName1);
        assertTrue(FailPoints.isActive(failPointName1));
        FailPoints.deactivate(failPointName1);
        assertFalse(FailPoints.isActive(failPointName1));

        // Check that fail points can be all deactivated at once.
        FailPoints.activate(failPointName1);
        FailPoints.activate(failPointName2);
        assertTrue(FailPoints.isActive(failPointName1));
        assertTrue(FailPoints.isActive(failPointName2));
        FailPoints.deactivateAll();
        assertFalse(FailPoints.isActive(failPointName1));
        assertFalse(FailPoints.isActive(failPointName2));
    }

    /**
     * A thread that will deactivate {@link #failPointName1} if that fail point is found to be active.
     */
    public class DeactivatingFailPoint1Thread extends Thread {
        public void run() {
            assertTrue(FailPoints.ENABLED);
            while (FailPoints.isActive(failPointName1)) {
                FailPoints.deactivate(failPointName1);
            }
        }
    }

    /**
     * Runs code in another thread that uses a fail point.
     */
    public void testFailPointInThread() throws InterruptedException {
        DeactivatingFailPoint1Thread myThread = new DeactivatingFailPoint1Thread();
        FailPoints.activate(failPointName1);
        assertTrue(FailPoints.isActive(failPointName1));
        myThread.start();
        myThread.join();
        assertFalse("Thread failed to deactivate fail point.", FailPoints.isActive(failPointName1));
    }
}
