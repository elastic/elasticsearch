/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.core.Assertions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fail points can be used to modify behavior anywhere in the code for testing. Typically, code blocks can be added to pause an operation or
 * artificially throw an error. For example, suppose a node must start an operation, but then it's desirable to pause it to allow a second
 * operation to race ahead before the first is allowed to resume -- exercising a potential race condition.
 * <p>
 *
 * A fail point should be statically registered.
 * <pre>{@code
 * public class Foo {
 *     public static final String failPointName;
 *     static {
 *         failPointName = "my-fail-point";
 *         FailPoints.register(failPointName);
 *     }
 *     ....
 * }
 * }</pre>
 * Note that fail point names cannot be duplicated.
 * <p>
 *
 * Then the fail point can be used anywhere in that class to inject artificial behavior.
 * <pre>{@code
 * while (FailPoints.ENABLED && FailPoints.isActive(failPointName)) {
 *     ....
 * }
 * }</pre>
 * It is important to check FailPoints.ENABLED first. It is a static value and the compiler will optimize the path away outside of testing.
 * <p>
 *
 * A test class can then activate and deactivate a particular fail point as desired.
 * <pre>{@code
 * public void testWithFailPoint() throws Exception {
 *     FailPoints.activate(Foo.failPointName);
 *     ....
 *     FailPoints.deactivate(Foo.failPointName);
 * }
 * }</pre>
 */
public class FailPoints {
    // Only enable failpoints when assertions are enabled.
    public static final boolean ENABLED = Assertions.ENABLED;

    // Optimize the size to zero for non-testing compiles.
    private static final Map<String, Boolean> failpoints = new ConcurrentHashMap<>(0);

    public static void register(String failPointName) {
        if (ENABLED == false) {
            // No purpose in populating a map that will never be used.
            return;
        }

        assert failPointName.isEmpty() == false;
        assert failpoints.containsKey(failPointName) == false : "Already registered failpoint '" + failPointName + "'.";
        failpoints.put(failPointName, false);
    }

    public static boolean isActive(String failPointName) {
        checkTestingIsActive();
        checkFailPointIsRegistered(failPointName);
        return failpoints.get(failPointName);
    }

    public static void activate(String failPointName) {
        checkTestingIsActive();
        checkFailPointIsRegistered(failPointName);
        failpoints.replace(failPointName, true);
    }

    public static void deactivate(String failPointName) {
        checkTestingIsActive();
        checkFailPointIsRegistered(failPointName);
        failpoints.replace(failPointName, false);
    }

    public static void deactivateAll() {
        checkTestingIsActive();
        for (var entry : failpoints.entrySet()) {
            entry.setValue(false);
        }
    }

    /**
     * Throws if FailPoints methods are called outside of testing.
     */
    private static void checkTestingIsActive() {
        if (ENABLED == false) {
            throw new IllegalStateException("FailPoints were used outside of testing.");
        }
    }

    /**
     * Throws if a caller attempts to use a fail point that was not first registered.
     */
    private static void checkFailPointIsRegistered(String failPointName) {
        assert failpoints.containsKey(failPointName) : "Did not find failpoint " + failPointName + ". It is not registered.";
    }
}
