/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.test;

import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import junit.framework.AssertionFailedError;
import org.junit.Assert;
import org.junit.runner.RunWith;

@RunWith(RandomizedRunner.class)
@TestMethodProviders({ JUnit4MethodProvider.class, JUnit3MethodProvider.class })
@ThreadLeakLingering(linger = 5000) // wait for "Connection worker" to die
public abstract class BaseTestCase extends Assert {

    // add expectThrows from junit 5
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Throwable;
    }

    public static <T extends Throwable> T expectThrows(Class<T> expectedType, ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            if (expectedType.isInstance(e)) {
                return expectedType.cast(e);
            }
            AssertionFailedError assertion = new AssertionFailedError(
                "Unexpected exception type, expected " + expectedType.getSimpleName() + " but got " + e
            );
            assertion.initCause(e);
            throw assertion;
        }
        throw new AssertionFailedError("Expected exception " + expectedType.getSimpleName() + " but no exception was thrown");
    }
}
