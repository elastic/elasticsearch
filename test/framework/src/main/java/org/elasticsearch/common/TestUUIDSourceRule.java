/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomLong;

/**
 * Seeds {@link TestUUIDSource} from the current test randomness for the duration of the wrapped suite or test method.
 */
public final class TestUUIDSourceRule implements TestRule {

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try (var ignored = TestUUIDSource.withSeed(randomLong())) {
                    base.evaluate();
                }
            }
        };
    }
}
