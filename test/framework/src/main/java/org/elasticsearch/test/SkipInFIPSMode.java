/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.core.Booleans;
import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A TestRule that skips respective tests when FIPS mode is enabled rather than the entire test configuration as
 * typically done using the following Gradle snippet:
 * <pre>
 * tasks.named("javaRestTest") {
 *   buildParams.withFipsEnabledOnly(it)
 * }
 * </pre>
 */
public class SkipInFIPSMode implements TestRule {
    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                if (Booleans.parseBoolean(System.getProperty("tests.fips.enabled"), false)) {
                    throw new AssumptionViolatedException("Skipping " + description.getClassName() + " when running with FIPS enabled");
                }
                base.evaluate();
            }
        };
    }
}
