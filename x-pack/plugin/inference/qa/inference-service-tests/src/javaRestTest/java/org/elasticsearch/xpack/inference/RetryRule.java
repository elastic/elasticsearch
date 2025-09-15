/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Provides a way to retry a failed test. To use this functionality add something like the following to your test class:
 * <br/>
 * {@literal @}Rule
 * <br/>
 * <code>public RetryRule retry = new RetryRule(3, TimeValue.timeValueSeconds(1));</code>
 * <br/>
 * See {@link InferenceGetServicesIT#retry} for an example.
 */
public class RetryRule implements TestRule {
    private static final Logger logger = LogManager.getLogger(RetryRule.class);
    private final int maxAttempts;
    private final TimeValue retryDelay;

    public RetryRule(int maxAttempts, TimeValue retryDelay) {
        this.maxAttempts = maxAttempts;
        this.retryDelay = Objects.requireNonNull(retryDelay);
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Throwable lastThrowable = null;
                for (int i = 0; i < maxAttempts; i++) {
                    try {
                        logger.info(Strings.format("Running test [%s] attempt [%d/%d]", description.getMethodName(), i + 1, maxAttempts));
                        statement.evaluate();
                        logger.info(
                            Strings.format("Test [%s] succeeded on attempt [%d/%d]", description.getMethodName(), i + 1, maxAttempts)
                        );
                        // Test succeeded so we'll return
                        return;
                    } catch (Throwable t) {
                        logger.info(
                            Strings.format(
                                "Test [%s] failed with exception: %s, attempt [%d/%d]",
                                description.getMethodName(),
                                t.getMessage(),
                                i + 1,
                                maxAttempts
                            )
                        );
                        lastThrowable = t;
                        // if this was the last iteration then let's skip sleeping
                        if (i < maxAttempts - 1) {
                            TimeUnit.MICROSECONDS.sleep(retryDelay.millis());
                        }
                    }
                }

                // if the test failed we should have the throwable, so let's bubble up that failure
                if (lastThrowable != null) {
                    logger.info(Strings.format("Test [%s] failed and exceeded retry limit, failing test.", description.getMethodName()));
                    throw lastThrowable;
                }
            }
        };
    }
}
