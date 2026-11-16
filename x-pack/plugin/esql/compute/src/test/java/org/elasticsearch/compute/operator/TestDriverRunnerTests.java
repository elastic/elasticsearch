/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.TimeValue;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The runner's timeout report: a driver parked on a listener nobody completes is reported with its status, the reason
 * it sleeps on and the pool's threads, so that a hang seen once leaves evidence behind.
 */
public class TestDriverRunnerTests extends ComputeTestCase {
    public void testTimeoutReportsTheDriverAndThePool() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        SubscribableListener<Void> never = new SubscribableListener<>();
        SourceOperator source = new SourceOperator() {
            @Override
            public Page getOutput() {
                return null;
            }

            @Override
            public boolean isFinished() {
                return false;
            }

            @Override
            public IsBlockedResult isBlocked() {
                return new IsBlockedResult(never, "a listener nobody completes");
            }

            @Override
            public void finish() {}

            @Override
            public void close() {}
        };
        try (Driver driver = TestDriverFactory.create(driverContext, source, List.of(), new PageConsumerOperator(Page::releaseBlocks))) {
            // the wait's reason enters the status at the driver's first iteration on the pool: a floor a loaded host still meets
            TestDriverRunner runner = new TestDriverRunner().timeout(TimeValue.timeValueMillis(between(1000, 2000)));
            AssertionError error = expectThrows(AssertionError.class, () -> runner.run(driver));
            assertThat(error.getCause(), instanceOf(ElasticsearchTimeoutException.class));
            assertThat(error.getMessage(), containsString("drivers did not complete within ["));
            assertThat(error.getMessage(), containsString("a listener nobody completes"));
            assertThat(error.getMessage(), containsString("thread [elasticsearch[test][esql]"));
        }
    }
}
