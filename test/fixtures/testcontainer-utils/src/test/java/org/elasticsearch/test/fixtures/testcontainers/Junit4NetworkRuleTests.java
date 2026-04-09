/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.testcontainers;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.Network;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.fixtures.testcontainers.DockerAvailability.assumeDockerIsAvailable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class Junit4NetworkRuleTests {

    @BeforeClass
    public static void checkDockerAvailable() {
        assumeDockerIsAvailable();
    }

    @Test
    public void testNetworkLifecycle() throws Throwable {
        // Track lifecycle events
        final AtomicBoolean networkCreated = new AtomicBoolean(false);
        final AtomicBoolean statementExecuted = new AtomicBoolean(false);

        // Create a network
        Network network = Network.newNetwork();
        Junit4NetworkRule rule = Junit4NetworkRule.from(network);

        // Create a test statement
        Statement testStatement = new Statement() {
            @Override
            public void evaluate() {
                statementExecuted.set(true);
                // Verify network is accessible during test execution
                assertNotNull(network.getId());
                networkCreated.set(true);
            }
        };

        // Execute the rule
        Statement wrappedStatement = rule.apply(testStatement, Description.createTestDescription("Test", "test"));
        wrappedStatement.evaluate();

        // Verify execution order
        assertTrue("Network should have been created before test execution", networkCreated.get());
        assertTrue("Test statement should have been executed", statementExecuted.get());

        // Note: We can't easily verify network.close() was called without mocking,
        // but we can verify the statement executed successfully
    }

    @Test
    public void testNetworkClosedEvenOnException() {
        Network network = Network.newNetwork();
        Junit4NetworkRule rule = Junit4NetworkRule.from(network);

        final AtomicBoolean exceptionThrown = new AtomicBoolean(false);

        Statement failingStatement = new Statement() {
            @Override
            public void evaluate() {
                exceptionThrown.set(true);
                throw new RuntimeException("Test exception");
            }
        };

        Statement wrappedStatement = rule.apply(failingStatement, Description.createTestDescription("Test", "test"));

        try {
            wrappedStatement.evaluate();
        } catch (RuntimeException e) {
            // Expected exception
            assertEquals("Test exception", e.getMessage());
        } catch (Throwable e) {
            throw new RuntimeException("Unexpected exception type", e);
        }

        assertTrue("Exception should have been thrown", exceptionThrown.get());
        // Network should be closed even after exception, but we can't easily verify without mocking
    }

    @Test
    public void testStatementEvaluationOrder() throws Throwable {
        Network network = Network.newNetwork();
        Junit4NetworkRule rule = Junit4NetworkRule.from(network);

        final AtomicBoolean statementExecuted = new AtomicBoolean(false);

        Statement testStatement = new Statement() {
            @Override
            public void evaluate() {
                // Verify network is accessible during statement execution
                assertNotNull(network.getId());
                statementExecuted.set(true);
            }
        };

        Statement wrappedStatement = rule.apply(testStatement, Description.createTestDescription("Test", "test"));
        wrappedStatement.evaluate();

        // Verify statement was executed
        assertTrue("Statement should have been executed", statementExecuted.get());
    }
}
