/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.testcontainers;

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.Network;

import static org.elasticsearch.test.fixtures.testcontainers.DockerAvailability.assumeDockerIsAvailable;

public class Junit4NetworkRule implements TestRule {

    private final Network network;

    Junit4NetworkRule(Network network) {
        this.network = network;
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    assumeDockerIsAvailable();
                    network.getId();
                    statement.evaluate();
                    network.close();
                } catch (AssumptionViolatedException e) {
                    throw e;
                } finally {
                    try {
                        network.close();
                    } catch (Throwable e) {

                    }
                }
            }
        };

    }

    public static Junit4NetworkRule from(Network network) {
        return new Junit4NetworkRule(network);
    }
}
