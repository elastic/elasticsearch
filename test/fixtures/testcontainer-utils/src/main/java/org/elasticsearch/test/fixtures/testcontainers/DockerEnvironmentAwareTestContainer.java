/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.testcontainers;

import org.elasticsearch.test.fixtures.CacheableTestFixture;
import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.concurrent.Future;

import static org.elasticsearch.test.fixtures.testcontainers.DockerAvailability.assumeDockerIsAvailable;

public class DockerEnvironmentAwareTestContainer extends GenericContainer<DockerEnvironmentAwareTestContainer>
    implements
        TestRule,
        CacheableTestFixture {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DockerEnvironmentAwareTestContainer.class);

    public DockerEnvironmentAwareTestContainer(Future<String> image) {
        super(image);
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate() {
                try {
                    start();
                    statement.evaluate();
                } catch (AssumptionViolatedException e) {
                    throw e;
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                } finally {
                    stop();
                }
            }
        };
    }

    @Override
    public void start() {
        assumeDockerIsAvailable();
        withLogConsumer(new Slf4jLogConsumer(LOGGER));
        super.start();
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping container {}", getContainerId());
        super.stop();
    }

    @Override
    public void cache() {
        try {
            start();
            stop();
        } catch (RuntimeException e) {
            logger().warn("Error while caching container images.", e);
        }
    }

}
