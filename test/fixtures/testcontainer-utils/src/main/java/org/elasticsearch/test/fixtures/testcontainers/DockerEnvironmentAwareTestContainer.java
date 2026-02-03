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

public abstract class DockerEnvironmentAwareTestContainer extends GenericContainer<DockerEnvironmentAwareTestContainer>
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
        String containerId = getContainerId();
        LOGGER.info("Stopping container {}", containerId);
        super.stop();
        ensureContainerFullyStopped(containerId);
    }

    /**
     * Ensures the container is fully stopped and all resources (especially bind mounts) are released.
     * This prevents race conditions where Docker hasn't fully released resources when cleanup happens.
     *
     * @param containerId the container ID to wait for, or null if container was never started
     */
    protected void ensureContainerFullyStopped(String containerId) {
        if (containerId == null) {
            return;
        }

        int maxAttempts = 10;
        int attemptDelayMs = 50;

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                var containerInfo = getDockerClient().inspectContainerCmd(containerId).exec();
                if (Boolean.FALSE.equals(containerInfo.getState().getRunning())) {
                    // Container is stopped, give it a moment to release bind mounts and other resources
                    if (attempt > 0) {
                        Thread.sleep(attemptDelayMs);
                    }
                    return;
                }
                Thread.sleep(attemptDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted while waiting for container {} to stop", containerId);
                return;
            } catch (Exception e) {
                // Container might already be removed, which is fine
                LOGGER.debug("Error inspecting container {} (may already be removed): {}", containerId, e.getMessage());
                return;
            }
        }

        LOGGER.warn("Container {} did not stop within expected time, proceeding with cleanup anyway", containerId);
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
