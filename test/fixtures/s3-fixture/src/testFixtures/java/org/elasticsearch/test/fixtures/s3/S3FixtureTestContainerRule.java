/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.fixtures.s3;

import org.jetbrains.annotations.NotNull;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;

import java.io.File;

public class S3FixtureTestContainerRule implements S3Fixture, Startable {

    private static final int servicePort = 80;
    private final ComposeContainer container;

    private ComposeContainer createContainer() {
        return new ComposeContainer(resolveFixtureHome()).withExposedService("s3-fixture", servicePort, Wait.forListeningPort())
            .withExposedService("s3-fixture-with-session-token", servicePort, Wait.forListeningPort())
            .withExposedService("s3-fixture-with-ec2", servicePort, Wait.forListeningPort());
    }

    @NotNull
    private static File resolveFixtureHome() {
        File home = new File(System.getProperty("fixture.home.s3-fixture"));
        return new File(home, "docker-compose.yml");
    }

    public S3FixtureTestContainerRule() {
        this.container = createContainer();
    }

    @NotNull
    @Override
    public Statement apply(@NotNull Statement base, @NotNull Description description) {
        container.start();
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                base.evaluate();
            }
        };
    }

    public S3FixtureTestContainerRule withExposedService(String service) {
        container.withExposedService(service, 80, Wait.forListeningPort());
        return this;
    }

    private int getServicePort(String serviceName) {
        return container.getServicePort(serviceName, servicePort);
    }

    public String getServiceUrl(String serviceName) {
        return "http://127.0.0.1:" + getServicePort(serviceName);
    }

    @Override
    public void start() {
        container.start();
    }

    @Override
    public void stop() {

    }
}
