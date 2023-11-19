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

import java.io.File;
import java.util.List;

public class S3FixtureTestContainerRule implements S3Fixture {

    private static final int servicePort = 80;
    private ComposeContainer container;

    private ComposeContainer createContainer(List<String> services) {
        ComposeContainer composeContainer = new ComposeContainer(resolveFixtureHome());
        services.forEach(service -> composeContainer.withExposedService(service, servicePort, Wait.forListeningPort()));
        return composeContainer;
    }

    @NotNull
    private static File resolveFixtureHome() {
        String userHomeProperty = System.getProperty("fixture.s3-fixture.home");
        File home = new File(userHomeProperty);
        return new File(home, "docker-compose.yml");
    }

    public S3FixtureTestContainerRule(List<String> configurationActions) {
        this.container = createContainer(configurationActions);
    }

    @NotNull
    @Override
    public Statement apply(@NotNull Statement base, @NotNull Description description) {
        container.start();
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } finally {

                }
            }
        };
    }

    public S3FixtureTestContainerRule withExposedService(String service) {
        container.withExposedService(service, servicePort, Wait.forListeningPort());
        return this;
    }

    private int getServicePort(String serviceName) {
        return container.getServicePort(serviceName, servicePort);
    }

    public String getServiceUrl(String serviceName) {
        return "http://127.0.0.1:" + getServicePort(serviceName);
    }

}
