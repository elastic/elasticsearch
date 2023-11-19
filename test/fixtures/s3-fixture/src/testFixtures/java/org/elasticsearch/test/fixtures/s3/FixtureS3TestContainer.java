/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.fixtures.s3;

import org.jetbrains.annotations.NotNull;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;

public class FixtureS3TestContainer implements TestRule {

    private static final int servicePort = 80;

    private ComposeContainer container = new ComposeContainer(
        new File("/Users/rene/dev/elastic/elasticsearch/test/fixtures/s3-fixture/docker-compose.yml")
    ).withExposedService("s3-fixture", servicePort, Wait.forListeningPort())
        .withExposedService("s3-fixture-with-session-token", servicePort, Wait.forListeningPort())
        .withExposedService("s3-fixture-with-ec2", servicePort, Wait.forListeningPort())
        .withLocalCompose(true);

    private Statement startContainer(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                container.apply(base, description).evaluate();
            }
        };
    }

    @NotNull
    @Override
    public Statement apply(@NotNull Statement base, @NotNull Description description) {
        return startContainer(base, description);
    }

    public FixtureS3TestContainer withExposedService(String service) {
        container.withExposedService(service, 80, Wait.forListeningPort());
        return this;
    }

    private int getServicePort(String serviceName) {
        return container.getServicePort(serviceName, servicePort);
    }

    public String getServiceUrl(String serviceName) {
        return "http://127.0.0.1:" + getServicePort(serviceName);
    }
}
