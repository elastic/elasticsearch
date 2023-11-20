/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.fixtures.minio;

import org.jetbrains.annotations.NotNull;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.util.List;

public class MinioFixtureTestContainer implements TestRule {

    private static final int servicePort = 9000;
    private ComposeContainer container;

    private ComposeContainer createContainer(List<String> services) {
        ComposeContainer composeContainer = new ComposeContainer(resolveFixtureHome());
        services.forEach(service -> composeContainer.withExposedService(service, servicePort, Wait.forListeningPort()));
        return composeContainer.withLocalCompose(true);
    }

    @NotNull
    private static File resolveFixtureHome() {
        String userHomeProperty = System.getProperty("fixture.minio-fixture.home");
        File home = new File(userHomeProperty);

        File file = new File(home, "docker-compose.yml");
        System.out.println("file = " + file.getPath() + "  --  " + file.exists());
        return file;
    }

    public MinioFixtureTestContainer(List<String> services) {
        if(services.isEmpty() == false) {
            this.container = createContainer(services);
        }
    }

    @NotNull
    @Override
    public Statement apply(@NotNull Statement base, @NotNull Description description) {
        if(container != null) {
            container.start();
        }
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

    private int getServicePort(String serviceName) {
        return container.getServicePort(serviceName, servicePort);
    }

    public String getServiceUrl() {
        return getServiceUrl("minio-fixture");
    }

    public String getServiceUrl(String serviceName) {
        return "http://127.0.0.1:" + getServicePort(serviceName);
    }
}
