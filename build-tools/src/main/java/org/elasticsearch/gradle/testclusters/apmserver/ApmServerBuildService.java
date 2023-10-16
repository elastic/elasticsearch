/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.testclusters.apmserver;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.tooling.events.FinishEvent;
import org.gradle.tooling.events.OperationCompletionListener;

import java.io.IOException;

public abstract class ApmServerBuildService
    implements
        BuildService<BuildServiceParameters.None>,
        OperationCompletionListener,
        AutoCloseable {
    private final Logger logger = Logging.getLogger(getClass());

    private MockApmServer mockServer;

    public ApmServerBuildService() {
        logger.info("starting up apm server");
        mockServer = new MockApmServer(9999);
        try {
            mockServer.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("shutting down apm server");
            mockServer.stop();
        }));
    }

    @Override
    public void close() throws Exception {
        logger.info("shutting down apm server");
        mockServer.stop();
    }

    @Override
    public void onFinish(FinishEvent finishEvent) {
        logger.info("shutting down apm server");
        mockServer.stop();
    }

    public int getPort() {
        return mockServer.getPort();
    }
}
