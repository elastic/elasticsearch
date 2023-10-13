/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.testclusters.apmserver;

import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.tooling.events.FinishEvent;
import org.gradle.tooling.events.OperationCompletionListener;

import java.io.IOException;

public abstract class ApmServerBuildService implements BuildService<BuildServiceParameters.None>, OperationCompletionListener, AutoCloseable{
    private MockApmServer mockServer;

    public ApmServerBuildService() {
        System.out.println("starting ");
        mockServer = new MockApmServer();
        try {
            mockServer.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("shutdown hook");
            mockServer.stop();
        }));
    }

    @Override
    public void close() throws Exception {
        System.out.println("close");
        mockServer.stop();
    }

    @Override
    public void onFinish(FinishEvent finishEvent) {
        System.out.println("onfinish");
        mockServer.stop();
    }
}
