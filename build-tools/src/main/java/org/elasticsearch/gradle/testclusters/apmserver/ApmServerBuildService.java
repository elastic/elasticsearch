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

public abstract class ApmServerBuildService implements BuildService<BuildServiceParameters.None>, AutoCloseable{
    private MockApmServer mockServer;

    public ApmServerBuildService() {
        mockServer = new MockApmServer();
    }

    @Override
    public void close() throws Exception {
        mockServer.stop();
    }

}
