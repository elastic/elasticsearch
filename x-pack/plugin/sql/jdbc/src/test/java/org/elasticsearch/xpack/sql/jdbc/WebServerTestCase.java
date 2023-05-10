/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.rest.root.MainResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.junit.After;
import org.junit.Before;

import java.util.Date;

/**
 * Base class for unit tests that need a web server for basic tests.
 */
public abstract class WebServerTestCase extends ESTestCase {

    private MockWebServer webServer = new MockWebServer();

    @Before
    public void init() throws Exception {
        webServer.start();
    }

    @After
    public void cleanup() {
        webServer.close();
    }

    public MockWebServer webServer() {
        return webServer;
    }

    MainResponse createCurrentVersionMainResponse() {
        return createMainResponse(Version.CURRENT);
    }

    MainResponse createMainResponse(Version version) {
        String clusterUuid = randomAlphaOfLength(10);
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        String nodeName = randomAlphaOfLength(10);
        final String date = new Date(randomNonNegativeLong()).toString();
        Build build = new Build(Build.Type.UNKNOWN, randomAlphaOfLength(8), date, randomBoolean(), version.toString());
        return new MainResponse(nodeName, version, clusterName, clusterUuid, build);
    }

    String webServerAddress() {
        return webServer.getHostName() + ":" + webServer.getPort();
    }
}
