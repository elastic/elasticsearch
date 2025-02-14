/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.Build;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.rest.root.MainResponse;
import org.elasticsearch.test.BuildUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

/**
 * Base class for unit tests that need a web server for basic tests.
 */
public abstract class WebServerTestCase extends ESTestCase {

    private final MockWebServer webServer = new MockWebServer();

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
        return createMainResponse(VersionTests.current());
    }

    MainResponse createMainResponse(SqlVersion version) {
        // the SQL client only cares about node version,
        // so ignore index & transport versions here (just set them to current)
        String clusterUuid = randomAlphaOfLength(10);
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        String nodeName = randomAlphaOfLength(10);
        IndexVersion indexVersion = IndexVersion.current();
        Build build = BuildUtils.newBuild(Build.current(), Map.of("version", version.toString()));
        return new MainResponse(nodeName, indexVersion.luceneVersion().toString(), clusterName, clusterUuid, build);
    }

    String webServerAddress() {
        return webServer.getHostName() + ":" + webServer.getPort();
    }
}
