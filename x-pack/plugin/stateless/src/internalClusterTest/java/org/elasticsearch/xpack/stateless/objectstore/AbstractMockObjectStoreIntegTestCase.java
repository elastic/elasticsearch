/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Map;

/**
 * Integration tests for {@link org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService} types that rely
 * on mock APIs that emulate cloud-based services.
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
public abstract class AbstractMockObjectStoreIntegTestCase extends AbstractObjectStoreIntegTestCase {

    private static final TestObjectStoreServer testObjectStoreServer = new TestObjectStoreServer();

    @BeforeClass
    public static void startHttpServer() throws Exception {
        testObjectStoreServer.start();
    }

    @AfterClass
    public static void stopHttpServer() {
        testObjectStoreServer.stop();
    }

    protected static String httpServerUrl() {
        return "http://" + testObjectStoreServer.serverUrl();
    }

    @Before
    public void setUpHttpServer() {
        testObjectStoreServer.setUp(Maps.copyOf(createHttpHandlers(), h -> ESMockAPIBasedRepositoryIntegTestCase.wrap(h, logger)));
    }

    @After
    public void tearDownHttpServer() throws Exception {
        // explicitly clean up the cluster here
        // the cluster needs to be stopped BEFORE the http server is stopped
        try {
            cleanUpCluster();
        } finally {
            testObjectStoreServer.tearDown();
        }
    }

    protected abstract Map<String, HttpHandler> createHttpHandlers();

}
