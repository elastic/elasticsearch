/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.objectstore;

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
 * Integration tests for {@link ObjectStoreService} types that rely on mock APIs that emulate cloud-based services.
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
