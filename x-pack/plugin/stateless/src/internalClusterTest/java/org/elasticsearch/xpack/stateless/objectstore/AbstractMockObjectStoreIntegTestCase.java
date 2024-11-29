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
import com.sun.net.httpserver.HttpServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for {@link ObjectStoreService} types that rely on mock APIs that emulate cloud-based services.
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
public abstract class AbstractMockObjectStoreIntegTestCase extends AbstractObjectStoreIntegTestCase {

    private static final Logger log = LogManager.getLogger(AbstractMockObjectStoreIntegTestCase.class);
    private static HttpServer httpServer;
    private static ExecutorService executorService;
    protected Map<String, HttpHandler> handlers;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        ThreadFactory threadFactory = EsExecutors.daemonThreadFactory("[" + AbstractMockObjectStoreIntegTestCase.class.getName() + "]");
        executorService = EsExecutors.newScaling(
            AbstractMockObjectStoreIntegTestCase.class.getName(),
            0,
            2,
            60,
            TimeUnit.SECONDS,
            true,
            threadFactory,
            new ThreadContext(Settings.EMPTY)
        );
        httpServer.setExecutor(r -> {
            executorService.execute(() -> {
                try {
                    r.run();
                } catch (Throwable t) {
                    log.error("Error in execution on mock http server IO thread", t);
                    throw t;
                }
            });
        });
        httpServer.start();
    }

    @AfterClass
    public static void stopHttpServer() {
        httpServer.stop(0);
        ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);
        httpServer = null;
    }

    protected static String httpServerUrl() {
        return "http://" + serverUrl();
    }

    protected static String serverUrl() {
        InetSocketAddress address = httpServer.getAddress();
        return InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    @Before
    public void setUpHttpServer() {
        handlers = Maps.copyOf(createHttpHandlers(), h -> ESMockAPIBasedRepositoryIntegTestCase.wrap(h, logger));
        handlers.forEach(httpServer::createContext);
    }

    @After
    public void tearDownHttpServer() throws Exception {
        // explicitly clean up the cluster here
        // the cluster needs to be stopped BEFORE the http server is stopped
        cleanUpCluster();

        if (handlers != null) {
            for (String handler : handlers.keySet()) {
                httpServer.removeContext(handler);
            }
        }
    }

    protected abstract Map<String, HttpHandler> createHttpHandlers();

}
