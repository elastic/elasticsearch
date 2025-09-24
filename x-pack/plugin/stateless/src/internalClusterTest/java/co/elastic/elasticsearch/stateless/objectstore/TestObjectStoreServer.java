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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.TestEsExecutors;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@SuppressForbidden(reason = "this class uses a HttpServer to emulate a cloud-based storage service")
public class TestObjectStoreServer {

    private static final Logger log = LogManager.getLogger(TestObjectStoreServer.class);

    private HttpServer httpServer;
    private ExecutorService executorService;
    private Map<String, HttpHandler> handlers;

    public TestObjectStoreServer() {}

    public void start() throws IOException {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        ThreadFactory threadFactory = TestEsExecutors.testOnlyDaemonThreadFactory(
            "[" + AbstractMockObjectStoreIntegTestCase.class.getName() + "]"
        );
        executorService = EsExecutors.newScaling(
            AbstractMockObjectStoreIntegTestCase.class.getName(),
            0,
            100,
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

    public void stop() {
        httpServer.stop(0);
        ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);
        httpServer = null;
    }

    public void setUp(Map<String, HttpHandler> handlers) {
        this.handlers = Map.copyOf(handlers);
        this.handlers.forEach(httpServer::createContext);
    }

    public void tearDown() {
        if (handlers != null) {
            for (String handler : handlers.keySet()) {
                httpServer.removeContext(handler);
            }
        }
    }

    public String serverUrl() {
        InetSocketAddress address = httpServer.getAddress();
        return InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }
}
