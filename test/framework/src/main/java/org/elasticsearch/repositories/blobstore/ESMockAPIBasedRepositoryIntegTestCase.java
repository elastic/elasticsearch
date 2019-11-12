/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.blobstore;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.BackgroundIndexer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for {@link BlobStoreRepository} implementations rely on mock APIs that emulate cloud-based services.
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
public abstract class ESMockAPIBasedRepositoryIntegTestCase extends ESBlobStoreRepositoryIntegTestCase {

    private static final byte[] BUFFER = new byte[1024];

    private static HttpServer httpServer;
    private Map<String, HttpHandler> handlers;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
    }

    @Before
    public void setUpHttpServer() {
        handlers = createHttpHandlers();
        handlers.forEach((c, h) -> httpServer.createContext(c, wrap(randomBoolean() ? createErroneousHttpHandler(h) : h, logger)));
    }

    @AfterClass
    public static void stopHttpServer() {
        httpServer.stop(0);
        httpServer = null;
    }

    @After
    public void tearDownHttpServer() {
        if (handlers != null) {
            handlers.keySet().forEach(context -> httpServer.removeContext(context));
        }
    }

    protected abstract Map<String, HttpHandler> createHttpHandlers();

    protected abstract HttpHandler createErroneousHttpHandler(HttpHandler delegate);

    /**
     * Test the snapshot and restore of an index which has large segments files.
     */
    public final void testSnapshotWithLargeSegmentFiles() throws Exception {
        final String repository = createRepository(randomName());
        final String index = "index-no-merges";
        createIndex(index, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());

        final long nbDocs = randomLongBetween(10_000L, 20_000L);
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, "_doc", client(), (int) nbDocs)) {
            waitForDocs(nbDocs, indexer);
        }

        flushAndRefresh(index);
        ForceMergeResponse forceMerge = client().admin().indices().prepareForceMerge(index).setFlush(true).setMaxNumSegments(1).get();
        assertThat(forceMerge.getSuccessfulShards(), equalTo(1));
        assertHitCount(client().prepareSearch(index).setSize(0).setTrackTotalHits(true).get(), nbDocs);

        assertSuccessfulSnapshot(client().admin().cluster().prepareCreateSnapshot(repository, "snapshot")
            .setWaitForCompletion(true).setIndices(index));

        assertAcked(client().admin().indices().prepareDelete(index));

        assertSuccessfulRestore(client().admin().cluster().prepareRestoreSnapshot(repository, "snapshot").setWaitForCompletion(true));
        ensureGreen(index);
        assertHitCount(client().prepareSearch(index).setSize(0).setTrackTotalHits(true).get(), nbDocs);
    }

    protected static String httpServerUrl() {
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    /**
     * Consumes and closes the given {@link InputStream}
     */
    protected static void drainInputStream(final InputStream inputStream) throws IOException {
        try (InputStream is = inputStream) {
            while (is.read(BUFFER) >= 0);
        }
    }

    /**
     * HTTP handler that injects random service errors
     *
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
    protected abstract static class ErroneousHttpHandler implements HttpHandler {

        // first key is a unique identifier for the incoming HTTP request,
        // value is the number of times the request has been seen
        private final Map<String, AtomicInteger> requests;
        private final HttpHandler delegate;
        private final int maxErrorsPerRequest;

        @SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
        protected ErroneousHttpHandler(final HttpHandler delegate, final int maxErrorsPerRequest) {
            this.requests = new ConcurrentHashMap<>();
            this.delegate = delegate;
            this.maxErrorsPerRequest = maxErrorsPerRequest;
            assert maxErrorsPerRequest > 1;
        }

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final String requestId = requestUniqueId(exchange);
            assert Strings.hasText(requestId);

            final boolean canFailRequest = canFailRequest(exchange);
            final int count = requests.computeIfAbsent(requestId, req -> new AtomicInteger(0)).incrementAndGet();
            if (count >= maxErrorsPerRequest || canFailRequest == false) {
                requests.remove(requestId);
                delegate.handle(exchange);
            } else {
                handleAsError(exchange);
            }
        }

        protected void handleAsError(final HttpExchange exchange) throws IOException {
            drainInputStream(exchange.getRequestBody());
            exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
            exchange.close();
        }

        protected abstract String requestUniqueId(HttpExchange exchange);

        protected boolean canFailRequest(final HttpExchange exchange) {
            return true;
        }
    }

    /**
     * Wrap a {@link HttpHandler} to log any thrown exception using the given {@link Logger}.
     */
    private static HttpHandler wrap(final HttpHandler handler, final Logger logger) {
        return exchange -> {
            try {
                handler.handle(exchange);
            } catch (final Exception e) {
                logger.error("Exception when handling request {} {} {}",
                    exchange.getRemoteAddress(), exchange.getRequestMethod(), exchange.getRequestURI(), e);
                throw e;
            }
        };
    }
}
