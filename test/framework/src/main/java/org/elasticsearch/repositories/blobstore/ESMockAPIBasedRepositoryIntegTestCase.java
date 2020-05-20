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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.test.BackgroundIndexer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for {@link BlobStoreRepository} implementations rely on mock APIs that emulate cloud-based services.
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
public abstract class ESMockAPIBasedRepositoryIntegTestCase extends ESBlobStoreRepositoryIntegTestCase {

    /**
     * A {@link HttpHandler} that allows to list stored blobs
     */
    @SuppressForbidden(reason = "Uses a HttpServer to emulate a cloud-based storage service")
    protected interface BlobStoreHttpHandler extends HttpHandler {
        Map<String, BytesReference> blobs();
    }

    private static final byte[] BUFFER = new byte[1024];

    private static HttpServer httpServer;
    protected Map<String, HttpHandler> handlers;

    private static final Logger log = LogManager.getLogger();

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.setExecutor(r -> {
            try {
                r.run();
            } catch (Throwable t) {
                log.error("Error in execution on mock http server IO thread", t);
                throw t;
            }
        });
        httpServer.start();
    }

    @Before
    public void setUpHttpServer() {
        handlers = new HashMap<>(createHttpHandlers());
        handlers.replaceAll((k, h) -> wrap(randomBoolean() ? createErroneousHttpHandler(h) : h, logger));
        handlers.forEach(httpServer::createContext);
    }

    @AfterClass
    public static void stopHttpServer() {
        httpServer.stop(0);
        httpServer = null;
    }

    @After
    public void tearDownHttpServer() {
        if (handlers != null) {
            for(Map.Entry<String, HttpHandler> handler : handlers.entrySet()) {
                httpServer.removeContext(handler.getKey());
                HttpHandler h = handler.getValue();
                while (h instanceof DelegatingHttpHandler) {
                    h = ((DelegatingHttpHandler) h).getDelegate();
                }
                if (h instanceof BlobStoreHttpHandler) {
                    List<String> blobs = ((BlobStoreHttpHandler) h).blobs().keySet().stream()
                        .filter(blob -> blob.contains("index") == false).collect(Collectors.toList());
                    assertThat("Only index blobs should remain in repository but found " + blobs, blobs, hasSize(0));
                }
            }
        }
    }

    protected abstract Map<String, HttpHandler> createHttpHandlers();

    protected abstract HttpHandler createErroneousHttpHandler(HttpHandler delegate);

    protected abstract List<String> requestTypesTracked();

    /**
     * Test the snapshot and restore of an index which has large segments files.
     */
    public final void testSnapshotWithLargeSegmentFiles() throws Exception {
        final String repository = createRepository(randomName());
        final String index = "index-no-merges";
        createIndex(index, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());

        final long nbDocs = randomLongBetween(10_000L, 20_000L);
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, "_doc", client(), (int) nbDocs)) {
            waitForDocs(nbDocs, indexer);
        }

        flushAndRefresh(index);
        ForceMergeResponse forceMerge = client().admin().indices().prepareForceMerge(index).setFlush(true).setMaxNumSegments(1).get();
        assertThat(forceMerge.getSuccessfulShards(), equalTo(1));
        assertHitCount(client().prepareSearch(index).setSize(0).setTrackTotalHits(true).get(), nbDocs);

        final String snapshot = "snapshot";
        assertSuccessfulSnapshot(client().admin().cluster().prepareCreateSnapshot(repository, snapshot)
            .setWaitForCompletion(true).setIndices(index));

        assertAcked(client().admin().indices().prepareDelete(index));

        assertSuccessfulRestore(client().admin().cluster().prepareRestoreSnapshot(repository, snapshot).setWaitForCompletion(true));
        ensureGreen(index);
        assertHitCount(client().prepareSearch(index).setSize(0).setTrackTotalHits(true).get(), nbDocs);

        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repository, snapshot).get());
    }

    public void testRequestStats() throws Exception {
        final String repository = createRepository(randomName());
        final String index = "index-no-merges";
        createIndex(index, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());

        final long nbDocs = randomLongBetween(10_000L, 20_000L);
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, "_doc", client(), (int) nbDocs)) {
            waitForDocs(nbDocs, indexer);
        }

        flushAndRefresh(index);
        ForceMergeResponse forceMerge = client().admin().indices().prepareForceMerge(index).setFlush(true).setMaxNumSegments(1).get();
        assertThat(forceMerge.getSuccessfulShards(), equalTo(1));
        assertHitCount(client().prepareSearch(index).setSize(0).setTrackTotalHits(true).get(), nbDocs);

        final String snapshot = "snapshot";
        assertSuccessfulSnapshot(client().admin().cluster().prepareCreateSnapshot(repository, snapshot)
            .setWaitForCompletion(true).setIndices(index));

        assertAcked(client().admin().indices().prepareDelete(index));

        assertSuccessfulRestore(client().admin().cluster().prepareRestoreSnapshot(repository, snapshot).setWaitForCompletion(true));
        ensureGreen(index);
        assertHitCount(client().prepareSearch(index).setSize(0).setTrackTotalHits(true).get(), nbDocs);

        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repository, snapshot).get());

        final RepositoryStats repositoryStats = StreamSupport.stream(
            internalCluster().getInstances(RepositoriesService.class).spliterator(), false)
            .map(repositoriesService -> {
                try {
                    return repositoriesService.repository(repository);
                } catch (RepositoryMissingException e) {
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .map(Repository::stats)
            .reduce(RepositoryStats::merge)
            .get();

        Map<String, Long> sdkRequestCounts = repositoryStats.requestCounts;

        for (String requestType : requestTypesTracked()) {
            assertSDKCallsMatchMockCalls(sdkRequestCounts, requestType);
        }
    }

    private void assertSDKCallsMatchMockCalls(Map<String, Long> sdkRequestCount, String requestTye) {
        final long sdkCalls = sdkRequestCount.getOrDefault(requestTye, 0L);
        final long mockCalls = handlers.values().stream()
            .mapToLong(h -> {
                while (h instanceof DelegatingHttpHandler) {
                    if (h instanceof HttpStatsCollectorHandler) {
                        return ((HttpStatsCollectorHandler) h).getCount(requestTye);
                    }
                    h = ((DelegatingHttpHandler) h).getDelegate();
                }

                return 0L;
            }).sum();

        String assertionErrorMsg = String.format("SDK sent %d [%s] calls and handler measured %d [%s] calls",
            sdkCalls,
            requestTye,
            mockCalls,
            requestTye);

        assertEquals(assertionErrorMsg, mockCalls, sdkCalls);
    }

    protected static String httpServerUrl() {
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    /**
     * Consumes and closes the given {@link InputStream}
     */
    protected static void drainInputStream(final InputStream inputStream) throws IOException {
        while (inputStream.read(BUFFER) >= 0) ;
    }

    /**
     * HTTP handler that injects random service errors
     *
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
    protected abstract static class ErroneousHttpHandler implements DelegatingHttpHandler {

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
            try {
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
            } finally {
                try {
                    int read = exchange.getRequestBody().read();
                    assert read == -1 : "Request body should have been fully read here but saw [" + read + "]";
                } catch (IOException e) {
                    // ignored, stream is assumed to have been closed by previous handler
                }
                exchange.close();
            }
        }

        protected void handleAsError(final HttpExchange exchange) throws IOException {
            try {
                drainInputStream(exchange.getRequestBody());
                exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
            } finally {
                exchange.close();
            }
        }

        protected abstract String requestUniqueId(HttpExchange exchange);

        protected boolean canFailRequest(final HttpExchange exchange) {
            return true;
        }

        public HttpHandler getDelegate() {
            return delegate;
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
    public interface DelegatingHttpHandler extends HttpHandler {
        HttpHandler getDelegate();
    }

    /**
     * HTTP handler that allows collect request stats per request type.
     *
     * Implementors should keep track of the desired requests on {@link #maybeTrack(String, Headers)}.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
    public abstract static class HttpStatsCollectorHandler implements DelegatingHttpHandler {

        private final HttpHandler delegate;

        private final Map<String, Long> operationCount = new HashMap<>();

        public HttpStatsCollectorHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public HttpHandler getDelegate() {
            return delegate;
        }

        synchronized long getCount(final String requestType) {
            return operationCount.getOrDefault(requestType, 0L);
        }

        protected synchronized void trackRequest(final String requestType) {
            operationCount.put(requestType, operationCount.getOrDefault(requestType, 0L) + 1);
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();

            maybeTrack(request, exchange.getRequestHeaders());

            delegate.handle(exchange);
        }

        /**
         * Tracks the given request if it matches the criteria.
         *
         * The request is represented as:
         * Request = Method SP Request-URI
         *
         * @param request the request to be tracked if it matches the criteria
         * @param requestHeaders the http request headers
         */
        protected abstract void maybeTrack(String request, Headers requestHeaders);
    }

    /**
     * Wrap a {@link HttpHandler} to log any thrown exception using the given {@link Logger}.
     */
    public static DelegatingHttpHandler wrap(final HttpHandler handler, final Logger logger) {
        return new ExceptionCatchingHttpHandler(handler, logger);
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
    private static class ExceptionCatchingHttpHandler implements DelegatingHttpHandler {

        private final HttpHandler handler;
        private final Logger logger;

        ExceptionCatchingHttpHandler(HttpHandler handler, Logger logger) {
            this.handler = handler;
            this.logger = logger;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                handler.handle(exchange);
            } catch (Throwable t) {
                logger.error(() -> new ParameterizedMessage("Exception when handling request {} {} {}",
                    exchange.getRemoteAddress(), exchange.getRequestMethod(), exchange.getRequestURI()), t);
                throw t;
            }
        }

        @Override
        public HttpHandler getDelegate() {
            return handler;
        }
    }
}
