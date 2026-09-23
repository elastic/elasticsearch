/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import fixture.azure.AzureHttpHandler;
import fixture.azure.MockAzureBlobStore;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpPrincipal;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.azure.AzureRepositoryPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.zip.CRC32;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Regression test for the buffer dup+drop corruption in BCC uploads. Flushes an index repeatedly while a background thread holds all
 * {@code repository_azure} threads together and releases them at once, so concurrent refill requests can race. Before the fix, that
 * race caused an upload to emit one 64KB buffer twice and skip the next, or send a body of the wrong length. Every BCC blob stored
 * by the mock server is scanned for duplicate 64KB chunks, and aborted uploads fail the test.
 */
@SuppressForbidden(reason = "uses HttpServer to emulate Azure storage")
public class AzureBccUploadRefillRaceIT extends AbstractStatelessPluginIntegTestCase {

    private static final String ACCOUNT = "account";
    private static final String CONTAINER = "container";

    // AzureBlobStore.DEFAULT_UPLOAD_BUFFERS_SIZE
    private static final int BUFFER_SIZE = 64 * 1024;
    // build.gradle sets maxPrefetchSize=8, so refill requests arrive every 4 buffers instead of 64, increasing pressure on the race
    private static final int ROUNDS = 30;
    private static final int ROUNDS_PER_INDEX = 5;
    private static final int DOCS_PER_ROUND = 300;
    private static final int DOC_SIZE = 48 * 1024;
    private static final long SQUEEZE_MILLIS = 5;
    private static final long GAP_MILLIS = 1;
    private static final int SHARDS = 4;

    private static TestObjectStoreServer testServer;
    private static AzureHttpHandler azureHandler;
    private static final AtomicInteger bccUploadsInFlight = new AtomicInteger();
    /** BCC uploads the client aborted because the body was shorter than Content-Length declared. */
    private static final AtomicInteger abortedBccUploads = new AtomicInteger();

    // unique per execution so each cluster starts with a clean mock store
    private static final AtomicInteger executions = new AtomicInteger();
    private String basePath;

    @Before
    public void chooseBasePath() {
        basePath = "execution-" + executions.incrementAndGet();
        abortedBccUploads.set(0);
    }

    @BeforeClass
    public static void startServer() throws IOException {
        testServer = new TestObjectStoreServer();
        azureHandler = new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE);
        testServer.start();
        final HttpHandler bccUploadWatchingHandler = exchange -> {
            final boolean bccUpload = "PUT".equals(exchange.getRequestMethod())
                && exchange.getRequestURI().getPath().contains(StatelessCompoundCommit.PREFIX);
            if (bccUpload == false) {
                azureHandler.handle(exchange);
                return;
            }
            bccUploadsInFlight.incrementAndGet();
            try {
                // read the full body before handing off; a closed connection mid-body counts as an aborted upload
                final BytesReference body;
                try {
                    body = Streams.readFully(exchange.getRequestBody());
                } catch (IOException e) {
                    abortedBccUploads.incrementAndGet();
                    exchange.close();
                    return;
                }
                azureHandler.handle(new BufferedBodyExchange(exchange, body.streamInput()));
            } finally {
                bccUploadsInFlight.decrementAndGet();
            }
        };
        testServer.setUp(Map.of("/" + ACCOUNT, bccUploadWatchingHandler));
    }

    @AfterClass
    public static void stopServer() {
        if (testServer != null) {
            testServer.tearDown();
            testServer.stop();
            testServer = null;
        }
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(AzureRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.test.account", ACCOUNT);
        // The mock server does not validate HMAC signatures; any base64-encoded value works.
        secureSettings.setString("azure.client.test.key", Base64.getEncoder().encodeToString("test-key".getBytes(StandardCharsets.UTF_8)));

        String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=http://" + testServer.serverUrl() + "/" + ACCOUNT;

        return super.nodeSettings().put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // upload every commit as its own BCC so each flush is one upload
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.AZURE)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), CONTAINER)
            .put(ObjectStoreService.BASE_PATH_SETTING.getKey(), basePath)
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test")
            .put("azure.client.test.endpoint_suffix", endpoint)
            .put("azure.client.test.max_retries", 2)
            .setSecureSettings(secureSettings);
    }

    public void testConcurrentRefillsCorruptUploadedBcc() throws Exception {
        final String indexNode = startMasterAndIndexNode();
        String indexName = randomIndexName();
        createIndex(indexName, noMergesIndexSettings());

        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, indexNode);
        final PoolSqueezer squeezer = new PoolSqueezer(threadPool);
        squeezer.start();
        try {
            final Set<String> scanned = new HashSet<>();
            int blobsScanned = 0;
            for (int round = 0; round < ROUNDS; round++) {
                if (round > 0 && round % ROUNDS_PER_INDEX == 0) {
                    // the mock store keeps every live BCC in the test JVM's heap
                    final String indexUUID = resolveIndex(indexName).getUUID();
                    assertAcked(indicesAdmin().prepareDelete(indexName));
                    final MockAzureBlobStore blobStore = azureHandler.getMockBlobStore();
                    for (String path : blobStore.listBlobs(basePath + "/indices/" + indexUUID + "/", null).keySet()) {
                        try {
                            blobStore.deleteBlob(path, null);
                        } catch (MockAzureBlobStore.AzureBlobStoreError e) {
                            // already deleted by the node
                        }
                    }
                    indexName = randomIndexName();
                    createIndex(indexName, noMergesIndexSettings());
                }
                indexDocs(
                    indexName,
                    DOCS_PER_ROUND,
                    UnaryOperator.identity(),
                    null,
                    () -> Map.of("data", randomAlphanumericOfLength(DOC_SIZE))
                );
                flush(indexName);
                if (abortedBccUploads.get() > 0) {
                    fail(
                        abortedBccUploads.get()
                            + " BCC upload(s) were aborted by the client because the body did not match its Content-Length, after "
                            + (round + 1)
                            + " flushes, "
                            + blobsScanned
                            + " BCC blobs scanned, "
                            + squeezer.squeezes.get()
                            + " pool squeezes"
                    );
                }
                for (Map.Entry<String, BytesReference> blob : azureHandler.blobs().entrySet()) {
                    if (blob.getKey().contains(StatelessCompoundCommit.PREFIX) == false || scanned.add(blob.getKey()) == false) {
                        continue;
                    }
                    blobsScanned++;
                    final String corruption = describeDuplicateChunks(blob.getKey(), blob.getValue());
                    if (corruption != null) {
                        fail(
                            corruption
                                + "\nafter "
                                + (round + 1)
                                + " flushes, "
                                + blobsScanned
                                + " BCC blobs scanned, "
                                + squeezer.squeezes.get()
                                + " pool squeezes"
                        );
                    }
                }
                if ((round + 1) % 10 == 0) {
                    logger.info("--> {} flushes, {} BCC blobs scanned, {} pool squeezes", round + 1, blobsScanned, squeezer.squeezes.get());
                }
            }
            logger.info(
                "--> no corruption after {} flushes, {} BCC blobs scanned, {} pool squeezes",
                ROUNDS,
                blobsScanned,
                squeezer.squeezes.get()
            );
        } finally {
            squeezer.stop();
        }
    }

    // merges would re-upload the merged data, growing the blobs the mock store keeps
    private static Settings noMergesIndexSettings() {
        return indexSettings(SHARDS, 0).put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING.getKey(), "1mb").build();
    }

    /** Finds 64KB-aligned chunks that appear twice in a BCC blob, ignoring all-zero padding, or {@code null} if there are none. */
    private static String describeDuplicateChunks(String blobName, BytesReference blob) throws IOException {
        final Map<Long, Integer> firstChunkByCrc = new HashMap<>();
        final byte[] bytes = new byte[BUFFER_SIZE];
        StringBuilder description = null;
        try (StreamInput in = blob.streamInput()) {
            for (int chunk = 0; (chunk + 1) * BUFFER_SIZE <= blob.length(); chunk++) {
                in.readBytes(bytes, 0, BUFFER_SIZE);
                if (allZero(bytes)) {
                    continue;
                }
                final CRC32 crc = new CRC32();
                crc.update(bytes, 0, BUFFER_SIZE);
                final Integer earlier = firstChunkByCrc.putIfAbsent(crc.getValue(), chunk);
                if (earlier != null
                    && blob.slice(earlier * BUFFER_SIZE, BUFFER_SIZE).equals(blob.slice(chunk * BUFFER_SIZE, BUFFER_SIZE))) {
                    if (description == null) {
                        description = new StringBuilder(blobName).append(" (")
                            .append(blob.length())
                            .append(" bytes) holds a 64KB upload buffer twice:");
                    }
                    description.append("\n  chunk ")
                        .append(chunk)
                        .append(" [")
                        .append(chunk * BUFFER_SIZE)
                        .append(", ")
                        .append((chunk + 1) * BUFFER_SIZE)
                        .append(") == chunk ")
                        .append(earlier)
                        .append(" (")
                        .append(chunk - earlier)
                        .append(" chunks earlier)");
                }
            }
        }
        return description == null ? null : description.toString();
    }

    private static boolean allZero(byte[] b) {
        for (byte value : b) {
            if (value != 0) {
                return false;
            }
        }
        return true;
    }

    private static final class BufferedBodyExchange extends HttpExchange {
        private final HttpExchange delegate;
        private final InputStream body;

        BufferedBodyExchange(HttpExchange delegate, InputStream body) {
            this.delegate = delegate;
            this.body = body;
        }

        @Override
        public InputStream getRequestBody() {
            return body;
        }

        @Override
        public Headers getRequestHeaders() {
            return delegate.getRequestHeaders();
        }

        @Override
        public Headers getResponseHeaders() {
            return delegate.getResponseHeaders();
        }

        @Override
        public URI getRequestURI() {
            return delegate.getRequestURI();
        }

        @Override
        public String getRequestMethod() {
            return delegate.getRequestMethod();
        }

        @Override
        public HttpContext getHttpContext() {
            return delegate.getHttpContext();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public OutputStream getResponseBody() {
            return delegate.getResponseBody();
        }

        @Override
        public void sendResponseHeaders(int rCode, long responseLength) throws IOException {
            delegate.sendResponseHeaders(rCode, responseLength);
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return delegate.getRemoteAddress();
        }

        @Override
        public int getResponseCode() {
            return delegate.getResponseCode();
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return delegate.getLocalAddress();
        }

        @Override
        public String getProtocol() {
            return delegate.getProtocol();
        }

        @Override
        public Object getAttribute(String name) {
            return delegate.getAttribute(name);
        }

        @Override
        public void setAttribute(String name, Object value) {
            delegate.setAttribute(name, value);
        }

        @Override
        public void setStreams(InputStream i, OutputStream o) {
            delegate.setStreams(i, o);
        }

        @Override
        public HttpPrincipal getPrincipal() {
            return delegate.getPrincipal();
        }
    }

    /** While a BCC upload is in flight, occupies all {@code repository_azure} threads briefly and releases them at the same time. */
    private static final class PoolSqueezer implements Runnable {
        private final Executor pool;
        private final int threads;
        private final Thread thread = new Thread(this, "repository-azure-pool-squeezer");
        private final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicInteger squeezes = new AtomicInteger();

        PoolSqueezer(ThreadPool threadPool) {
            this.pool = threadPool.executor(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME);
            this.threads = threadPool.info(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME).getMax();
        }

        void start() {
            thread.start();
        }

        void stop() throws InterruptedException {
            running.set(false);
            thread.join(TimeValue.timeValueSeconds(10).millis());
        }

        @Override
        public void run() {
            while (running.get()) {
                if (bccUploadsInFlight.get() == 0) {
                    try {
                        Thread.sleep(GAP_MILLIS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    continue;
                }
                final CountDownLatch release = new CountDownLatch(1);
                for (int i = 0; i < threads; i++) {
                    pool.execute(() -> {
                        try {
                            release.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
                squeezes.incrementAndGet();
                try {
                    Thread.sleep(SQUEEZE_MILLIS);
                    release.countDown();
                    Thread.sleep(GAP_MILLIS);
                } catch (InterruptedException e) {
                    release.countDown();
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
