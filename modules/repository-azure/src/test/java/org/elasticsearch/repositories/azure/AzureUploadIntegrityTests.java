/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.CONTAINER_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.MAX_SINGLE_PART_UPLOAD_SIZE_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ENDPOINT_SUFFIX_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.KEY_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.TIMEOUT_SETTING;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;

/**
 * Checks that the bytes Azure receives for a blob are exactly the bytes the upload was given, when uploads run
 * concurrently and when they are retried.
 *
 * <p>Motivated by a production corruption in which a commit blob arrived with one 64KiB-aligned block replaced by a
 * byte-exact copy of a block one or two buffers earlier. The blob length was exact and every other block was correct,
 * so the {@code bytesRead} accounting in {@link AzureBlobStore} could not see it: a duplicate replaces rather than
 * adds, leaving the total unchanged. Only comparing what arrived against what was sent catches this.
 *
 * <p>The failure is rare at the shipped send-queue prefetch. To hunt it rather than just guard against it:
 * <pre>
 * ./gradlew :modules:repository-azure:test \
 *   --tests org.elasticsearch.repositories.azure.AzureUploadIntegrityTests \
 *   -Dtests.upload.iterations=10 -Dtests.upload.buffersPerBlob=600 -Dtests.heap.size=2G \
 *   -Dtests.jvm.argline="-Dreactor.netty.send.maxPrefetchSize=4"
 * </pre>
 * {@code reactor.netty.channel.MonoSend} requests {@code maxPrefetchSize} buffers up front and refills when demand
 * falls to half of it, so lowering it takes the refill path constantly instead of once per 64 buffers. Under that
 * setting the failure reproduces within a handful of uploads.
 */
@SuppressForbidden(reason = "use a http server")
public class AzureUploadIntegrityTests extends ESTestCase {

    private static final String ACCOUNT = "account";
    private static final String CONTAINER = "container";

    /** The upload buffer size {@link AzureBlobStore} reads with, and the block size the production corruption aligned to. */
    private static final int BUFFER_SIZE = Math.toIntExact(ByteSizeValue.of(64, ByteSizeUnit.KB).getBytes());

    /** Uploads per thread. Raised when hunting the rare failure. */
    private static final int ITERATIONS = Integer.getInteger("tests.upload.iterations", 3);

    /** Buffers per blob. Must exceed the send queue's prefetch for the refill path to be exercised at all. */
    private static final int BUFFERS_PER_BLOB = Integer.getInteger("tests.upload.buffersPerBlob", 200);

    /** Bytes the fixture drains between pauses. Lower means the connection spends more time under backpressure. */
    private static final int DRAIN_PAUSE_BYTES = Integer.getInteger("tests.upload.drainPauseBytes", BUFFER_SIZE * 4);

    private HttpServer httpServer;
    private ExecutorService httpServerExecutor;
    private ThreadPool threadPool;
    private AzureClientProvider clientProvider;
    private ClusterService clusterService;

    @Before
    public void startServer() throws Exception {
        threadPool = new TestThreadPool(
            getTestClass().getName(),
            AzureRepositoryPlugin.executorBuilder(Settings.EMPTY),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        // the default executor is single threaded, which would serialise the handlers and defeat the concurrent tests:
        // each upload needs its own handler thread, especially as the handler drains slowly
        httpServerExecutor = EsExecutors.newScaling(
            "azure-upload-integrity-fixture",
            0,
            32,
            60L,
            TimeUnit.SECONDS,
            true,
            EsExecutors.daemonThreadFactory(Settings.EMPTY, "azure-upload-integrity-fixture"),
            new ThreadContext(Settings.EMPTY)
        );
        httpServer.setExecutor(httpServerExecutor);
        httpServer.start();
        clientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY);
        clientProvider.start();
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void stopServer() throws Exception {
        clientProvider.close();
        httpServer.stop(0);
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
        terminate(httpServerExecutor);
    }

    /**
     * Concurrent uploads with no injected failures, so nothing is retried. Production found no retry log lines around
     * the corrupt uploads, so it matters that the plain path is checked and not only the retry path.
     */
    public void testConcurrentUploadsAreByteExact() throws Exception {
        runConcurrentUploads("plain", iteration -> 0);
    }

    /**
     * The same, with attempts failed underneath the uploads so the SDK re-subscribes and reads the blob again from a
     * fresh stream while the previous subscription is being torn down.
     */
    public void testConcurrentUploadsAreByteExactAcrossRetries() throws Exception {
        runConcurrentUploads("retried", iteration -> between(1, 3));
    }

    /**
     * The multi-part path, where several parts of one blob are staged at once against a shared stream provider.
     */
    public void testMultiPartUploadIsByteExact() throws Exception {
        final Fixture fixture = new Fixture(randomHighEntropyBytes(BUFFERS_PER_BLOB * BUFFER_SIZE));
        // a 1MB block size against a multi-megabyte blob, so every upload stages several parts concurrently
        final BlobContainer container = createBlobContainer(fixture, ByteSizeValue.of(1, ByteSizeUnit.MB));

        for (int i = 0; i < ITERATIONS; i++) {
            final String name = "multipart-" + i;
            fixture.expectBlob(name, between(0, 2));
            container.writeBlobAtomic(
                randomPurpose(),
                name,
                fixture.source.length,
                fixture.provider(),
                false,
                threadPool.executor(ThreadPool.Names.SNAPSHOT)
            );
            fixture.assertBlobIntact(name);
        }
    }

    private void runConcurrentUploads(String prefix, IntUnaryOperator failuresForIteration) throws Exception {
        final Fixture fixture = new Fixture(randomHighEntropyBytes(BUFFERS_PER_BLOB * BUFFER_SIZE));
        // single-part uploads, as the production commit blobs were: one PUT carries the whole blob
        final BlobContainer container = createBlobContainer(fixture, ByteSizeValue.of(128, ByteSizeUnit.MB));

        final int threads = 6;
        final CountDownLatch start = new CountDownLatch(1);
        final List<Exception> thrown = new CopyOnWriteArrayList<>();
        final List<Thread> workers = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            final Thread worker = new Thread(() -> {
                try {
                    safeAwait(start);
                    for (int i = 0; i < ITERATIONS; i++) {
                        final String name = prefix + "-" + threadId + "-" + i;
                        fixture.expectBlob(name, failuresForIteration.applyAsInt(i));
                        container.writeBlobAtomic(randomPurpose(), name, fixture.source.length, fixture.provider(), false, Runnable::run);
                        fixture.assertBlobIntact(name);
                    }
                } catch (Exception e) {
                    thrown.add(e);
                }
            }, prefix + "-upload-" + t);
            workers.add(worker);
            worker.start();
        }
        start.countDown();
        for (Thread worker : workers) {
            worker.join();
        }
        fixture.assertNoHandlerFailures();
        if (thrown.isEmpty() == false) {
            final AssertionError error = new AssertionError("concurrent uploads did not round-trip byte-exactly");
            thrown.forEach(error::addSuppressed);
            throw error;
        }
    }

    /**
     * Random bytes rather than a repeating pattern: the fault being looked for is a block that is byte-identical to a
     * nearby block, and with low-entropy content such a match would carry no information.
     */
    private static byte[] randomHighEntropyBytes(int length) {
        final byte[] bytes = new byte[length];
        random().nextBytes(bytes);
        return bytes;
    }

    /**
     * Serves one blob's worth of bytes to the uploads and checks what comes back.
     *
     * <p>Bodies are compared against the source as they are read rather than buffered and compared at the end: the
     * blobs are tens of megabytes and several are in flight at once, so holding them would dominate the test's memory.
     */
    private class Fixture {
        final byte[] source;
        /** Attempts still to be failed, per blob. Absent means the blob is not expected. */
        final Map<String, AtomicInteger> pendingFailures = new ConcurrentHashMap<>();
        /** The verdict for each blob once its upload has been accepted: empty means it matched. */
        final Map<String, String> verdicts = new ConcurrentHashMap<>();
        /** Staged parts of an in-flight multi-part upload, keyed by blob name then block id. */
        final Map<String, Map<String, byte[]>> stagedBlocks = new ConcurrentHashMap<>();
        /** Anything thrown on a server thread, which the HttpServer would otherwise swallow as a dropped connection. */
        final List<Throwable> handlerFailures = new CopyOnWriteArrayList<>();

        Fixture(byte[] source) {
            this.source = source;
        }

        void expectBlob(String name, int failuresFirst) {
            pendingFailures.put(name, new AtomicInteger(failuresFirst));
        }

        boolean shouldFail(String blobName) {
            final AtomicInteger remaining = pendingFailures.get(blobName);
            return remaining != null && remaining.getAndUpdate(n -> Math.max(0, n - 1)) > 0;
        }

        BlobContainer.BlobMultiPartInputStreamProvider provider() {
            return (offset, length) -> new ByteArrayInputStream(source, Math.toIntExact(offset), Math.toIntExact(length));
        }

        void assertBlobIntact(String name) {
            assertNoHandlerFailures();
            final String verdict = verdicts.remove(name);
            assertNotNull("blob [" + name + "] was never accepted by the fixture", verdict);
            if (verdict.isEmpty() == false) {
                throw new AssertionError("blob [" + name + "] does not match what was uploaded" + verdict);
            }
        }

        void assertNoHandlerFailures() {
            if (handlerFailures.isEmpty() == false) {
                final StringBuilder message = new StringBuilder("the test's HTTP handler failed");
                for (Throwable t : handlerFailures) {
                    final StringWriter writer = new StringWriter();
                    t.printStackTrace(new PrintWriter(writer));
                    message.append('\n').append(writer);
                }
                throw new AssertionError(message.toString());
            }
        }

        /**
         * Reads a request body a buffer at a time, comparing each buffer against the source, and pausing periodically
         * so the uploading side cannot write the whole blob into the socket at once.
         *
         * <p>The pauses matter. Over loopback with a fixture that reads as fast as it can, the send queue never fills,
         * so the refill path where the production corruption appeared is only ever taken with no writes outstanding.
         *
         * @return a description of the blocks that differ, or an empty string if the body matched
         */
        String drainAndCompare(InputStream in, int length) throws IOException {
            final StringBuilder differences = new StringBuilder();
            final byte[] received = new byte[BUFFER_SIZE];
            int offset = 0;
            int sinceLastPause = 0;
            while (offset < length) {
                final int expected = Math.min(BUFFER_SIZE, length - offset);
                int filled = 0;
                while (filled < expected) {
                    final int read = in.read(received, filled, expected - filled);
                    if (read == -1) {
                        return differences.append("\n  body ended after ").append(offset + filled).append(" of ").append(length).toString();
                    }
                    filled += read;
                }
                describeIfDifferent(differences, offset, received, expected);
                offset += expected;
                sinceLastPause += expected;
                if (sinceLastPause >= DRAIN_PAUSE_BYTES) {
                    sinceLastPause = 0;
                    safeSleep(1);
                }
            }
            return differences.toString();
        }

        /**
         * Appends a description of one received buffer if it does not match the source at {@code at}, calling out the
         * production shape - a block holding a byte-exact copy of a block a buffer or two earlier - when that is it.
         */
        void describeIfDifferent(StringBuilder differences, int at, byte[] received, int length) {
            if (Arrays.equals(source, at, at + length, received, 0, length)) {
                return;
            }
            final int block = at / BUFFER_SIZE;
            differences.append("\n  block ").append(block).append(" [").append(at).append("..").append(at + length).append(") differs");
            for (int lag = 1; lag <= block; lag++) {
                final int earlier = at - lag * BUFFER_SIZE;
                if (Arrays.equals(source, earlier, earlier + length, received, 0, length)) {
                    differences.append(" and is a byte-exact copy of block ")
                        .append(block - lag)
                        .append(" (lag ")
                        .append((long) lag * BUFFER_SIZE)
                        .append(" bytes) - this is the production corruption signature");
                    return;
                }
            }
        }
    }

    private BlobContainer createBlobContainer(Fixture fixture, ByteSizeValue blockSize) {
        httpServer.createContext("/" + ACCOUNT + "/" + CONTAINER, exchange -> {
            try {
                final String path = exchange.getRequestURI().getPath();
                final String blobName = path.substring(path.lastIndexOf('/') + 1);
                final Map<String, String> params = queryParams(exchange.getRequestURI().getQuery());

                if ("PUT".equals(exchange.getRequestMethod()) == false) {
                    exchange.sendResponseHeaders(RestStatus.METHOD_NOT_ALLOWED.getStatus(), -1);
                    return;
                }

                if ("blocklist".equals(params.get("comp"))) {
                    commitBlockList(fixture, exchange, blobName);
                    return;
                }

                if (fixture.shouldFail(blobName)) {
                    // reject the attempt so the SDK retries and re-subscribes, reading the blob again from a fresh
                    // stream. The body is drained first so the failure lands on the response rather than mid-upload.
                    Streams.readFully(exchange.getRequestBody());
                    AzureHttpHandler.sendError(exchange, RestStatus.SERVICE_UNAVAILABLE);
                    return;
                }

                final String blockId = params.get("blockid");
                if (blockId == null) {
                    fixture.verdicts.put(blobName, fixture.drainAndCompare(exchange.getRequestBody(), fixture.source.length));
                } else {
                    // a staged part carries no offset, so it is kept until the block list says where it belongs
                    fixture.stagedBlocks.computeIfAbsent(blobName, ignored -> new ConcurrentHashMap<>())
                        .put(blockId, BytesReference.toBytes(Streams.readFully(exchange.getRequestBody())));
                }
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
            } catch (Throwable t) {
                fixture.handlerFailures.add(t);
            } finally {
                exchange.close();
            }
        });

        return createBlobContainer(blockSize);
    }

    /** Assembles a multi-part upload in the order the block list gives, and compares the result against the source. */
    private static void commitBlockList(Fixture fixture, HttpExchange exchange, String blobName) throws IOException {
        final String body = new String(BytesReference.toBytes(Streams.readFully(exchange.getRequestBody())), UTF_8);
        final Map<String, byte[]> staged = fixture.stagedBlocks.getOrDefault(blobName, Map.of());
        final StringBuilder differences = new StringBuilder();
        int offset = 0;
        for (String blockId : parseBlockIds(body)) {
            final byte[] part = staged.get(blockId);
            assertNotNull("block [" + blockId + "] of blob [" + blobName + "] was never staged", part);
            for (int at = 0; at < part.length; at += BUFFER_SIZE) {
                final int length = Math.min(BUFFER_SIZE, part.length - at);
                fixture.describeIfDifferent(differences, offset + at, Arrays.copyOfRange(part, at, at + length), length);
            }
            offset += part.length;
        }
        if (offset != fixture.source.length) {
            differences.append("\n  assembled length ").append(offset).append(" != expected ").append(fixture.source.length);
        }
        fixture.verdicts.put(blobName, differences.toString());
        exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
        exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
    }

    private BlobContainer createBlobContainer(ByteSizeValue blockSize) {
        final String clientName = "test";
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), ACCOUNT);
        secureSettings.setString(
            KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
            Base64.getEncoder().encodeToString(randomAlphaOfLength(14).getBytes(UTF_8))
        );

        final InetSocketAddress address = httpServer.getAddress();
        final String host = address.getAddress().isLoopbackAddress() ? "localhost" : InetAddresses.toUriString(address.getAddress());
        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=http://"
            + host
            + ":"
            + address.getPort()
            + "/"
            + ACCOUNT;

        final Settings settings = Settings.builder()
            .put(ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint)
            .put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), 10)
            .put(TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), TimeValue.timeValueSeconds(60))
            .setSecureSettings(secureSettings)
            .build();

        final AzureStorageService service = new AzureStorageService(
            settings,
            clientProvider,
            clusterService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        ) {
            @Override
            RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
                final RequestRetryOptions options = super.getRetryOptions(locationMode, azureStorageSettings);
                return new RequestRetryOptions(
                    RetryPolicyType.EXPONENTIAL,
                    options.getMaxTries(),
                    options.getTryTimeoutDuration(),
                    Duration.ofMillis(10),
                    Duration.ofMillis(50),
                    null
                );
            }

            @Override
            long getUploadBlockSize() {
                return blockSize.getBytes();
            }
        };

        final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
            "repository",
            AzureRepository.TYPE,
            Settings.builder()
                .put(CONTAINER_SETTING.getKey(), CONTAINER)
                .put(ACCOUNT_SETTING.getKey(), clientName)
                .put(MAX_SINGLE_PART_UPLOAD_SIZE_SETTING.getKey(), blockSize)
                .build()
        );

        return new AzureBlobContainer(
            BlobPath.EMPTY,
            new AzureBlobStore(
                ProjectId.DEFAULT,
                repositoryMetadata,
                service,
                BigArrays.NON_RECYCLING_INSTANCE,
                RepositoriesMetrics.NOOP,
                null,
                null
            )
        );
    }

    private static Map<String, String> queryParams(String query) {
        if (query == null || query.isEmpty()) {
            return Map.of();
        }
        final Map<String, String> params = new HashMap<>();
        for (String pair : query.split("&")) {
            final int eq = pair.indexOf('=');
            if (eq > 0) {
                params.put(
                    pair.substring(0, eq).toLowerCase(Locale.ROOT),
                    // block ids are base64 and contain '+', which URLDecoder would otherwise turn into a space
                    URLDecoder.decode(pair.substring(eq + 1).replace("+", "%2B"), UTF_8)
                );
            }
        }
        return params;
    }

    /** Pulls the {@code <Latest>} block ids, in order, out of a commit-block-list request body. */
    private static List<String> parseBlockIds(String body) {
        final List<String> ids = new ArrayList<>();
        final Matcher matcher = Pattern.compile("<Latest>([^<]+)</Latest>").matcher(body);
        while (matcher.find()) {
            ids.add(matcher.group(1));
        }
        return ids;
    }
}
