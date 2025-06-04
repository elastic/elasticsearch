/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.s3;

import fixture.s3.S3HttpHandler;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.http.HttpStatus;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.DnsResolver;
import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.blobstore.AbstractBlobContainerRetriesTestCase;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;
import java.util.regex.Pattern;

import static org.elasticsearch.cluster.node.DiscoveryNode.STATELESS_ENABLED_SETTING_NAME;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomNonDataPurpose;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.repositories.s3.S3ClientSettings.DISABLE_CHUNKED_ENCODING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.MAX_CONNECTIONS_SETTING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.READ_TIMEOUT_SETTING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * This class tests how a {@link S3BlobContainer} and its underlying AWS S3 client are retrying requests when reading or writing blobs.
 */
@SuppressForbidden(reason = "use a http server")
public class S3BlobContainerRetriesTests extends AbstractBlobContainerRetriesTestCase {

    private static final int MAX_NUMBER_SNAPSHOT_DELETE_RETRIES = 10;
    private S3Service service;
    private volatile boolean shouldErrorOnDns;
    private RecordingMeterRegistry recordingMeterRegistry;

    @Before
    public void setUp() throws Exception {
        shouldErrorOnDns = false;
        service = new S3Service(
            Mockito.mock(Environment.class),
            ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool()),
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            Mockito.mock(ResourceWatcherService.class),
            () -> null
        ) {
            private InetAddress[] resolveHost(String host) throws UnknownHostException {
                assertEquals("127.0.0.1", host);
                if (shouldErrorOnDns && randomBoolean() && randomBoolean()) {
                    throw new UnknownHostException(host);
                }
                return new InetAddress[] { InetAddress.getLoopbackAddress() };
            }

            @Override
            DnsResolver getCustomDnsResolver() {
                return this::resolveHost;
            }

            /**
             * Overrides the S3Client's HTTP Client connection acquisition timeout. Essentially, once the client's max connection number is
             * reached ({@link S3ClientSettings#MAX_CONNECTIONS_SETTING}), new requests will fail (timeout) fast when a connection is not
             * available.
             */
            @Override
            Optional<Duration> getConnectionAcquisitionTimeout() {
                // This override is used to make requests timeout nearly immediately if the max number of concurrent connections is reached
                // on the HTTP client.
                return Optional.of(Duration.of(1, ChronoUnit.MILLIS));
            }

        };
        service.start();
        recordingMeterRegistry = new RecordingMeterRegistry();
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.close(service);
        super.tearDown();
    }

    @Override
    protected String downloadStorageEndpoint(BlobContainer container, String blob) {
        return "/bucket/" + container.path().buildAsString() + blob;
    }

    @Override
    protected String bytesContentType() {
        return "text/plain; charset=utf-8";
    }

    @Override
    protected Class<? extends Exception> unresponsiveExceptionType() {
        return SdkException.class;
    }

    @Override
    protected BlobContainer createBlobContainer(
        final @Nullable Integer maxRetries,
        final @Nullable TimeValue readTimeout,
        final @Nullable Boolean disableChunkedEncoding,
        final @Nullable Integer maxConnections,
        final @Nullable ByteSizeValue bufferSize,
        final @Nullable Integer maxBulkDeletes,
        final @Nullable BlobPath blobContainerPath
    ) {
        final Settings.Builder clientSettings = Settings.builder();
        final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        final InetSocketAddress address = httpServer.getAddress();
        final String endpoint = "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
        logger.info("--> creating client with endpoint [{}]", endpoint);
        clientSettings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint);

        if (maxRetries != null) {
            clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxRetries);
        }
        if (readTimeout != null) {
            clientSettings.put(READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), readTimeout);
        }
        if (disableChunkedEncoding != null) {
            clientSettings.put(DISABLE_CHUNKED_ENCODING.getConcreteSettingForNamespace(clientName).getKey(), disableChunkedEncoding);
        }
        if (maxConnections != null) {
            clientSettings.put(MAX_CONNECTIONS_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxConnections);
        }

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(
            S3ClientSettings.ACCESS_KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
            "test_access_key"
        );
        secureSettings.setString(
            S3ClientSettings.SECRET_KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
            "test_secret_key"
        );
        clientSettings.setSecureSettings(secureSettings);
        service.refreshAndClearCache(S3ClientSettings.load(clientSettings.build()));

        final var repositorySettings = Settings.builder()
            .put(S3Repository.CLIENT_NAME.getKey(), clientName)
            .put(S3Repository.GET_REGISTER_RETRY_DELAY.getKey(), TimeValue.ZERO);
        if (maxBulkDeletes != null) {
            repositorySettings.put(S3Repository.DELETION_BATCH_SIZE_SETTING.getKey(), maxBulkDeletes);
        }
        final RepositoryMetadata repositoryMetadata = new RepositoryMetadata("repository", S3Repository.TYPE, repositorySettings.build());

        final S3BlobStore s3BlobStore = new S3BlobStore(
            randomProjectIdOrDefault(),
            service,
            "bucket",
            S3Repository.SERVER_SIDE_ENCRYPTION_SETTING.getDefault(Settings.EMPTY),
            bufferSize == null ? S3Repository.BUFFER_SIZE_SETTING.getDefault(Settings.EMPTY) : bufferSize,
            S3Repository.MAX_COPY_SIZE_BEFORE_MULTIPART.getDefault(Settings.EMPTY),
            S3Repository.CANNED_ACL_SETTING.getDefault(Settings.EMPTY),
            S3Repository.STORAGE_CLASS_SETTING.getDefault(Settings.EMPTY),
            repositoryMetadata,
            BigArrays.NON_RECYCLING_INSTANCE,
            new DeterministicTaskQueue().getThreadPool(),
            new S3RepositoriesMetrics(new RepositoriesMetrics(recordingMeterRegistry)),
            BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(1), MAX_NUMBER_SNAPSHOT_DELETE_RETRIES)
        );
        return new S3BlobContainer(
            Objects.requireNonNullElse(blobContainerPath, randomBoolean() ? BlobPath.EMPTY : BlobPath.EMPTY.add("foo")),
            s3BlobStore
        ) {
            @Override
            public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
                return new AssertingInputStream(new S3RetryingInputStream(purpose, s3BlobStore, buildKey(blobName)) {
                    @Override
                    protected long getRetryDelayInMillis() {
                        assert super.getRetryDelayInMillis() > 0;
                        return 0;
                    }
                }, blobName);
            }

            @Override
            public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                final InputStream inputStream;
                if (length == 0) {
                    inputStream = new ByteArrayInputStream(new byte[0]);
                } else {
                    inputStream = new S3RetryingInputStream(
                        purpose,
                        s3BlobStore,
                        buildKey(blobName),
                        position,
                        Math.addExact(position, length - 1)
                    ) {
                        @Override
                        protected long getRetryDelayInMillis() {
                            assert super.getRetryDelayInMillis() > 0;
                            return 0;
                        }
                    };
                }
                return new AssertingInputStream(inputStream, blobName, position, length);
            }
        };
    }

    private static S3HttpHandler.S3Request parseRequest(HttpExchange exchange) {
        return new S3HttpHandler("bucket").parseRequest(exchange);
    }

    public void testWriteBlobWithRetries() throws Exception {
        final int maxRetries = randomInt(5);
        final CountDown countDown = new CountDown(maxRetries + 1);

        final BlobContainer blobContainer = blobContainerBuilder().maxRetries(maxRetries).disableChunkedEncoding(true).build();

        final byte[] bytes = randomBlobContent();
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "write_blob_max_retries"), exchange -> {
            final S3HttpHandler.S3Request s3Request = parseRequest(exchange);
            if (s3Request.isPutObjectRequest()) {
                if (countDown.countDown()) {
                    final BytesReference body = Streams.readFully(exchange.getRequestBody());
                    if (Objects.deepEquals(bytes, BytesReference.toBytes(body))) {
                        exchange.sendResponseHeaders(HttpStatus.SC_OK, -1);
                    } else {
                        exchange.sendResponseHeaders(HttpStatus.SC_BAD_REQUEST, -1);
                    }
                    exchange.close();
                    return;
                }

                if (randomBoolean()) {
                    if (randomBoolean()) {
                        org.elasticsearch.core.Streams.readFully(
                            exchange.getRequestBody(),
                            new byte[randomIntBetween(1, Math.max(1, bytes.length - 1))]
                        );
                    } else {
                        Streams.readFully(exchange.getRequestBody());
                        exchange.sendResponseHeaders(
                            randomFrom(
                                HttpStatus.SC_INTERNAL_SERVER_ERROR,
                                HttpStatus.SC_BAD_GATEWAY,
                                HttpStatus.SC_SERVICE_UNAVAILABLE,
                                HttpStatus.SC_GATEWAY_TIMEOUT
                            ),
                            -1
                        );
                    }
                }
                exchange.close();
            }
        });
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
            blobContainer.writeBlob(randomPurpose(), "write_blob_max_retries", stream, bytes.length, false);
        }
        assertThat(countDown.isCountedDown(), is(true));
    }

    public void testWriteBlobWithReadTimeouts() {
        final byte[] bytes = randomByteArrayOfLength(randomIntBetween(10, 128));
        final TimeValue readTimeout = TimeValue.timeValueMillis(randomIntBetween(100, 500));
        final BlobContainer blobContainer = blobContainerBuilder().maxRetries(1)
            .readTimeout(readTimeout)
            .disableChunkedEncoding(true)
            .build();

        // HTTP server does not send a response
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "write_blob_timeout"), exchange -> {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    org.elasticsearch.core.Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, bytes.length - 1)]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                }
            }
        });

        Exception exception = expectThrows(IOException.class, () -> {
            try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
                blobContainer.writeBlob(randomPurpose(), "write_blob_timeout", stream, bytes.length, false);
            }
        });
        assertThat(
            exception.getMessage().toLowerCase(Locale.ROOT),
            containsString("unable to upload object [" + blobContainer.path().buildAsString() + "write_blob_timeout] using a single upload")
        );

        assertThat(exception.getCause(), instanceOf(SdkClientException.class));
        assertThat(exception.getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));

        assertThat(exception.getCause().getCause(), instanceOf(SocketTimeoutException.class));
        assertThat(exception.getCause().getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
    }

    public void testWriteLargeBlob() throws Exception {
        final boolean useTimeout = rarely();
        final TimeValue readTimeout = useTimeout ? TimeValue.timeValueMillis(randomIntBetween(100, 500)) : null;
        final ByteSizeValue bufferSize = ByteSizeValue.of(5, ByteSizeUnit.MB);
        final BlobContainer blobContainer = blobContainerBuilder().readTimeout(readTimeout)
            .disableChunkedEncoding(true)
            .bufferSize(bufferSize)
            .build();

        final int parts = randomIntBetween(1, 5);
        final long lastPartSize = randomLongBetween(10, 512);
        final long blobSize = (parts * bufferSize.getBytes()) + lastPartSize;

        final int nbErrors = 2; // we want all requests to fail at least once
        final CountDown countDownInitiate = new CountDown(nbErrors);
        final AtomicInteger countDownUploads = new AtomicInteger(nbErrors * (parts + 1));
        final CountDown countDownComplete = new CountDown(nbErrors);

        httpServer.createContext(downloadStorageEndpoint(blobContainer, "write_large_blob"), exchange -> {
            final S3HttpHandler.S3Request s3Request = parseRequest(exchange);
            final long contentLength = Long.parseLong(exchange.getRequestHeaders().getFirst("Content-Length"));

            if (s3Request.isInitiateMultipartUploadRequest()) {
                // initiate multipart upload request
                if (countDownInitiate.countDown()) {
                    byte[] response = ("""
                        <?xml version="1.0" encoding="UTF-8"?>
                        <InitiateMultipartUploadResult>
                          <Bucket>bucket</Bucket>
                          <Key>write_large_blob</Key>
                          <UploadId>TEST</UploadId>
                        </InitiateMultipartUploadResult>""").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
                    exchange.getResponseBody().write(response);
                    exchange.close();
                    return;
                }
            } else if (s3Request.isUploadPartRequest()) {
                // upload part request
                BytesReference bytes = Streams.readFully(exchange.getRequestBody());
                assertThat((long) bytes.length(), anyOf(equalTo(lastPartSize), equalTo(bufferSize.getBytes())));
                assertThat(contentLength, anyOf(equalTo(lastPartSize), equalTo(bufferSize.getBytes())));

                if (countDownUploads.decrementAndGet() % 2 == 0) {
                    exchange.getResponseHeaders().add("ETag", getBase16MD5Digest(bytes));
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, -1);
                    exchange.close();
                    return;
                }

            } else if (s3Request.isCompleteMultipartUploadRequest()) {
                // complete multipart upload request
                if (countDownComplete.countDown()) {
                    Streams.readFully(exchange.getRequestBody());
                    byte[] response = ("""
                        <?xml version="1.0" encoding="UTF-8"?>
                        <CompleteMultipartUploadResult>
                          <Bucket>bucket</Bucket>
                          <Key>write_large_blob</Key>
                        </CompleteMultipartUploadResult>""").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
                    exchange.getResponseBody().write(response);
                    exchange.close();
                    return;
                }
            }

            // sends an error back or let the request time out
            if (useTimeout == false) {
                if (randomBoolean() && contentLength > 0) {
                    org.elasticsearch.core.Streams.readFully(
                        exchange.getRequestBody(),
                        new byte[randomIntBetween(1, Math.toIntExact(contentLength - 1))]
                    );
                } else {
                    Streams.readFully(exchange.getRequestBody());
                    exchange.sendResponseHeaders(
                        randomFrom(
                            HttpStatus.SC_INTERNAL_SERVER_ERROR,
                            HttpStatus.SC_BAD_GATEWAY,
                            HttpStatus.SC_SERVICE_UNAVAILABLE,
                            HttpStatus.SC_GATEWAY_TIMEOUT
                        ),
                        -1
                    );
                }
                exchange.close();
            }
        });

        blobContainer.writeBlob(randomPurpose(), "write_large_blob", new ZeroInputStream(blobSize), blobSize, false);

        assertThat(countDownInitiate.isCountedDown(), is(true));
        assertThat(countDownUploads.get(), equalTo(0));
        assertThat(countDownComplete.isCountedDown(), is(true));
    }

    public void testWriteLargeBlobStreaming() throws Exception {
        final boolean useTimeout = rarely();
        final TimeValue readTimeout = useTimeout ? TimeValue.timeValueMillis(randomIntBetween(100, 500)) : null;
        final ByteSizeValue bufferSize = ByteSizeValue.of(5, ByteSizeUnit.MB);
        final BlobContainer blobContainer = blobContainerBuilder().readTimeout(readTimeout)
            .disableChunkedEncoding(true)
            .bufferSize(bufferSize)
            .build();

        final int parts = randomIntBetween(1, 5);
        final long lastPartSize = randomLongBetween(10, 512);
        final long blobSize = (parts * bufferSize.getBytes()) + lastPartSize;

        final int nbErrors = 2; // we want all requests to fail at least once
        final CountDown countDownInitiate = new CountDown(nbErrors);
        final AtomicInteger counterUploads = new AtomicInteger(0);
        final AtomicLong bytesReceived = new AtomicLong(0L);
        final CountDown countDownComplete = new CountDown(nbErrors);

        httpServer.createContext(downloadStorageEndpoint(blobContainer, "write_large_blob_streaming"), exchange -> {
            final S3HttpHandler.S3Request s3Request = parseRequest(exchange);
            final long contentLength = Long.parseLong(exchange.getRequestHeaders().getFirst("Content-Length"));

            if (s3Request.isInitiateMultipartUploadRequest()) {
                // initiate multipart upload request
                if (countDownInitiate.countDown()) {
                    byte[] response = ("""
                        <?xml version="1.0" encoding="UTF-8"?>
                        <InitiateMultipartUploadResult>
                          <Bucket>bucket</Bucket>
                          <Key>write_large_blob_streaming</Key>
                          <UploadId>TEST</UploadId>
                        </InitiateMultipartUploadResult>""").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
                    exchange.getResponseBody().write(response);
                    exchange.close();
                    return;
                }
            } else if (s3Request.isUploadPartRequest()) {
                // upload part request
                BytesReference bytes = Streams.readFully(exchange.getRequestBody());

                if (counterUploads.incrementAndGet() % 2 == 0) {
                    bytesReceived.addAndGet(bytes.length());
                    exchange.getResponseHeaders().add("ETag", getBase16MD5Digest(bytes));
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, -1);
                    exchange.close();
                    return;
                }

            } else if (s3Request.isCompleteMultipartUploadRequest()) {
                // complete multipart upload request
                if (countDownComplete.countDown()) {
                    Streams.readFully(exchange.getRequestBody());
                    byte[] response = ("""
                        <?xml version="1.0" encoding="UTF-8"?>
                        <CompleteMultipartUploadResult>
                          <Bucket>bucket</Bucket>
                          <Key>write_large_blob_streaming</Key>
                        </CompleteMultipartUploadResult>""").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
                    exchange.getResponseBody().write(response);
                    exchange.close();
                    return;
                }
            }

            // sends an error back or let the request time out
            if (useTimeout == false) {
                if (randomBoolean() && contentLength > 0) {
                    org.elasticsearch.core.Streams.readFully(
                        exchange.getRequestBody(),
                        new byte[randomIntBetween(1, Math.toIntExact(contentLength - 1))]
                    );
                } else {
                    Streams.readFully(exchange.getRequestBody());
                    exchange.sendResponseHeaders(
                        randomFrom(
                            HttpStatus.SC_INTERNAL_SERVER_ERROR,
                            HttpStatus.SC_BAD_GATEWAY,
                            HttpStatus.SC_SERVICE_UNAVAILABLE,
                            HttpStatus.SC_GATEWAY_TIMEOUT
                        ),
                        -1
                    );
                }
                exchange.close();
            }
        });

        blobContainer.writeMetadataBlob(randomNonDataPurpose(), "write_large_blob_streaming", false, randomBoolean(), out -> {
            final byte[] buffer = new byte[16 * 1024];
            long outstanding = blobSize;
            while (outstanding > 0) {
                if (randomBoolean()) {
                    int toWrite = Math.toIntExact(Math.min(randomIntBetween(64, buffer.length), outstanding));
                    out.write(buffer, 0, toWrite);
                    outstanding -= toWrite;
                } else {
                    out.write(0);
                    outstanding--;
                }
            }
        });

        assertEquals(blobSize, bytesReceived.get());
    }

    public void testMaxConnections() throws InterruptedException, IOException {
        final CountDownLatch requestReceived = new CountDownLatch(1);
        final CountDownLatch releaseRequest = new CountDownLatch(1);
        int maxConnections = 1;
        final BlobContainer blobContainer = createBlobContainer(null, null, null, maxConnections, null, null, null);

        // Setting up a simple request handler that returns NOT_FOUND, so as to avoid setting up a response.
        @SuppressForbidden(reason = "use a http server")
        class NotFoundReadHandler implements HttpHandler {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                logger.info("---> First request received");
                // Signal that the request has begun.
                requestReceived.countDown();
                try {
                    // Wait for a signal to stop hanging.
                    releaseRequest.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                logger.info("---> First request released");
                exchange.sendResponseHeaders(HttpStatus.SC_NOT_FOUND, -1);
                exchange.close();
                logger.info("---> First request finished");
            }
        }
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "max_connection_timeout"), new NotFoundReadHandler());

        // Start up an async thread to monopolize the one http client connection (the request will hang per the above handling).
        Thread thread = new Thread(() -> {
            expectThrows(NoSuchFileException.class, () -> {
                try (InputStream inputStream = blobContainer.readBlob(randomRetryingPurpose(), "max_connection_timeout")) {
                    Streams.readFully(inputStream);
                }
            });
        });
        thread.start();
        logger.info("---> Sending first request");
        requestReceived.await();
        logger.info("---> First request was received and is hanging");

        // Now we'll run a second request, which should error out with a connection exception.
        final var exception = expectThrows(SdkClientException.class, () -> {
            try (
                var inputStream = blobContainer.readBlob(
                    OperationPurpose.REPOSITORY_ANALYSIS /* no retries needed */,
                    "read_blob_not_found"
                )
            ) {
                Streams.readFully(inputStream);
            } finally {
                releaseRequest.countDown();
                logger.info("---> First request is released");
                thread.join();
                logger.info("---> First request has finished");
            }
        });

        assertThat(exception, instanceOf(SdkClientException.class));
        assertThat(exception.getCause(), instanceOf(ConnectionPoolTimeoutException.class));

        assertThat(exception.getMessage(), containsString("Unable to execute HTTP request: Timeout waiting for connection from pool"));
        assertThat(exception.getCause().getMessage(), containsString("Timeout waiting for connection from pool"));
    }

    public void testReadRetriesAfterMeaningfulProgress() throws Exception {
        final int maxRetries = between(0, 5);
        final int bufferSizeBytes = scaledRandomIntBetween(
            0,
            randomFrom(1000, Math.toIntExact(S3Repository.BUFFER_SIZE_SETTING.get(Settings.EMPTY).getBytes()))
        );
        final BlobContainer blobContainer = blobContainerBuilder().maxRetries(maxRetries)
            .disableChunkedEncoding(true)
            .bufferSize(ByteSizeValue.ofBytes(bufferSizeBytes))
            .build();
        final int meaningfulProgressBytes = Math.max(1, bufferSizeBytes / 100);

        final byte[] bytes = randomBlobContent();

        @SuppressForbidden(reason = "use a http server")
        class FlakyReadHandler implements HttpHandler {
            private int failuresWithoutProgress;

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                Streams.readFully(exchange.getRequestBody());
                if (failuresWithoutProgress >= maxRetries) {
                    final int rangeStart = getRangeStart(exchange);
                    assertThat(rangeStart, lessThan(bytes.length));
                    exchange.getResponseHeaders().add("Content-Type", bytesContentType());
                    final var remainderLength = bytes.length - rangeStart;
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, remainderLength);
                    exchange.getResponseBody()
                        .write(
                            bytes,
                            rangeStart,
                            remainderLength < meaningfulProgressBytes ? remainderLength : between(meaningfulProgressBytes, remainderLength)
                        );
                } else if (randomBoolean()) {
                    failuresWithoutProgress += 1;
                    exchange.sendResponseHeaders(
                        randomFrom(
                            HttpStatus.SC_INTERNAL_SERVER_ERROR,
                            HttpStatus.SC_BAD_GATEWAY,
                            HttpStatus.SC_SERVICE_UNAVAILABLE,
                            HttpStatus.SC_GATEWAY_TIMEOUT
                        ),
                        -1
                    );
                    exchange.getResponseBody().flush();
                } else if (randomBoolean()) {
                    final var bytesSent = sendIncompleteContent(exchange, bytes);
                    if (bytesSent < meaningfulProgressBytes) {
                        failuresWithoutProgress += 1;
                    }
                } else {
                    failuresWithoutProgress += 1;
                }
                exchange.getResponseBody().flush();
                exchange.close();
            }
        }

        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_blob_max_retries"), new FlakyReadHandler());

        try (InputStream inputStream = blobContainer.readBlob(randomRetryingPurpose(), "read_blob_max_retries")) {
            final int readLimit;
            final InputStream wrappedStream;
            if (randomBoolean()) {
                // read stream only partly
                readLimit = randomIntBetween(0, bytes.length);
                wrappedStream = Streams.limitStream(inputStream, readLimit);
            } else {
                readLimit = bytes.length;
                wrappedStream = inputStream;
            }
            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(wrappedStream));
            assertArrayEquals(Arrays.copyOfRange(bytes, 0, readLimit), bytesRead);
        }
    }

    public void testReadDoesNotRetryForRepositoryAnalysis() {
        final int maxRetries = between(0, 5);
        final int bufferSizeBytes = scaledRandomIntBetween(
            0,
            randomFrom(1000, Math.toIntExact(S3Repository.BUFFER_SIZE_SETTING.get(Settings.EMPTY).getBytes()))
        );
        final BlobContainer blobContainer = blobContainerBuilder().maxRetries(maxRetries)
            .disableChunkedEncoding(true)
            .bufferSize(ByteSizeValue.ofBytes(bufferSizeBytes))
            .build();

        final byte[] bytes = randomBlobContent();

        @SuppressForbidden(reason = "use a http server")
        class FlakyReadHandler implements HttpHandler {
            private int failureCount;

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                if (failureCount != 0) {
                    ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError("failureCount=" + failureCount));
                }
                failureCount += 1;
                Streams.readFully(exchange.getRequestBody());
                sendIncompleteContent(exchange, bytes);
                exchange.getResponseBody().flush();
                exchange.close();
            }
        }

        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_blob_repo_analysis"), new FlakyReadHandler());

        expectThrows(Exception.class, () -> {
            try (InputStream inputStream = blobContainer.readBlob(OperationPurpose.REPOSITORY_ANALYSIS, "read_blob_repo_analysis")) {
                final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(inputStream));
                assertArrayEquals(Arrays.copyOfRange(bytes, 0, bytes.length), bytesRead);
            }
        });
    }

    public void testReadWithIndicesPurposeRetriesForever() throws IOException {
        final int maxRetries = between(0, 5);
        final int totalFailures = Math.max(30, maxRetries * between(30, 80));
        final int bufferSizeBytes = scaledRandomIntBetween(
            0,
            randomFrom(1000, Math.toIntExact(S3Repository.BUFFER_SIZE_SETTING.get(Settings.EMPTY).getBytes()))
        );
        final BlobContainer blobContainer = blobContainerBuilder().maxRetries(maxRetries)
            .disableChunkedEncoding(true)
            .bufferSize(ByteSizeValue.ofBytes(bufferSizeBytes))
            .build();
        final int meaningfulProgressBytes = Math.max(1, bufferSizeBytes / 100);

        final byte[] bytes = randomBlobContent(512);

        shouldErrorOnDns = true;
        final AtomicInteger failures = new AtomicInteger();
        @SuppressForbidden(reason = "use a http server")
        class FlakyReadHandler implements HttpHandler {

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                Streams.readFully(exchange.getRequestBody());
                if (failures.get() > totalFailures && randomBoolean()) {
                    final int rangeStart = getRangeStart(exchange);
                    assertThat(rangeStart, lessThan(bytes.length));
                    exchange.getResponseHeaders().add("Content-Type", bytesContentType());
                    final OptionalInt rangeEnd = getRangeEnd(exchange);
                    final int length;
                    if (rangeEnd.isPresent() == false) {
                        final var remainderLength = bytes.length - rangeStart;
                        exchange.sendResponseHeaders(HttpStatus.SC_OK, remainderLength);
                        length = remainderLength < meaningfulProgressBytes
                            ? remainderLength
                            : between(meaningfulProgressBytes, remainderLength);
                    } else {
                        final int effectiveRangeEnd = Math.min(bytes.length - 1, rangeEnd.getAsInt());
                        length = (effectiveRangeEnd - rangeStart) + 1;
                        exchange.sendResponseHeaders(HttpStatus.SC_OK, length);
                    }
                    exchange.getResponseBody().write(bytes, rangeStart, length);
                } else {
                    if (randomBoolean()) {
                        failures.incrementAndGet();
                        exchange.sendResponseHeaders(
                            randomFrom(
                                HttpStatus.SC_INTERNAL_SERVER_ERROR,
                                HttpStatus.SC_BAD_GATEWAY,
                                HttpStatus.SC_SERVICE_UNAVAILABLE,
                                HttpStatus.SC_GATEWAY_TIMEOUT
                            ),
                            -1
                        );
                    } else {
                        if (randomBoolean()) {
                            final var bytesSent = sendIncompleteContent(exchange, bytes);
                            if (bytesSent >= meaningfulProgressBytes) {
                                exchange.getResponseBody().flush();
                            }
                        } else {
                            failures.incrementAndGet();
                        }
                    }
                }
                exchange.close();
            }
        }

        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_blob_retries_forever"), new FlakyReadHandler());

        // Ranged read
        final int position = between(0, bytes.length - 2);
        final int length = between(1, randomBoolean() ? bytes.length : Integer.MAX_VALUE);
        logger.info("--> position={}, length={}", position, length);
        try (InputStream inputStream = blobContainer.readBlob(OperationPurpose.INDICES, "read_blob_retries_forever", position, length)) {
            assertMetricsForOpeningStream();
            recordingMeterRegistry.getRecorder().resetCalls();
            failures.set(0);

            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(inputStream));
            assertArrayEquals(Arrays.copyOfRange(bytes, position, Math.min(bytes.length, position + length)), bytesRead);
            assertMetricsForReadingStream();
        }
        assertThat(failures.get(), greaterThan(totalFailures));

        // Read the whole blob
        failures.set(0);
        recordingMeterRegistry.getRecorder().resetCalls();
        try (InputStream inputStream = blobContainer.readBlob(OperationPurpose.INDICES, "read_blob_retries_forever")) {
            assertMetricsForOpeningStream();
            recordingMeterRegistry.getRecorder().resetCalls();
            failures.set(0);

            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(inputStream));
            assertArrayEquals(bytes, bytesRead);

            assertMetricsForReadingStream();
        }
        assertThat(failures.get(), greaterThan(totalFailures));
    }

    public void testDoesNotRetryOnNotFound() {
        final int maxRetries = between(3, 5);
        final BlobContainer blobContainer = blobContainerBuilder().maxRetries(maxRetries).disableChunkedEncoding(true).build();

        final AtomicInteger numberOfReads = new AtomicInteger(0);
        @SuppressForbidden(reason = "use a http server")
        class NotFoundReadHandler implements HttpHandler {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                numberOfReads.incrementAndGet();
                exchange.sendResponseHeaders(HttpStatus.SC_NOT_FOUND, -1);
                exchange.close();
            }
        }

        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_blob_not_found"), new NotFoundReadHandler());
        expectThrows(NoSuchFileException.class, () -> {
            try (
                InputStream inputStream = randomBoolean()
                    ? blobContainer.readBlob(randomRetryingPurpose(), "read_blob_not_found")
                    : blobContainer.readBlob(randomRetryingPurpose(), "read_blob_not_found", between(0, 100), between(1, 100))
            ) {
                Streams.readFully(inputStream);

            }
        });
        assertThat(numberOfReads.get(), equalTo(1));
        assertThat(getRetryStartedMeasurements(), empty());
        assertThat(getRetryCompletedMeasurements(), empty());
        assertThat(getRetryHistogramMeasurements(), empty());
    }

    public void testSnapshotDeletesRetryOnThrottlingError() throws IOException {
        final BlobContainer blobContainer = blobContainerBuilder()
            // disable AWS-client retries
            .maxRetries(0)
            .disableChunkedEncoding(true)
            .build();

        int numBlobsToDelete = randomIntBetween(500, 3000);
        List<String> blobsToDelete = new ArrayList<>();
        for (int i = 0; i < numBlobsToDelete; i++) {
            blobsToDelete.add(randomIdentifier());
        }
        int throttleTimesBeforeSuccess = randomIntBetween(1, MAX_NUMBER_SNAPSHOT_DELETE_RETRIES);
        logger.info("--> Throttling {} times before success", throttleTimesBeforeSuccess);
        ThrottlingDeleteHandler handler = new ThrottlingDeleteHandler(throttleTimesBeforeSuccess, attempt -> {});
        httpServer.createContext("/", handler);
        blobContainer.deleteBlobsIgnoringIfNotExists(randomFrom(operationPurposesThatRetryOnDelete()), blobsToDelete.iterator());

        int expectedNumberOfBatches = expectedNumberOfBatches(numBlobsToDelete);
        assertThat(handler.numberOfDeleteAttempts.get(), equalTo(throttleTimesBeforeSuccess + expectedNumberOfBatches));
        assertThat(handler.numberOfSuccessfulDeletes.get(), equalTo(expectedNumberOfBatches));
    }

    public void testSnapshotDeletesAbortRetriesWhenThreadIsInterrupted() {
        final BlobContainer blobContainer = blobContainerBuilder()
            // disable AWS-client retries
            .maxRetries(0)
            .disableChunkedEncoding(true)
            .build();

        int numBlobsToDelete = randomIntBetween(500, 3000);
        List<String> blobsToDelete = new ArrayList<>();
        for (int i = 0; i < numBlobsToDelete; i++) {
            blobsToDelete.add(randomIdentifier());
        }

        final Thread clientThread = Thread.currentThread();
        int interruptBeforeAttempt = randomIntBetween(0, randomIntBetween(1, 10));
        logger.info("--> Deleting {} blobs, interrupting before attempt {}", numBlobsToDelete, interruptBeforeAttempt);
        ThrottlingDeleteHandler handler = new ThrottlingDeleteHandler(Integer.MAX_VALUE, attempt -> {
            if (attempt == interruptBeforeAttempt) {
                clientThread.interrupt();
            }
        });
        httpServer.createContext("/", handler);

        try {
            IOException exception = assertThrows(
                IOException.class,
                () -> blobContainer.deleteBlobsIgnoringIfNotExists(
                    randomFrom(operationPurposesThatRetryOnDelete()),
                    blobsToDelete.iterator()
                )
            );
            assertThat(exception.getCause(), instanceOf(SdkException.class));
            assertThat(handler.numberOfDeleteAttempts.get(), equalTo(interruptBeforeAttempt + 1));
            assertThat(handler.numberOfSuccessfulDeletes.get(), equalTo(0));
        } finally {
            // interrupt should be preserved, clear it to prevent it leaking between tests
            assertTrue(Thread.interrupted());
        }
    }

    public void testNonSnapshotDeletesAreNotRetried() {
        final BlobContainer blobContainer = blobContainerBuilder()
            // disable AWS-client retries
            .maxRetries(0)
            .disableChunkedEncoding(true)
            .build();

        int numBlobsToDelete = randomIntBetween(500, 3000);
        List<String> blobsToDelete = new ArrayList<>();
        for (int i = 0; i < numBlobsToDelete; i++) {
            blobsToDelete.add(randomIdentifier());
        }
        ThrottlingDeleteHandler handler = new ThrottlingDeleteHandler(Integer.MAX_VALUE, attempt -> {});
        httpServer.createContext("/", handler);
        IOException exception = assertThrows(
            IOException.class,
            () -> blobContainer.deleteBlobsIgnoringIfNotExists(
                randomValueOtherThanMany(
                    op -> operationPurposesThatRetryOnDelete().contains(op),
                    () -> randomFrom(OperationPurpose.values())
                ),
                blobsToDelete.iterator()
            )
        );
        assertEquals(
            ThrottlingDeleteHandler.THROTTLING_ERROR_CODE,
            asInstanceOf(S3Exception.class, exception.getCause()).awsErrorDetails().errorCode()
        );
        assertThat(handler.numberOfDeleteAttempts.get(), equalTo(expectedNumberOfBatches(numBlobsToDelete)));
        assertThat(handler.numberOfSuccessfulDeletes.get(), equalTo(0));
    }

    public void testNonThrottlingErrorsAreNotRetried() {
        final BlobContainer blobContainer = blobContainerBuilder()
            // disable AWS-client retries
            .maxRetries(0)
            .disableChunkedEncoding(true)
            .build();

        int numBlobsToDelete = randomIntBetween(500, 3000);
        List<String> blobsToDelete = new ArrayList<>();
        for (int i = 0; i < numBlobsToDelete; i++) {
            blobsToDelete.add(randomIdentifier());
        }
        ThrottlingDeleteHandler handler = new ThrottlingDeleteHandler(Integer.MAX_VALUE, attempt -> {}, "NotThrottling");
        httpServer.createContext("/", handler);
        assertThrows(
            IOException.class,
            () -> blobContainer.deleteBlobsIgnoringIfNotExists(randomFrom(operationPurposesThatRetryOnDelete()), blobsToDelete.iterator())
        );
        assertThat(handler.numberOfDeleteAttempts.get(), equalTo(expectedNumberOfBatches(numBlobsToDelete)));
        assertThat(handler.numberOfSuccessfulDeletes.get(), equalTo(0));
    }

    private int expectedNumberOfBatches(int blobsToDelete) {
        return (blobsToDelete / 1_000) + (blobsToDelete % 1_000 == 0 ? 0 : 1);
    }

    @SuppressForbidden(reason = "use a http server")
    private static class ThrottlingDeleteHandler extends S3HttpHandler {

        private static final String THROTTLING_ERROR_CODE = "SlowDown";

        private final AtomicInteger throttleTimesBeforeSuccess;
        private final AtomicInteger numberOfDeleteAttempts;
        private final AtomicInteger numberOfSuccessfulDeletes;
        private final IntConsumer onAttemptCallback;
        private final String errorCode;

        ThrottlingDeleteHandler(int throttleTimesBeforeSuccess, IntConsumer onAttemptCallback) {
            this(throttleTimesBeforeSuccess, onAttemptCallback, THROTTLING_ERROR_CODE);
        }

        ThrottlingDeleteHandler(int throttleTimesBeforeSuccess, IntConsumer onAttemptCallback, String errorCode) {
            super("bucket");
            this.numberOfDeleteAttempts = new AtomicInteger();
            this.numberOfSuccessfulDeletes = new AtomicInteger();
            this.throttleTimesBeforeSuccess = new AtomicInteger(throttleTimesBeforeSuccess);
            this.onAttemptCallback = onAttemptCallback;
            this.errorCode = errorCode;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (isMultiDeleteRequest(exchange)) {
                onAttemptCallback.accept(numberOfDeleteAttempts.get());
                numberOfDeleteAttempts.incrementAndGet();
                if (throttleTimesBeforeSuccess.getAndDecrement() > 0) {
                    final byte[] responseBytes = Strings.format("""
                        <?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                          <Code>%s</Code>
                          <Message>This is a throttling message</Message>
                          <Resource>/bucket/</Resource>
                          <RequestId>4442587FB7D0A2F9</RequestId>
                        </Error>""", errorCode).getBytes(StandardCharsets.UTF_8);

                    exchange.sendResponseHeaders(HttpStatus.SC_SERVICE_UNAVAILABLE, responseBytes.length);
                    exchange.getResponseBody().write(responseBytes);
                    exchange.close();
                } else {
                    numberOfSuccessfulDeletes.incrementAndGet();
                    super.handle(exchange);
                }
            } else {
                super.handle(exchange);
            }
        }
    }

    private Set<OperationPurpose> operationPurposesThatRetryOnDelete() {
        return Set.of(OperationPurpose.SNAPSHOT_DATA, OperationPurpose.SNAPSHOT_METADATA);
    }

    public void testGetRegisterRetries() {
        final var maxRetries = between(0, 3);
        final BlobContainer blobContainer = blobContainerBuilder().maxRetries(maxRetries).build();

        interface FailingHandlerFactory {
            void addHandler(String blobName, Integer... responseCodes);
        }

        final var requestCounter = new AtomicInteger();
        final FailingHandlerFactory countingFailingHandlerFactory = (blobName, responseCodes) -> httpServer.createContext(
            downloadStorageEndpoint(blobContainer, blobName),
            exchange -> {
                requestCounter.incrementAndGet();
                try (exchange) {
                    exchange.sendResponseHeaders(randomFrom(responseCodes), -1);
                }
            }
        );

        countingFailingHandlerFactory.addHandler("test_register_no_internal_retries", HttpStatus.SC_UNPROCESSABLE_ENTITY);
        countingFailingHandlerFactory.addHandler(
            "test_register_internal_retries",
            HttpStatus.SC_INTERNAL_SERVER_ERROR,
            HttpStatus.SC_SERVICE_UNAVAILABLE
        );
        countingFailingHandlerFactory.addHandler("test_register_not_found", HttpStatus.SC_NOT_FOUND);

        {
            final var exceptionWithInternalRetries = safeAwaitFailure(
                OptionalBytesReference.class,
                l -> blobContainer.getRegister(randomRetryingPurpose(), "test_register_internal_retries", l)
            );

            assertEquals(Integer.valueOf(maxRetries + 1), asInstanceOf(S3Exception.class, exceptionWithInternalRetries).numAttempts());
            assertEquals((maxRetries + 1) * (maxRetries + 1), requestCounter.get());
            assertEquals(
                maxRetries * 2 /* each failure yields a suppressed S3Exception and a suppressed SdkClientException  */,
                exceptionWithInternalRetries.getSuppressed().length
            );
        }

        {
            requestCounter.set(0);
            final var exceptionWithoutInternalRetries = safeAwaitFailure(
                OptionalBytesReference.class,
                l -> blobContainer.getRegister(randomRetryingPurpose(), "test_register_no_internal_retries", l)
            );
            assertEquals(Integer.valueOf(1), asInstanceOf(S3Exception.class, exceptionWithoutInternalRetries).numAttempts());
            assertEquals(maxRetries + 1, requestCounter.get());
            assertEquals(maxRetries, exceptionWithoutInternalRetries.getSuppressed().length);
        }

        {
            requestCounter.set(0);
            final var repoAnalysisException = safeAwaitFailure(
                OptionalBytesReference.class,
                l -> blobContainer.getRegister(OperationPurpose.REPOSITORY_ANALYSIS, "test_register_no_internal_retries", l)
            );
            assertEquals(Integer.valueOf(1), asInstanceOf(S3Exception.class, repoAnalysisException).numAttempts());
            assertEquals(1, requestCounter.get());
            assertEquals(0, repoAnalysisException.getSuppressed().length);
        }

        {
            requestCounter.set(0);
            final OptionalBytesReference expectEmpty = safeAwait(
                l -> blobContainer.getRegister(randomPurpose(), "test_register_not_found", l)
            );
            assertEquals(OptionalBytesReference.EMPTY, expectEmpty);
            assertEquals(1, requestCounter.get());
        }
    }

    public void testSuppressedDeletionErrorsAreCapped() {
        final TimeValue readTimeout = TimeValue.timeValueMillis(randomIntBetween(100, 500));
        int maxBulkDeleteSize = randomIntBetween(1, 10);
        final BlobContainer blobContainer = blobContainerBuilder().maxRetries(1)
            .readTimeout(readTimeout)
            .disableChunkedEncoding(true)
            .maxBulkDeletes(maxBulkDeleteSize)
            .build();
        httpServer.createContext("/", exchange -> {
            if (isMultiDeleteRequest(exchange)) {
                exchange.sendResponseHeaders(
                    randomFrom(
                        HttpStatus.SC_INTERNAL_SERVER_ERROR,
                        HttpStatus.SC_BAD_GATEWAY,
                        HttpStatus.SC_SERVICE_UNAVAILABLE,
                        HttpStatus.SC_GATEWAY_TIMEOUT,
                        HttpStatus.SC_NOT_FOUND,
                        HttpStatus.SC_UNAUTHORIZED
                    ),
                    -1
                );
                exchange.close();
            } else {
                fail("expected only deletions");
            }
        });
        var maxNoOfDeletions = 2 * S3BlobStore.MAX_DELETE_EXCEPTIONS;
        var blobs = randomList(1, maxNoOfDeletions * maxBulkDeleteSize, ESTestCase::randomIdentifier);
        var exception = expectThrows(
            IOException.class,
            "deletion should not succeed",
            () -> blobContainer.deleteBlobsIgnoringIfNotExists(randomPurpose(), blobs.iterator())
        );

        var sdkGeneratedExceptions = 0;
        final var innerExceptions = exception.getCause().getSuppressed();
        for (final var innerException : innerExceptions) {
            if (innerException instanceof SdkClientException && innerException.getMessage().startsWith("Request attempt ")) {
                sdkGeneratedExceptions += 1;
            }
        }
        assertThat(innerExceptions.length - sdkGeneratedExceptions, lessThan(S3BlobStore.MAX_DELETE_EXCEPTIONS));
    }

    public void testTrimmedLogAndCappedSuppressedErrorOnMultiObjectDeletionException() {
        final TimeValue readTimeout = TimeValue.timeValueMillis(randomIntBetween(100, 500));
        int maxBulkDeleteSize = randomIntBetween(10, 30);
        final BlobContainer blobContainer = blobContainerBuilder().maxRetries(1)
            .readTimeout(readTimeout)
            .disableChunkedEncoding(true)
            .maxBulkDeletes(maxBulkDeleteSize)
            .build();

        final Pattern pattern = Pattern.compile("<Key>(.+?)</Key>");
        httpServer.createContext("/", exchange -> {
            if (isMultiDeleteRequest(exchange)) {
                final String requestBody = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));
                final var matcher = pattern.matcher(requestBody);
                final StringBuilder deletes = new StringBuilder();
                deletes.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                deletes.append("<DeleteResult>");
                while (matcher.find()) {
                    final String key = matcher.group(1);
                    deletes.append("<Error>");
                    deletes.append("<Code>").append(randomAlphaOfLength(10)).append("</Code>");
                    deletes.append("<Key>").append(key).append("</Key>");
                    deletes.append("<Message>").append(randomAlphaOfLength(40)).append("</Message>");
                    deletes.append("</Error>");
                }
                deletes.append("</DeleteResult>");

                byte[] response = deletes.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
            } else {
                fail("expected only deletions");
            }
        });
        var blobs = randomList(maxBulkDeleteSize, maxBulkDeleteSize, ESTestCase::randomIdentifier);
        try (var mockLog = MockLog.capture(S3BlobStore.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "deletion log",
                    S3BlobStore.class.getCanonicalName(),
                    Level.WARN,
                    blobs.size() > S3BlobStore.MAX_DELETE_EXCEPTIONS
                        ? "Failed to delete some blobs [*... (* in total, * omitted)"
                        : "Failed to delete some blobs [*]"
                )
            );
            var exception = expectThrows(
                IOException.class,
                "deletion should not succeed",
                () -> blobContainer.deleteBlobsIgnoringIfNotExists(randomPurpose(), blobs.iterator())
            );
            assertThat(exception.getCause().getSuppressed().length, lessThan(S3BlobStore.MAX_DELETE_EXCEPTIONS));
            mockLog.awaitAllExpectationsMatched();
        }
    }

    public void testRetryOn403InStateless() {
        final var maxRetries = between(1, 5);
        final var denyAccessAfterAttempt = between(1, 5);
        logger.info("--> maxRetries = {}, denyAccessAfterAttempt = {}", maxRetries, denyAccessAfterAttempt);
        final var blobContainerPath = BlobPath.EMPTY.add(getTestName());
        final var statefulBlobContainer = createBlobContainer(maxRetries, null, null, null, null, null, blobContainerPath);
        final var requestCount = new AtomicInteger();
        final var invalidAccessKeyIdResponseCount = new AtomicInteger();
        final var accessDeniedResponseCount = new AtomicInteger();
        httpServer.createContext(downloadStorageEndpoint(statefulBlobContainer, "always_forbidden"), exchange -> {
            try (exchange) {
                final var currentAttempt = requestCount.incrementAndGet();
                final String errorCode;
                if (currentAttempt <= denyAccessAfterAttempt) {
                    invalidAccessKeyIdResponseCount.incrementAndGet();
                    errorCode = "InvalidAccessKeyId";
                } else {
                    accessDeniedResponseCount.incrementAndGet();
                    errorCode = "AccessDenied";
                }
                final var responseBody = Strings.format(
                    """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                          <Code>%s</Code>
                          <Message>%s</Message>
                          <RequestId>%s</RequestId>
                        </Error>""",
                    errorCode,
                    "FakeResponse " + errorCode + " " + currentAttempt + " " + randomAlphaOfLengthBetween(10, 20),
                    randomUUID()
                ).getBytes(StandardCharsets.UTF_8);

                exchange.sendResponseHeaders(RestStatus.FORBIDDEN.getStatus(), responseBody.length);
                exchange.getResponseBody().write(responseBody);
            }
        });

        final var purpose = randomPurpose();

        assertThat(
            ExceptionsHelper.unwrap(
                expectThrows(
                    IOException.class,
                    () -> statefulBlobContainer.writeBlobAtomic(purpose, "always_forbidden", BytesArray.EMPTY, false)
                ),
                S3Exception.class
            ).getMessage(),
            containsString("Status Code: 403")
        );
        // PutObject is not retried by default
        assertEquals(0, accessDeniedResponseCount.getAndSet(0));
        assertEquals(1, invalidAccessKeyIdResponseCount.getAndSet(0));
        assertEquals(1, requestCount.getAndSet(0));
        service.close();

        service = new S3Service(
            Mockito.mock(Environment.class),
            ClusterServiceUtils.createClusterService(
                new DeterministicTaskQueue().getThreadPool(),
                Settings.builder().put(STATELESS_ENABLED_SETTING_NAME, "true").build()
            ),
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            Mockito.mock(ResourceWatcherService.class),
            () -> null
        );
        service.start();
        recordingMeterRegistry = new RecordingMeterRegistry();
        final var statelessBlobContainer = createBlobContainer(maxRetries, null, null, null, null, null, blobContainerPath);

        assertThat(
            ExceptionsHelper.unwrap(
                expectThrows(
                    IOException.class,
                    () -> statelessBlobContainer.writeBlobAtomic(purpose, "always_forbidden", BytesArray.EMPTY, false)
                ),
                S3Exception.class
            ).getMessage(),
            containsString("Status Code: 403")
        );
        assertEquals(requestCount.get(), accessDeniedResponseCount.get() + invalidAccessKeyIdResponseCount.getAndSet(0));
        assertEquals(Math.min(maxRetries, denyAccessAfterAttempt) + 1, requestCount.get());
        assertEquals(denyAccessAfterAttempt <= maxRetries ? 1 : 0, accessDeniedResponseCount.get());
    }

    private static String getBase16MD5Digest(BytesReference bytesReference) {
        return MessageDigests.toHexString(MessageDigests.digest(bytesReference, MessageDigests.md5()));
    }

    public void testGetBase16MD5Digest() {
        // from Wikipedia, see also org.elasticsearch.common.hash.MessageDigestsTests.testMd5
        assertBase16MD5Digest("", "d41d8cd98f00b204e9800998ecf8427e");
        assertBase16MD5Digest("The quick brown fox jumps over the lazy dog", "9e107d9d372bb6826bd81d3542a419d6");
        assertBase16MD5Digest("The quick brown fox jumps over the lazy dog.", "e4d909c290d0fb1ca068ffaddf22cbd0");
    }

    private static void assertBase16MD5Digest(String input, String expectedDigestString) {
        assertEquals(expectedDigestString, getBase16MD5Digest(new BytesArray(input)));
    }

    @Override
    protected Matcher<Integer> getMaxRetriesMatcher(int maxRetries) {
        // some attempts make meaningful progress and do not count towards the max retry limit
        return allOf(greaterThanOrEqualTo(maxRetries), lessThanOrEqualTo(S3RetryingInputStream.MAX_SUPPRESSED_EXCEPTIONS));
    }

    @Override
    protected OperationPurpose randomRetryingPurpose() {
        return randomValueOtherThan(OperationPurpose.REPOSITORY_ANALYSIS, BlobStoreTestUtil::randomPurpose);
    }

    @Override
    protected OperationPurpose randomFiniteRetryingPurpose() {
        return randomValueOtherThanMany(
            purpose -> purpose == OperationPurpose.REPOSITORY_ANALYSIS || purpose == OperationPurpose.INDICES,
            BlobStoreTestUtil::randomPurpose
        );
    }

    private void assertMetricsForOpeningStream() {
        final long numberOfOperations = getOperationMeasurements();
        // S3 client sdk internally also retries within the configured maxRetries for retryable errors.
        // The retries in S3RetryingInputStream are triggered when the client internal retries are unsuccessful
        if (numberOfOperations > 1) {
            // For opening the stream, there should be exactly one pair of started and completed records.
            // There should be one histogram record, the number of retries must be greater than 0
            final Map<String, Object> attributes = metricAttributes("open");
            assertThat(getRetryStartedMeasurements(), contains(new Measurement(1L, attributes, false)));
            assertThat(getRetryCompletedMeasurements(), contains(new Measurement(1L, attributes, false)));
            final List<Measurement> retryHistogramMeasurements = getRetryHistogramMeasurements();
            assertThat(retryHistogramMeasurements, hasSize(1));
            assertThat(retryHistogramMeasurements.get(0).getLong(), equalTo(numberOfOperations - 1));
            assertThat(retryHistogramMeasurements.get(0).attributes(), equalTo(attributes));
        } else {
            assertThat(getRetryStartedMeasurements(), empty());
            assertThat(getRetryCompletedMeasurements(), empty());
            assertThat(getRetryHistogramMeasurements(), empty());
        }
    }

    private void assertMetricsForReadingStream() {
        // For reading the stream, there could be multiple pairs of started and completed records.
        // It is important that they always come in pairs and the number of pairs match the number
        // of histogram records.
        final Map<String, Object> attributes = metricAttributes("read");
        final List<Measurement> retryHistogramMeasurements = getRetryHistogramMeasurements();
        final int numberOfReads = retryHistogramMeasurements.size();
        retryHistogramMeasurements.forEach(measurement -> {
            assertThat(measurement.getLong(), greaterThan(0L));
            assertThat(measurement.attributes(), equalTo(attributes));
        });

        final List<Measurement> retryStartedMeasurements = getRetryStartedMeasurements();
        assertThat(retryStartedMeasurements, hasSize(1));
        assertThat(retryStartedMeasurements.get(0).getLong(), equalTo((long) numberOfReads));
        assertThat(retryStartedMeasurements.get(0).attributes(), equalTo(attributes));
        assertThat(retryStartedMeasurements, equalTo(getRetryCompletedMeasurements()));
    }

    private long getOperationMeasurements() {
        final List<Measurement> operationMeasurements = Measurement.combine(
            recordingMeterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, RepositoriesMetrics.METRIC_OPERATIONS_TOTAL)
        );
        assertThat(operationMeasurements, hasSize(1));
        return operationMeasurements.get(0).getLong();
    }

    private List<Measurement> getRetryStartedMeasurements() {
        return Measurement.combine(
            recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, S3RepositoriesMetrics.METRIC_RETRY_EVENT_TOTAL)
        );
    }

    private List<Measurement> getRetryCompletedMeasurements() {
        return Measurement.combine(
            recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, S3RepositoriesMetrics.METRIC_RETRY_SUCCESS_TOTAL)
        );
    }

    private List<Measurement> getRetryHistogramMeasurements() {
        return recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, S3RepositoriesMetrics.METRIC_RETRY_ATTEMPTS_HISTOGRAM);
    }

    private Map<String, Object> metricAttributes(String action) {
        return Map.of("repo_type", "s3", "repo_name", "repository", "operation", "GetObject", "purpose", "Indices", "action", action);
    }

    private static boolean isMultiDeleteRequest(HttpExchange exchange) {
        return new S3HttpHandler("bucket").parseRequest(exchange).isMultiObjectDeleteRequest();
    }

    /**
     * Asserts that an InputStream is fully consumed, or aborted, when it is closed
     */
    private static class AssertingInputStream extends FilterInputStream {

        private final String blobName;
        private final boolean range;
        private final long position;
        private final long length;

        AssertingInputStream(InputStream in, String blobName) {
            super(in);
            this.blobName = blobName;
            this.position = 0L;
            this.length = Long.MAX_VALUE;
            this.range = false;
        }

        AssertingInputStream(InputStream in, String blobName, long position, long length) {
            super(in);
            assert position >= 0L;
            assert length >= 0;
            this.blobName = blobName;
            this.position = position;
            this.length = length;
            this.range = true;
        }

        @Override
        public String toString() {
            String description = "[blobName='" + blobName + "', range=" + range;
            if (range) {
                description += ", position=" + position;
                description += ", length=" + length;
            }
            description += ']';
            return description;
        }

        @Override
        public void close() throws IOException {
            super.close();
            if (in instanceof final S3RetryingInputStream s3Stream) {
                assertTrue(
                    "Stream "
                        + toString()
                        + " should have reached EOF or should have been aborted but got [eof="
                        + s3Stream.isEof()
                        + ", aborted="
                        + s3Stream.isAborted()
                        + ']',
                    s3Stream.isEof() || s3Stream.isAborted()
                );
            } else {
                assertThat(in, instanceOf(ByteArrayInputStream.class));
                assertThat(((ByteArrayInputStream) in).available(), equalTo(0));
            }
        }
    }
}
