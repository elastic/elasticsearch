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
import fixture.azure.MockAzureBlobStore;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.CONTAINER_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.COPY_POLL_INTERVAL;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.MAX_SINGLE_PART_UPLOAD_SIZE_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ENDPOINT_SUFFIX_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.KEY_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.TIMEOUT_SETTING;

/**
 * Verifies that a blob written through the concurrent multipart path is stored with exactly the bytes its
 * {@link org.elasticsearch.common.blobstore.BlobContainer.BlobMultiPartInputStreamProvider} supplied.
 *
 * <p>The multipart write path was previously exercised only by
 * {@code AzureBlobContainerAccessTierTests#testDataAccessTierSentOnWriteBlobAtomicMultipartUpload}, which asserts the
 * access tier and never reads the blob back. That test also stages parts serially ({@code Runnable::run}) and supplies
 * them from a flat {@code ByteArrayInputStream} slice, i.e. a provider that is correct by construction and so cannot
 * expose a defect in a composed one.
 *
 * <p>Real callers compose blobs from many sources: the stateless commit uploader builds each part by concatenating a
 * header region with a series of variable-length file slices, none of which align with the part boundaries. These tests
 * therefore use a composed provider and a genuinely concurrent executor.
 */
@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerMultipartContentTests extends ESTestCase {

    private static final String ACCOUNT = "account";
    private static final String CONTAINER = "container";
    private static final long BLOCK_SIZE = ByteSizeUnit.MB.toBytes(1);

    private static HttpServer httpServer;
    private static ThreadPool threadPool;
    private static AzureClientProvider clientProvider;
    private static ClusterService clusterService;

    @BeforeClass
    public static void startServer() throws IOException {
        threadPool = new TestThreadPool(
            AzureBlobContainerMultipartContentTests.class.getName(),
            AzureRepositoryPlugin.executorBuilder(Settings.EMPTY),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        clientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY);
        clientProvider.start();
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @AfterClass
    public static void stopServer() throws Exception {
        clientProvider.close();
        httpServer.stop(0);
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    private AzureHttpHandler azureHttpHandler;

    @Before
    public void configureAzureHandler() {
        azureHttpHandler = new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE);
        httpServer.createContext("/", azureHttpHandler);
    }

    @After
    public void removeAzureHandler() {
        httpServer.removeContext("/");
    }

    /**
     * A flat source, staged concurrently. Establishes the baseline: whatever else varies, the stored bytes must equal
     * the supplied bytes.
     */
    public void testMultipartUploadStoresExactlyTheSuppliedBytes() throws Exception {
        final AzureBlobContainer container = buildContainer();
        final String blobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(Math.toIntExact(BLOCK_SIZE * randomIntBetween(3, 5) + randomIntBetween(1, 4096)));

        container.writeBlobAtomic(
            OperationPurpose.SNAPSHOT_DATA,
            blobName,
            data.length,
            (offset, length) -> new ByteArrayInputStream(data, Math.toIntExact(offset), Math.toIntExact(length)),
            false,
            threadPool.generic()
        );

        assertArrayEquals(data, storedBytes(blobName));
    }

    /**
     * A composed source, staged concurrently. The provider concatenates a leading header region with many
     * variable-length segments, none aligned to the part boundaries, so serving a part requires stitching across
     * several segments and starting mid-segment. This is the shape the stateless commit uploader produces, and the
     * shape that a flat {@code ByteArrayInputStream} provider cannot represent.
     */
    public void testMultipartUploadStoresExactlyTheSuppliedBytesWithComposedProvider() throws Exception {
        final AzureBlobContainer container = buildContainer();
        final String blobName = randomIdentifier();

        // A deliberately irregular layout: a header, then segments whose lengths do not divide the block size.
        final var segments = new ArrayList<byte[]>();
        segments.add(randomByteArrayOfLength(randomIntBetween(20_000, 40_000))); // "header"
        long total = segments.getFirst().length;
        while (total < BLOCK_SIZE * 3) {
            byte[] segment = randomByteArrayOfLength(randomIntBetween(50_000, 400_000));
            segments.add(segment);
            total += segment.length;
        }
        final byte[] expected = concat(segments);

        container.writeBlobAtomic(
            OperationPurpose.SNAPSHOT_DATA,
            blobName,
            expected.length,
            (offset, length) -> slicedOver(segments, offset, length),
            false,
            threadPool.generic()
        );

        final byte[] stored = storedBytes(blobName);
        // Check the leading region first: a displaced header is the failure mode this guards against, and comparing it
        // separately gives a far clearer message than a diff over several megabytes.
        final int headerLength = segments.getFirst().length;
        assertArrayEquals(
            "leading region of the stored blob differs from the supplied bytes",
            java.util.Arrays.copyOfRange(expected, 0, headerLength),
            java.util.Arrays.copyOfRange(stored, 0, Math.min(headerLength, stored.length))
        );
        assertArrayEquals(expected, stored);
    }

    /**
     * The same composed source, but every part is requested from the provider twice before the upload, leaving the
     * first set of streams closed short of EOF. A provider whose slices are not safely re-servable would produce
     * displaced content on the second pass.
     */
    public void testMultipartUploadStoresExactlyTheSuppliedBytesAfterProviderStreamsWereAbandoned() throws Exception {
        final AzureBlobContainer container = buildContainer();
        final String blobName = randomIdentifier();

        final var segments = new ArrayList<byte[]>();
        segments.add(randomByteArrayOfLength(randomIntBetween(20_000, 40_000)));
        long total = segments.getFirst().length;
        while (total < BLOCK_SIZE * 3) {
            byte[] segment = randomByteArrayOfLength(randomIntBetween(50_000, 400_000));
            segments.add(segment);
            total += segment.length;
        }
        final byte[] expected = concat(segments);

        container.writeBlobAtomic(OperationPurpose.SNAPSHOT_DATA, blobName, expected.length, (offset, length) -> {
            // Hand out a stream, read part of it, then drop it without reaching EOF, as a cancelled stage does.
            try (var abandoned = slicedOver(segments, offset, length)) {
                abandoned.readNBytes(Math.toIntExact(Math.max(1L, length / 3L)));
            }
            return slicedOver(segments, offset, length);
        }, false, threadPool.generic());

        assertArrayEquals(expected, storedBytes(blobName));
    }

    private byte[] storedBytes(String blobName) throws IOException {
        return BytesReference.toBytes(azureHttpHandler.getMockBlobStore().getBlob(blobName, null).getContents());
    }

    private static byte[] concat(List<byte[]> segments) {
        int total = segments.stream().mapToInt(s -> s.length).sum();
        var out = new byte[total];
        int at = 0;
        for (byte[] segment : segments) {
            System.arraycopy(segment, 0, out, at, segment.length);
            at += segment.length;
        }
        return out;
    }

    /**
     * Serves {@code [offset, offset + length)} of the concatenated segments by stitching the segments it spans,
     * starting mid-segment where required.
     *
     * <p>Uses the production {@link SlicedInputStream}, both because the multipart path requires the provided stream to
     * support mark/reset (asserted in {@code AzureBlobStore#stageBlock}) and because that is the composition machinery
     * real callers use. Each slice re-opens at the same size, as {@code SlicedInputStream#openSlice} requires.
     */
    private static InputStream slicedOver(List<byte[]> segments, long offset, long length) {
        record Slice(byte[] segment, int from, int take) {}

        final var slices = new ArrayList<Slice>();
        long remaining = length;
        long cursor = 0;
        for (byte[] segment : segments) {
            if (remaining <= 0) {
                break;
            }
            final long segmentEnd = cursor + segment.length;
            if (segmentEnd > offset) {
                final int from = Math.toIntExact(Math.max(0, offset - cursor));
                final int take = Math.toIntExact(Math.min(segment.length - from, remaining));
                slices.add(new Slice(segment, from, take));
                remaining -= take;
            }
            cursor = segmentEnd;
        }
        return new SlicedInputStream(slices.size()) {
            @Override
            protected InputStream openSlice(int slice) {
                final Slice s = slices.get(slice);
                return new ByteArrayInputStream(s.segment(), s.from(), s.take());
            }
        };
    }

    private AzureBlobContainer buildContainer() {
        final String clientName = randomIdentifier();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), ACCOUNT);
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(14).getBytes(UTF_8));
        secureSettings.setString(KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), key);

        final InetSocketAddress address = httpServer.getAddress();
        final String host = address.getAddress().isLoopbackAddress() ? "localhost" : InetAddresses.toUriString(address.getAddress());
        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=http://"
            + host
            + ":"
            + address.getPort()
            + "/"
            + ACCOUNT;

        final Settings clientSettings = Settings.builder()
            .put(ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint)
            .put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), 0)
            .put(TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), TimeValue.timeValueSeconds(60))
            .setSecureSettings(secureSettings)
            .build();

        final AzureStorageService service = new AzureStorageService(
            clientSettings,
            clientProvider,
            clusterService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        ) {
            @Override
            RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
                final RequestRetryOptions base = super.getRetryOptions(locationMode, azureStorageSettings);
                return new RequestRetryOptions(
                    RetryPolicyType.EXPONENTIAL,
                    base.getMaxTries(),
                    base.getTryTimeoutDuration(),
                    Duration.ofMillis(50),
                    Duration.ofMillis(100),
                    null
                );
            }

            @Override
            long getUploadBlockSize() {
                return BLOCK_SIZE;
            }
        };

        final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
            "repository",
            AzureRepository.TYPE,
            Settings.builder()
                .put(CONTAINER_SETTING.getKey(), CONTAINER)
                .put(ACCOUNT_SETTING.getKey(), clientName)
                .put(MAX_SINGLE_PART_UPLOAD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.MB))
                .put(COPY_POLL_INTERVAL.getKey(), TimeValue.timeValueMillis(100))
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
}
