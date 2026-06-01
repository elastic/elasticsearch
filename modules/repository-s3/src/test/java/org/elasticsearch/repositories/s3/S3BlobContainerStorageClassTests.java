/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.elasticsearch.repositories.s3.S3ClientSettings.ACCESS_KEY_SETTING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.SECRET_KEY_SETTING;
import static org.elasticsearch.repositories.s3.S3ClientSettingsTests.DEFAULT_REGION_UNAVAILABLE;

@SuppressForbidden(reason = "use a http server")
public class S3BlobContainerStorageClassTests extends ESTestCase {

    private static final String BUCKET = "bucket";

    /**
     * Minimum allowed multipart upload size in the S3 client. Any multipart upload via
     * {@link S3BlobContainer#writeBlob} must be at least this large.
     */
    private static final ByteSizeValue MULTIPART_BUFFER_SIZE = S3Repository.MIN_PART_SIZE_USING_MULTIPART;

    /**
     * A buffer size small enough to trigger the multipart path inside {@link S3BlobContainer#writeMetadataBlob},
     * which uses {@link org.elasticsearch.repositories.blobstore.ChunkedBlobOutputStream} directly and does not
     * enforce the 5 MB per-part minimum that {@link S3BlobContainer#writeBlob} does.
     */
    private static final ByteSizeValue SMALL_BUFFER_SIZE = ByteSizeValue.ofBytes(100);

    private static HttpServer httpServer;
    private static S3Service service;

    @BeforeClass
    public static void startServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        service = new S3Service(
            Mockito.mock(Environment.class),
            ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool()),
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            Mockito.mock(ResourceWatcherService.class),
            DEFAULT_REGION_UNAVAILABLE
        );
        service.start();
    }

    @AfterClass
    public static void stopServer() throws Exception {
        httpServer.stop(0);
        IOUtils.close(service);
        httpServer = null;
        service = null;
    }

    private S3HttpHandler s3HttpHandler;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        s3HttpHandler = new S3HttpHandler(BUCKET, S3ConsistencyModel.randomConsistencyModel());
        httpServer.createContext("/", s3HttpHandler);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        httpServer.removeContext("/");
        super.tearDown();
    }

    private S3BlobContainer buildContainer(@Nullable String dataStorageClass, @Nullable String metadataStorageClass) {
        return buildContainer(dataStorageClass, metadataStorageClass, null, SMALL_BUFFER_SIZE);
    }

    private S3BlobContainer buildContainer(
        @Nullable String dataStorageClass,
        @Nullable String metadataStorageClass,
        @Nullable String fallbackStorageClass
    ) {
        return buildContainer(dataStorageClass, metadataStorageClass, fallbackStorageClass, SMALL_BUFFER_SIZE);
    }

    private S3BlobContainer buildContainer(
        @Nullable String dataStorageClass,
        @Nullable String metadataStorageClass,
        @Nullable String fallbackStorageClass,
        ByteSizeValue bufferSize
    ) {
        final String clientName = randomIdentifier();
        final Settings.Builder clientSettings = Settings.builder();

        final InetSocketAddress address = httpServer.getAddress();
        final String host = InetAddresses.toUriString(address.getAddress());
        clientSettings.put(
            ENDPOINT_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
            "http://" + host + ":" + address.getPort()
        );
        clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), 0);

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCESS_KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "test_access_key");
        secureSettings.setString(SECRET_KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "test_secret_key");
        clientSettings.setSecureSettings(secureSettings);
        service.refreshAndClearCache(S3ClientSettings.load(clientSettings.build()));

        final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
            "repository",
            S3Repository.TYPE,
            Settings.builder()
                .put(S3Repository.CLIENT_NAME.getKey(), clientName)
                .put(S3Repository.GET_REGISTER_RETRY_DELAY.getKey(), TimeValue.ZERO)
                .build()
        );

        final S3BlobStore blobStore = new S3BlobStore(
            ProjectId.DEFAULT,
            service,
            BUCKET,
            false,
            bufferSize,
            S3Repository.CANNED_ACL_SETTING.getDefault(Settings.EMPTY),
            fallbackStorageClass != null ? fallbackStorageClass : S3Repository.FALLBACK_STORAGE_CLASS_SETTING.getDefault(Settings.EMPTY),
            dataStorageClass != null ? dataStorageClass : S3Repository.DATA_STORAGE_CLASS_SETTING.getDefault(Settings.EMPTY),
            metadataStorageClass != null ? metadataStorageClass : S3Repository.METADATA_STORAGE_CLASS_SETTING.getDefault(Settings.EMPTY),
            true,
            repositoryMetadata,
            BigArrays.NON_RECYCLING_INSTANCE,
            new DeterministicTaskQueue().getThreadPool(),
            new S3RepositoriesMetrics(RepositoriesMetrics.NOOP),
            BackoffPolicy.noBackoff()
        );

        return new S3BlobContainer(BlobPath.EMPTY, blobStore);
    }

    private String blobKey(String blobName) {
        return "/" + BUCKET + "/" + blobName;
    }

    public void testDataStorageClassSentOnSingleBlobUpload() throws IOException {
        final S3BlobContainer container = buildContainer("standard_ia", null);
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertEquals("STANDARD_IA", s3HttpHandler.blobs().get(blobKey(blobName)).storageClass());
    }

    public void testMetadataStorageClassSentOnSingleBlobUpload() throws IOException {
        final S3BlobContainer container = buildContainer(null, "intelligent_tiering");
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertEquals("INTELLIGENT_TIERING", s3HttpHandler.blobs().get(blobKey(blobName)).storageClass());
    }

    public void testFallbackStorageClassUsedForNonSnapshotPurpose() throws IOException {
        final S3BlobContainer container = buildContainer("standard_ia", "intelligent_tiering", "reduced_redundancy");
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.CLUSTER_STATE, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertEquals("REDUCED_REDUNDANCY", s3HttpHandler.blobs().get(blobKey(blobName)).storageClass());
    }

    public void testFallbackStorageClassUsedWhenDataClassNotSet() throws IOException {
        final S3BlobContainer container = buildContainer(null, null, "reduced_redundancy");
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertEquals("REDUCED_REDUNDANCY", s3HttpHandler.blobs().get(blobKey(blobName)).storageClass());
    }

    public void testFallbackStorageClassUsedWhenMetadataClassNotSet() throws IOException {
        final S3BlobContainer container = buildContainer(null, null, "reduced_redundancy");
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertEquals("REDUCED_REDUNDANCY", s3HttpHandler.blobs().get(blobKey(blobName)).storageClass());
    }

    public void testDefaultStorageClassUsedWhenNothingConfigured() throws IOException {
        final S3BlobContainer container = buildContainer(null, null);
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertEquals("STANDARD", s3HttpHandler.blobs().get(blobKey(blobName)).storageClass());
    }

    public void testDataStorageClassSentOnMultipartUpload() throws IOException {
        final S3BlobContainer container = buildContainer("standard_ia", null, null, MULTIPART_BUFFER_SIZE);
        final String blobName = randomIdentifier();
        // Write more than MULTIPART_BUFFER_SIZE to trigger multipart; writeBlob also requires >= MIN_PART_SIZE_USING_MULTIPART
        final byte[] data = randomByteArrayOfLength((int) MULTIPART_BUFFER_SIZE.getBytes() + 1);

        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new BytesArray(data), false);

        assertEquals("STANDARD_IA", s3HttpHandler.blobs().get(blobKey(blobName)).storageClass());
    }

    public void testMetadataStorageClassSentOnMultipartUpload() throws IOException {
        final S3BlobContainer container = buildContainer(null, "intelligent_tiering", null, MULTIPART_BUFFER_SIZE);
        final String blobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength((int) MULTIPART_BUFFER_SIZE.getBytes() + 1);

        container.writeBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, new BytesArray(data), false);

        assertEquals("INTELLIGENT_TIERING", s3HttpHandler.blobs().get(blobKey(blobName)).storageClass());
    }

    public void testMetadataStorageClassSentOnWriteMetadataBlob() throws IOException {
        final S3BlobContainer container = buildContainer(null, "intelligent_tiering");
        final String blobName = randomIdentifier();
        // Write more than SMALL_BUFFER_SIZE to trigger the multipart path inside writeMetadataBlob
        final byte[] data = randomByteArrayOfLength((int) SMALL_BUFFER_SIZE.getBytes() + 1);

        container.writeMetadataBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, false, false, out -> out.write(data));

        assertEquals("INTELLIGENT_TIERING", s3HttpHandler.blobs().get(blobKey(blobName)).storageClass());
    }

    public void testDataStorageClassSentOnCopyBlob() throws IOException {
        final S3BlobContainer container = buildContainer("standard_ia", null);
        final String sourceBlobName = randomIdentifier();
        final String destBlobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(64);

        // Write source with a non-snapshot purpose so it uses the fallback (STANDARD)
        container.writeBlob(OperationPurpose.CLUSTER_STATE, sourceBlobName, new BytesArray(data), false);
        assertEquals("STANDARD", s3HttpHandler.blobs().get(blobKey(sourceBlobName)).storageClass());

        // Copy with SNAPSHOT_DATA purpose — destination should receive the configured data storage class
        container.copyBlob(OperationPurpose.SNAPSHOT_DATA, container, sourceBlobName, destBlobName, data.length);

        assertEquals("STANDARD_IA", s3HttpHandler.blobs().get(blobKey(destBlobName)).storageClass());
    }

    public void testMetadataStorageClassSentOnCopyBlob() throws IOException {
        final S3BlobContainer container = buildContainer(null, "intelligent_tiering");
        final String sourceBlobName = randomIdentifier();
        final String destBlobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(64);

        // Write source with a non-snapshot purpose so it uses the fallback (STANDARD)
        container.writeBlob(OperationPurpose.CLUSTER_STATE, sourceBlobName, new BytesArray(data), false);
        assertEquals("STANDARD", s3HttpHandler.blobs().get(blobKey(sourceBlobName)).storageClass());

        // Copy with SNAPSHOT_METADATA purpose — destination should receive the configured metadata storage class
        container.copyBlob(OperationPurpose.SNAPSHOT_METADATA, container, sourceBlobName, destBlobName, data.length);

        assertEquals("INTELLIGENT_TIERING", s3HttpHandler.blobs().get(blobKey(destBlobName)).storageClass());
    }
}
