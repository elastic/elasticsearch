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
import software.amazon.awssdk.services.s3.model.StorageClass;

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

    private S3BlobContainer buildContainer(
        @Nullable String dataStorageClass,
        @Nullable String metadataStorageClass,
        @Nullable String fallbackStorageClass
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
            S3Repository.MIN_PART_SIZE_USING_MULTIPART,
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

    private void assertStorageClass(String STANDARD_IA, String blobName) {
        assertEquals(STANDARD_IA, s3HttpHandler.blobs().get(blobKey(blobName)).storageClass());
    }

    private String getRandomAllowedStorageClassString() {
        return randomFrom(S3BlobStore.ALLOWED_STORAGE_CLASSES.stream().map(StorageClass::toString).toList());
    }

    public void testDataStorageClassSentOnSingleBlobUpload() throws IOException {
        String dataStorageClass = getRandomAllowedStorageClassString();
        final S3BlobContainer container = buildContainer(dataStorageClass, null, null);
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertStorageClass(dataStorageClass, blobName);
    }

    public void testMetadataStorageClassSentOnSingleBlobUpload() throws IOException {
        String metadataStorageClass = getRandomAllowedStorageClassString();
        final S3BlobContainer container = buildContainer(null, metadataStorageClass, null);
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertStorageClass(metadataStorageClass, blobName);
    }

    public void testFallbackStorageClassUsedForNonSnapshotPurpose() throws IOException {
        String fallbackStorageClass = getRandomAllowedStorageClassString();
        final S3BlobContainer container = buildContainer(
            getRandomAllowedStorageClassString(),
            getRandomAllowedStorageClassString(),
            fallbackStorageClass
        );

        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.CLUSTER_STATE, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertStorageClass(fallbackStorageClass, blobName);
    }

    public void testFallbackStorageClassUsedWhenDataClassNotSet() throws IOException {
        String fallbackStorageClass = getRandomAllowedStorageClassString();
        final S3BlobContainer container = buildContainer(null, null, fallbackStorageClass);
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertStorageClass(fallbackStorageClass, blobName);
    }

    public void testFallbackStorageClassUsedWhenMetadataClassNotSet() throws IOException {
        String fallbackStorageClass = getRandomAllowedStorageClassString();
        final S3BlobContainer container = buildContainer(null, null, fallbackStorageClass);
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertStorageClass(fallbackStorageClass, blobName);
    }

    public void testDefaultStorageClassUsedWhenNothingConfigured() throws IOException {
        final S3BlobContainer container = buildContainer(null, null, null);
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertStorageClass("STANDARD", blobName);
    }

    public void testDataStorageClassSentOnMultipartUpload() throws IOException {
        String dataStorageClass = getRandomAllowedStorageClassString();
        final S3BlobContainer container = buildContainer(dataStorageClass, null, null);
        final String blobName = randomIdentifier();
        // Write more than MULTIPART_BUFFER_SIZE to trigger multipart; writeBlob also requires >= MIN_PART_SIZE_USING_MULTIPART
        final byte[] data = randomByteArrayOfLength((int) S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes() + 1);

        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new BytesArray(data), false);

        assertStorageClass(dataStorageClass, blobName);
    }

    public void testMetadataStorageClassSentOnMultipartUpload() throws IOException {
        String metadataStorageClass = getRandomAllowedStorageClassString();
        final S3BlobContainer container = buildContainer(null, metadataStorageClass, null);
        final String blobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength((int) S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes() + 1);

        container.writeBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, new BytesArray(data), false);

        assertStorageClass(metadataStorageClass, blobName);
    }

    public void testMetadataStorageClassSentOnWriteMetadataBlob() throws IOException {
        String metadataStorageClass = getRandomAllowedStorageClassString();
        final S3BlobContainer container = buildContainer(null, metadataStorageClass, null);
        final String blobName = randomIdentifier();
        // Write more than MIN_PART_SIZE_USING_MULTIPART to trigger the multipart path inside writeMetadataBlob
        final byte[] data = randomByteArrayOfLength((int) S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes() + 1);

        container.writeMetadataBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, false, false, out -> out.write(data));

        assertStorageClass(metadataStorageClass, blobName);
    }

    public void testDataStorageClassSentOnCopyBlob() throws IOException {
        String dataStorageClass = getRandomAllowedStorageClassString();
        final S3BlobContainer container = buildContainer(dataStorageClass, null, null);
        final String sourceBlobName = randomIdentifier();
        final String destBlobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(64);

        // Write source with a non-snapshot purpose so it uses the fallback (STANDARD)
        container.writeBlob(OperationPurpose.CLUSTER_STATE, sourceBlobName, new BytesArray(data), false);
        assertStorageClass("STANDARD", sourceBlobName);

        // Copy with SNAPSHOT_DATA purpose — destination should receive the configured data storage class
        container.copyBlob(OperationPurpose.SNAPSHOT_DATA, container, sourceBlobName, destBlobName, data.length);

        assertStorageClass(dataStorageClass, destBlobName);
    }

    public void testMetadataStorageClassSentOnCopyBlob() throws IOException {
        String metadataStorageClass = getRandomAllowedStorageClassString();
        final S3BlobContainer container = buildContainer(null, metadataStorageClass, null);
        final String sourceBlobName = randomIdentifier();
        final String destBlobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(64);

        // Write source with a non-snapshot purpose so it uses the fallback (STANDARD)
        container.writeBlob(OperationPurpose.CLUSTER_STATE, sourceBlobName, new BytesArray(data), false);
        assertStorageClass("STANDARD", sourceBlobName);

        // Copy with SNAPSHOT_METADATA purpose — destination should receive the configured metadata storage class
        container.copyBlob(OperationPurpose.SNAPSHOT_METADATA, container, sourceBlobName, destBlobName, data.length);

        assertStorageClass(metadataStorageClass, destBlobName);
    }
}
