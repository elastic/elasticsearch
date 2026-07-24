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

import com.azure.storage.blob.models.AccessTier;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
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

@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerAccessTierTests extends ESTestCase {

    private static final String ACCOUNT = "account";
    private static final String CONTAINER = "container";

    private static HttpServer httpServer;
    private static ThreadPool threadPool;
    private static AzureClientProvider clientProvider;
    private static ClusterService clusterService;

    @BeforeClass
    public static void startServer() throws IOException {
        threadPool = new TestThreadPool(
            AzureBlobContainerAccessTierTests.class.getName(),
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

    private AzureBlobContainer buildContainer(@Nullable String dataAccessTier, @Nullable String metadataAccessTier) {
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
                return ByteSizeUnit.MB.toBytes(1);
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
                dataAccessTier,
                metadataAccessTier
            )
        );
    }

    private void asserAccessTier(String Hot, String blobName) {
        assertEquals(Hot, azureHttpHandler.getMockBlobStore().getBlob(blobName, null).accessTier());
    }

    private String getRandomAllowedAccessTierString() {
        return randomFrom(AzureBlobStore.ALLOWED_ACCESS_TIERS_BY_LOWER_NAME.values().stream().map(AccessTier::toString).toList());
    }

    public void testDataAccessTierSentOnSingleBlobUpload() throws IOException {
        String dataAccessTier = getRandomAllowedAccessTierString();
        final AzureBlobContainer container = buildContainer(dataAccessTier, null);
        final String blobName = randomIdentifier();

        container.getBlobStore()
            .writeBlob(
                OperationPurpose.SNAPSHOT_DATA,
                blobName,
                BytesReference.fromByteBuffer(ByteBuffer.wrap(randomByteArrayOfLength(128))),
                false
            );

        asserAccessTier(dataAccessTier, blobName);
    }

    public void testMetadataAccessTierSentOnSingleBlobUpload() throws IOException {
        String metadataAccessTier = getRandomAllowedAccessTierString();
        final AzureBlobContainer container = buildContainer(null, metadataAccessTier);
        final String blobName = randomIdentifier();

        container.getBlobStore()
            .writeBlob(
                OperationPurpose.SNAPSHOT_METADATA,
                blobName,
                BytesReference.fromByteBuffer(ByteBuffer.wrap(randomByteArrayOfLength(128))),
                false
            );

        asserAccessTier(metadataAccessTier, blobName);
    }

    public void testNoTierSentWhenNotConfigured() throws IOException {
        final AzureBlobContainer container = buildContainer(null, null);
        final String blobName = randomIdentifier();

        container.getBlobStore()
            .writeBlob(
                OperationPurpose.SNAPSHOT_DATA,
                blobName,
                BytesReference.fromByteBuffer(ByteBuffer.wrap(randomByteArrayOfLength(128))),
                false
            );

        assertNull(azureHttpHandler.getMockBlobStore().getBlob(blobName, null).accessTier());
    }

    public void testNoTierSentForNonSnapshotPurpose() throws IOException {

        final AzureBlobContainer container = buildContainer(getRandomAllowedAccessTierString(), getRandomAllowedAccessTierString());
        final String blobName = randomIdentifier();

        container.getBlobStore()
            .writeBlob(
                OperationPurpose.CLUSTER_STATE,
                blobName,
                BytesReference.fromByteBuffer(ByteBuffer.wrap(randomByteArrayOfLength(128))),
                false
            );

        assertNull(azureHttpHandler.getMockBlobStore().getBlob(blobName, null).accessTier());
    }

    public void testDataAccessTierSentOnMultipartUpload() throws IOException {
        String dataAccessTier = getRandomAllowedAccessTierString();
        final AzureBlobContainer container = buildContainer(dataAccessTier, null);
        final String blobName = randomIdentifier();

        // Write more than the single-part threshold to trigger the block list upload path
        final int blobSize = (int) (container.getBlobStore().getLargeBlobThresholdInBytes() + 1);
        final byte[] data = randomByteArrayOfLength(blobSize);

        container.getBlobStore().writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new ByteArrayInputStream(data), blobSize, false);

        asserAccessTier(dataAccessTier, blobName);
    }

    public void testMetadataAccessTierSentOnWriteMetadataBlobMultipartUpload() throws IOException {
        String metadataAccessTier = getRandomAllowedAccessTierString();
        final AzureBlobContainer container = buildContainer(null, metadataAccessTier);
        final String blobName = randomIdentifier();

        // Write more than the single-part threshold to flush at least one block, triggering the BlockBlobCommitBlockListOptions path
        final int blobSize = (int) (container.getBlobStore().getLargeBlobThresholdInBytes() + 1);
        final byte[] data = randomByteArrayOfLength(blobSize);

        container.writeMetadataBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, false, false, out -> out.write(data));

        asserAccessTier(metadataAccessTier, blobName);
    }

    public void testDataAccessTierSentOnWriteBlobAtomicMultipartUpload() throws IOException {
        String dataAccessTier = getRandomAllowedAccessTierString();
        final AzureBlobContainer container = buildContainer(dataAccessTier, null);
        final String blobName = randomIdentifier();

        // Write more than the single-part threshold to trigger the concurrent multipart path in writeBlobAtomic
        final int blobSize = (int) (container.getBlobStore().getLargeBlobThresholdInBytes() + 1);
        final byte[] data = randomByteArrayOfLength(blobSize);

        container.writeBlobAtomic(
            OperationPurpose.SNAPSHOT_DATA,
            blobName,
            blobSize,
            (offset, length) -> new ByteArrayInputStream(data, Math.toIntExact(offset), Math.toIntExact(length)),
            false,
            Runnable::run
        );

        asserAccessTier(dataAccessTier, blobName);
    }

    public void testDataAccessTierSentOnCopyBlob() throws IOException {
        String dataAccessTier = getRandomAllowedAccessTierString();
        final AzureBlobContainer container = buildContainer(dataAccessTier, null);
        final String sourceBlobName = randomIdentifier();
        final String destBlobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(128);

        // Write source blob with a non-snapshot purpose so it has no access tier
        container.getBlobStore()
            .writeBlob(OperationPurpose.CLUSTER_STATE, sourceBlobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(data)), false);
        assertNull(azureHttpHandler.getMockBlobStore().getBlob(sourceBlobName, null).accessTier());

        // Copy with snapshot data purpose — the destination should receive the configured data access tier
        container.copyBlob(OperationPurpose.SNAPSHOT_DATA, container, sourceBlobName, destBlobName, data.length);

        asserAccessTier(dataAccessTier, destBlobName);
    }

    public void testMetadataAccessTierSentOnCopyBlob() throws IOException {
        String metadataAccessTier = getRandomAllowedAccessTierString();
        final AzureBlobContainer container = buildContainer(null, metadataAccessTier);
        final String sourceBlobName = randomIdentifier();
        final String destBlobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(128);

        // Write source blob with a non-snapshot purpose so it has no access tier
        container.getBlobStore()
            .writeBlob(OperationPurpose.CLUSTER_STATE, sourceBlobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(data)), false);
        assertNull(azureHttpHandler.getMockBlobStore().getBlob(sourceBlobName, null).accessTier());

        // Copy with snapshot metadata purpose — the destination should receive the configured metadata access tier
        container.copyBlob(OperationPurpose.SNAPSHOT_METADATA, container, sourceBlobName, destBlobName, data.length);

        asserAccessTier(metadataAccessTier, destBlobName);
    }
}
