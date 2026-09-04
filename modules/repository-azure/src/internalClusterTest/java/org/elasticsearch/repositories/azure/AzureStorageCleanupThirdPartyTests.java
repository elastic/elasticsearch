/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpFixture;
import fixture.azure.MockAzureBlobStore;

import com.azure.core.exception.HttpResponseException;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestStatus;
import org.junit.ClassRule;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.common.io.Streams.limitStream;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * These tests sometimes run against a genuine Azure endpoint with credentials obtained from Vault. These credentials expire periodically
 * and must be manually renewed; the process is in the onboarding/process docs.
 * Most tests that run against a genuine Azure endpoint can take between 1 and 3 minutes, hence the large suite timeout.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class AzureStorageCleanupThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {
    private static final Logger logger = LogManager.getLogger(AzureStorageCleanupThirdPartyTests.class);
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.azure.fixture", "true"));

    private static final String AZURE_ACCOUNT = System.getProperty("test.azure.account");

    /**
     * AzureRepositoryPlugin that sets a low value for getUploadBlockSize()
     */
    public static class TestAzureRepositoryPlugin extends AzureRepositoryPlugin {

        public TestAzureRepositoryPlugin(Settings settings) {
            super(settings);
        }

        @Override
        AzureStorageService createAzureStorageService(
            Settings settings,
            AzureClientProvider azureClientProvider,
            ClusterService clusterService,
            ProjectResolver projectResolver
        ) {
            final long blockSize = ByteSizeValue.ofKb(64L).getBytes() * randomIntBetween(1, 15);
            return new AzureStorageService(settings, azureClientProvider, clusterService, projectResolver) {
                @Override
                long getUploadBlockSize() {
                    return blockSize;
                }
            };
        }
    }

    @ClassRule
    public static AzureHttpFixture fixture = new AzureHttpFixture(
        USE_FIXTURE ? AzureHttpFixture.Protocol.HTTP : AzureHttpFixture.Protocol.NONE,
        null,
        AZURE_ACCOUNT,
        System.getProperty("test.azure.container"),
        System.getProperty("test.azure.tenant_id"),
        System.getProperty("test.azure.client_id"),
        AzureHttpFixture.sharedKeyForAccountPredicate(AZURE_ACCOUNT),
        MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE
    );

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestAzureRepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        if (USE_FIXTURE) {
            final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + fixture.getAddress();
            return Settings.builder().put(super.nodeSettings()).put("azure.client.default.endpoint_suffix", endpoint).build();
        }
        return super.nodeSettings();
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(System.getProperty("test.azure.account"), not(blankOrNullString()));
        final boolean hasSasToken = Strings.hasText(System.getProperty("test.azure.sas_token"));
        if (hasSasToken == false) {
            assertThat(System.getProperty("test.azure.key"), not(blankOrNullString()));
        } else {
            assertThat(System.getProperty("test.azure.key"), blankOrNullString());
        }
        assertThat(System.getProperty("test.azure.container"), not(blankOrNullString()));
        assertThat(System.getProperty("test.azure.base"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.default.account", System.getProperty("test.azure.account"));
        if (hasSasToken) {
            logger.info("--> Using SAS token authentication");
            secureSettings.setString("azure.client.default.sas_token", System.getProperty("test.azure.sas_token"));
        } else {
            logger.info("--> Using key authentication");
            secureSettings.setString("azure.client.default.key", System.getProperty("test.azure.key"));
        }
        return secureSettings;
    }

    @Override
    protected void createRepository(String repoName) {
        AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            repoName
        )
            .setType("azure")
            .setSettings(
                Settings.builder()
                    .put("container", System.getProperty("test.azure.container"))
                    .put("base_path", System.getProperty("test.azure.base") + randomAlphaOfLength(8))
                    .put("max_single_part_upload_size", ByteSizeValue.of(1, ByteSizeUnit.MB))
            )
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
        if (Strings.hasText(System.getProperty("test.azure.sas_token"))) {
            ensureSasTokenPermissions();
        }
    }

    private void ensureSasTokenPermissions() {
        final BlobStoreRepository repository = getRepository();
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        repository.threadPool().generic().execute(ActionRunnable.wrap(future, l -> {
            final AzureBlobStore blobStore = (AzureBlobStore) repository.blobStore();
            final AzureBlobServiceClient azureBlobServiceClient = blobStore.getService()
                .client(ProjectId.DEFAULT, "default", LocationMode.PRIMARY_ONLY, randomFrom(OperationPurpose.values()));
            final BlobServiceClient client = azureBlobServiceClient.getSyncClient();
            try {
                final BlobContainerClient blobContainer = client.getBlobContainerClient(blobStore.toString());
                blobContainer.exists();
                future.onFailure(
                    new RuntimeException(
                        "The SAS token used in this test allowed for checking container existence. This test only supports tokens "
                            + "that grant only the documented permission requirements for the Azure repository plugin."
                    )
                );
            } catch (BlobStorageException e) {
                if (e.getStatusCode() == HttpURLConnection.HTTP_FORBIDDEN) {
                    future.onResponse(null);
                } else {
                    future.onFailure(e);
                }
            }
        }));
        future.actionGet();
    }

    public void testMultiBlockUpload() throws Exception {
        final BlobStoreRepository repo = getRepository();
        assertThat(
            asInstanceOf(AzureBlobStore.class, repo.blobStore()).getLargeBlobThresholdInBytes(),
            equalTo(ByteSizeUnit.MB.toBytes(1L))
        );
        assertThat(asInstanceOf(AzureBlobStore.class, repo.blobStore()).getUploadBlockSize(), lessThan(ByteSizeUnit.MB.toBytes(1L)));

        // The configured threshold for this test suite is 1mb
        final long blobSize = randomLongBetween(ByteSizeUnit.MB.toBytes(2), ByteSizeUnit.MB.toBytes(4));
        final int bufferSize = 8192;

        final var file = createTempFile();
        final long expectedChecksum;
        try (var output = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(file)), new CRC32())) {
            long remaining = blobSize;
            while (remaining > 0L) {
                final var buffer = randomByteArrayOfLength(Math.toIntExact(Math.min(bufferSize, remaining)));
                output.write(buffer);
                remaining -= buffer.length;
            }
            output.flush();
            expectedChecksum = output.getChecksum().getValue();
        }

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        repo.threadPool().generic().execute(ActionRunnable.run(future, () -> {
            final BlobContainer blobContainer = repo.blobStore().blobContainer(repo.basePath().add("large_write"));
            try {
                final var blobName = UUIDs.base64UUID();
                if (randomBoolean()) {
                    try (var input = new BufferedInputStream(Files.newInputStream(file))) {
                        blobContainer.writeBlob(randomPurpose(), blobName, input, blobSize, false);
                    }
                } else {
                    assertThat(blobContainer.supportsConcurrentMultipartUploads(), equalTo(true));
                    blobContainer.writeBlobAtomic(randomPurpose(), blobName, blobSize, (offset, length) -> {
                        var channel = Files.newByteChannel(file);
                        if (offset > 0L) {
                            if (channel.size() <= offset) {
                                throw new AssertionError();
                            }
                            channel.position(offset);
                        }
                        assert channel.position() == offset;
                        return new BufferedInputStream(limitStream(Channels.newInputStream(channel), length));
                    }, false, Runnable::run);
                }

                long bytesCount = 0L;
                try (var input = new CheckedInputStream(blobContainer.readBlob(OperationPurpose.INDICES, blobName), new CRC32())) {
                    var buffer = new byte[bufferSize];
                    int bytesRead;
                    while ((bytesRead = input.read(buffer)) != -1) {
                        bytesCount += bytesRead;
                    }

                    assertThat(bytesCount, equalTo(blobSize));
                    assertThat(input.getChecksum().getValue(), equalTo(expectedChecksum));
                }
            } finally {
                blobContainer.delete(randomPurpose());
            }
        }));
        future.get();
    }

    public void testReadFromPositionLargerThanBlobLength() {
        testReadFromPositionLargerThanBlobLength(
            e -> asInstanceOf(BlobStorageException.class, ExceptionsHelper.unwrap(e, HttpResponseException.class))
                .getStatusCode() == RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus()
        );
    }

    public void testCopy() throws Exception {
        final var sourceBlobName = randomIdentifier();
        final var repository = getRepository();
        final var destinationBlobName = randomIdentifier();
        final var blobStore = repository.blobStore();
        final var sourceBlobContainer = blobStore.blobContainer(repository.basePath());
        final var blobBytes = randomBytesReference(randomIntBetween(100, 2_000_000));
        sourceBlobContainer.writeBlob(randomPurpose(), sourceBlobName, blobBytes, true);
        assertBusy(() -> assertTrue(sourceBlobContainer.blobExists(randomPurpose(), sourceBlobName)));

        final var destinationBlobContainer = repository.blobStore().blobContainer(repository.basePath().add("target"));
        destinationBlobContainer.copyBlob(randomPurpose(), sourceBlobContainer, sourceBlobName, destinationBlobName, blobBytes.length());
        assertThat(Streams.readFully(destinationBlobContainer.readBlob(randomPurpose(), destinationBlobName)), equalBytes(blobBytes));

        sourceBlobContainer.delete(randomPurpose());
        assertThrows(
            NoSuchFileException.class,
            () -> destinationBlobContainer.copyBlob(
                randomPurpose(),
                sourceBlobContainer,
                sourceBlobName,
                destinationBlobName,
                blobBytes.length()
            )
        );
        destinationBlobContainer.delete(randomPurpose());
    }

    /**
     * Verifies that the {@code data_access_tier} / {@code metadata_access_tier} settings are applied to the correct uploads. We configure
     * two distinct access tiers, write blobs with each {@link OperationPurpose} via the single-part, multi-part and server-side copy paths,
     * and read the access tier back from the blob properties.
     */
    public void testAccessTierPerOperationPurpose() throws Exception {
        final List<AccessTier> accessTiers = shuffledList(new ArrayList<>(AzureBlobStore.ALLOWED_ACCESS_TIERS_BY_LOWER_NAME.values()));
        final AccessTier dataAccessTier = accessTiers.get(0);
        final AccessTier metadataAccessTier = accessTiers.get(1);
        logger.info("--> data_access_tier [{}], metadata_access_tier [{}]", dataAccessTier, metadataAccessTier);

        final String repoName = "test-access-tier-repo";
        final AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            repoName
        )
            .setType("azure")
            .setSettings(
                Settings.builder()
                    .put("container", System.getProperty("test.azure.container"))
                    .put("base_path", System.getProperty("test.azure.base") + randomAlphaOfLength(8))
                    .put(AzureRepository.Repository.MAX_SINGLE_PART_UPLOAD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.MB))
                    .put(AzureRepository.Repository.DATA_ACCESS_TIER_SETTING.getKey(), dataAccessTier.toString())
                    .put(AzureRepository.Repository.METADATA_ACCESS_TIER_SETTING.getKey(), metadataAccessTier.toString())
            )
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        try {
            final BlobStoreRepository repository = (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository(
                repoName
            );
            final AzureBlobStore blobStore = asInstanceOf(AzureBlobStore.class, repository.blobStore());
            final BlobContainer blobContainer = blobStore.blobContainer(repository.basePath());
            final String keyPrefix = repository.basePath().buildAsString();
            try {
                // a purpose that resolves to no explicit access tier (i.e. neither SNAPSHOT_DATA nor SNAPSHOT_METADATA)
                final OperationPurpose noTierPurpose = randomFrom(
                    OperationPurpose.REPOSITORY_ANALYSIS,
                    OperationPurpose.CLUSTER_STATE,
                    OperationPurpose.INDICES,
                    OperationPurpose.TRANSLOG
                );
                for (final OperationPurpose purpose : List.of(
                    OperationPurpose.SNAPSHOT_DATA,
                    OperationPurpose.SNAPSHOT_METADATA,
                    noTierPurpose
                )) {
                    final Optional<AccessTier> expectedTier = blobStore.resolveAccessTier(purpose);

                    // single-part upload (blob stays below the multipart threshold)
                    final String singlePartName = randomIdentifier();
                    final BytesReference singlePartBytes = randomBytesReference(between(1, 512));
                    blobContainer.writeBlob(purpose, singlePartName, singlePartBytes, true);
                    assertAccessTier(blobStore, keyPrefix + singlePartName, expectedTier, "single-part upload", purpose);

                    // multi-part upload (exceeds the single-part threshold so the block list upload path is used)
                    final String multiPartName = randomIdentifier();
                    final int multiPartSize = Math.toIntExact(blobStore.getLargeBlobThresholdInBytes()) + between(1, 1024);
                    final byte[] multiPartBytes = randomByteArrayOfLength(multiPartSize);
                    blobContainer.writeBlob(purpose, multiPartName, new ByteArrayInputStream(multiPartBytes), multiPartSize, true);
                    assertAccessTier(blobStore, keyPrefix + multiPartName, expectedTier, "multi-part upload", purpose);

                    // server-side copy (source is the small single-part blob written above)
                    final String copyName = randomIdentifier();
                    blobContainer.copyBlob(purpose, blobContainer, singlePartName, copyName, singlePartBytes.length());
                    assertAccessTier(blobStore, keyPrefix + copyName, expectedTier, "server-side copy", purpose);
                }
            } finally {
                blobContainer.delete(randomPurpose());
            }
        } finally {
            clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).get();
        }
    }

    private static void assertAccessTier(
        AzureBlobStore blobStore,
        String blobKey,
        Optional<AccessTier> expectedTier,
        String uploadType,
        OperationPurpose purpose
    ) {
        final BlobServiceClient client = blobStore.getService()
            .client(ProjectId.DEFAULT, "default", LocationMode.PRIMARY_ONLY, purpose)
            .getSyncClient();
        final BlobClient blobClient = client.getBlobContainerClient(blobStore.toString()).getBlobClient(blobKey);
        final BlobProperties properties = blobClient.getProperties();
        final String message = uploadType + " with purpose [" + purpose + "]";
        if (expectedTier.isPresent()) {
            assertThat(message, properties.getAccessTier(), equalTo(expectedTier.get()));
            assertThat(message + " should carry an explicit tier", Boolean.TRUE.equals(properties.isAccessTierInferred()), is(false));
        } else {
            // No tier configured: the fixture reports no tier, while a real account reports its inferred default tier
            assertThat(
                message + " should not carry an explicit tier",
                properties.getAccessTier() == null || Boolean.TRUE.equals(properties.isAccessTierInferred()),
                is(true)
            );
        }
    }
}
