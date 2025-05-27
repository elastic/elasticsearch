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
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
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
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestStatus;
import org.junit.ClassRule;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.HttpURLConnection;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.Collection;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import static org.elasticsearch.common.io.Streams.limitStream;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * These tests sometimes run against a genuine Azure endpoint with credentials obtained from Vault. These credentials expire periodically
 * and must be manually renewed; the process is in the onboarding/process docs.
 */
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
        AzureStorageService createAzureStorageService(Settings settings, AzureClientProvider azureClientProvider) {
            final long blockSize = ByteSizeValue.ofKb(64L).getBytes() * randomIntBetween(1, 15);
            return new AzureStorageService(settings, azureClientProvider) {
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
                .client("default", LocationMode.PRIMARY_ONLY, randomFrom(OperationPurpose.values()));
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
        assertThat(
            asInstanceOf(AzureBlobStore.class, repo.blobStore()).getUploadBlockSize(),
            lessThan(ByteSizeUnit.MB.toBytes(1L))
        );

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
                    }, false);
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
}
