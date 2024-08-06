/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpFixture;

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
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestStatus;
import org.junit.ClassRule;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.util.Collection;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AzureStorageCleanupThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.azure.fixture", "true"));

    private static final String AZURE_ACCOUNT = System.getProperty("test.azure.account");

    @ClassRule
    public static AzureHttpFixture fixture = new AzureHttpFixture(
        USE_FIXTURE ? AzureHttpFixture.Protocol.HTTP : AzureHttpFixture.Protocol.NONE,
        AZURE_ACCOUNT,
        System.getProperty("test.azure.container"),
        System.getProperty("test.azure.tenant_id"),
        System.getProperty("test.azure.client_id"),
        AzureHttpFixture.sharedKeyForAccountPredicate(AZURE_ACCOUNT)
    );

    @Override
    public void testCreateSnapshot() {
        super.testCreateSnapshot();
    }

    @Override
    public void testIndexLatest() throws Exception {
        super.testIndexLatest();
    }

    @Override
    public void testListChildren() {
        super.testListChildren();
    }

    @Override
    public void testCleanup() throws Exception {
        super.testCleanup();
    }

    @Override
    public void testReadFromPositionWithLength() {
        super.testReadFromPositionWithLength();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(AzureRepositoryPlugin.class);
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
            secureSettings.setString("azure.client.default.sas_token", System.getProperty("test.azure.sas_token"));
        } else {
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
                    .put("max_single_part_upload_size", new ByteSizeValue(1, ByteSizeUnit.MB))
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
            final AzureBlobServiceClient azureBlobServiceClient = blobStore.getService().client("default", LocationMode.PRIMARY_ONLY);
            final BlobServiceClient client = azureBlobServiceClient.getSyncClient();
            try {
                SocketAccess.doPrivilegedException(() -> {
                    final BlobContainerClient blobContainer = client.getBlobContainerClient(blobStore.toString());
                    return blobContainer.exists();
                });
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
        // The configured threshold for this test suite is 1mb
        final int blobSize = ByteSizeUnit.MB.toIntBytes(2);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        repo.threadPool().generic().execute(ActionRunnable.run(future, () -> {
            final BlobContainer blobContainer = repo.blobStore().blobContainer(repo.basePath().add("large_write"));
            blobContainer.writeBlob(
                randomPurpose(),
                UUIDs.base64UUID(),
                new ByteArrayInputStream(randomByteArrayOfLength(blobSize)),
                blobSize,
                false
            );
            blobContainer.delete(randomPurpose());
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
