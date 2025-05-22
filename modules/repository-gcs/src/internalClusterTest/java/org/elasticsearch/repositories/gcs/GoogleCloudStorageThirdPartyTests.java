/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import fixture.gcs.GoogleCloudStorageHttpFixture;
import fixture.gcs.TestUtils;

import com.google.cloud.storage.StorageException;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Base64;
import java.util.Collection;

import static org.elasticsearch.common.io.Streams.readFully;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_HOST_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_PORT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_TYPE_SETTING;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class GoogleCloudStorageThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.google.fixture", "true"));
    private static final String PROXIED_TEST_REPO = "proxied-test-repo";
    private static final String PROXIED_CLIENT = "proxied";

    @ClassRule
    public static GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(USE_FIXTURE, "bucket", "o/oauth2/token");
    private static WebProxyServer proxyServer;

    @BeforeClass
    public static void beforeClass() {
        proxyServer = new WebProxyServer();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        proxyServer.close();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());

        if (USE_FIXTURE) {
            builder.put("gcs.client.default.endpoint", fixture.getAddress());
            builder.put("gcs.client.default.token_uri", fixture.getAddress() + "/o/oauth2/token");
            builder.put("gcs.client.proxied.endpoint", fixture.getAddress());
            builder.put("gcs.client.proxied.token_uri", fixture.getAddress() + "/o/oauth2/token");
        }

        // Add a proxied client so we can test resume on fail
        builder.put(PROXY_HOST_SETTING.getConcreteSettingForNamespace(PROXIED_CLIENT).getKey(), proxyServer.getHost());
        builder.put(PROXY_PORT_SETTING.getConcreteSettingForNamespace(PROXIED_CLIENT).getKey(), proxyServer.getPort());
        builder.put(PROXY_TYPE_SETTING.getConcreteSettingForNamespace(PROXIED_CLIENT).getKey(), "http");

        return builder.build();
    }

    @Override
    protected SecureSettings credentials() {
        if (USE_FIXTURE == false) {
            assertThat(System.getProperty("test.google.account"), not(blankOrNullString()));
        }
        assertThat(System.getProperty("test.google.bucket"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        if (USE_FIXTURE) {
            secureSettings.setFile("gcs.client.default.credentials_file", TestUtils.createServiceAccount(random()));
            secureSettings.setFile("gcs.client.proxied.credentials_file", TestUtils.createServiceAccount(random()));
        } else {
            secureSettings.setFile(
                "gcs.client.default.credentials_file",
                Base64.getDecoder().decode(System.getProperty("test.google.account"))
            );
            secureSettings.setFile(
                "gcs.client.proxied.credentials_file",
                Base64.getDecoder().decode(System.getProperty("test.google.account"))
            );
        }
        return secureSettings;
    }

    @Override
    protected void createRepository(final String repoName) {
        createRepository(repoName, "default");
    }

    private void createRepository(final String repoName, String clientName) {
        AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            repoName
        )
            .setType("gcs")
            .setSettings(
                Settings.builder()
                    .put("bucket", System.getProperty("test.google.bucket"))
                    .put("base_path", System.getProperty("test.google.base", "/") + "_" + repoName)
                    .put("client", clientName)
            )
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    public void testReadFromPositionLargerThanBlobLength() {
        testReadFromPositionLargerThanBlobLength(
            e -> asInstanceOf(StorageException.class, e.getCause()).getCode() == RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus()
        );
    }

    public void testResumeAfterUpdate() {
        createRepository(PROXIED_TEST_REPO, PROXIED_CLIENT);

        try {
            // The blob needs to be large enough that it won't be entirely buffered on the first request
            final int enoughBytesToNotBeEntirelyBuffered = Math.toIntExact(ByteSizeValue.ofMb(10).getBytes());

            final BlobStoreRepository repo = getRepository(PROXIED_TEST_REPO);
            final String blobKey = randomIdentifier();
            final byte[] initialValue = randomByteArrayOfLength(enoughBytesToNotBeEntirelyBuffered);
            executeOnBlobStore(repo, container -> {
                container.writeBlob(randomPurpose(), blobKey, new BytesArray(initialValue), true);

                try (InputStream inputStream = container.readBlob(randomPurpose(), blobKey)) {
                    // Trigger the first request for the blob, partially read it
                    int read = inputStream.read();
                    assert read != -1;

                    // Restart the server (this triggers a retry)
                    proxyServer.restart();

                    // Update the file
                    byte[] updatedValue = randomByteArrayOfLength(enoughBytesToNotBeEntirelyBuffered);
                    container.writeBlob(randomPurpose(), blobKey, new BytesArray(updatedValue), false);

                    // Read the rest of the stream, it should throw because the contents changed
                    String message = assertThrows(NoSuchFileException.class, () -> readFully(inputStream)).getMessage();
                    assertThat(
                        message,
                        startsWith(
                            "Blob object ["
                                + container.path().buildAsString()
                                + blobKey
                                + "] generation [1] unavailable on resume (contents changed, or object deleted):"
                        )
                    );
                } catch (Exception e) {
                    fail(e);
                }
                return null;
            });
        } finally {
            final BlobStoreRepository repository = getRepository(PROXIED_TEST_REPO);
            deleteAndAssertEmpty(repository, repository.basePath());
        }
    }
}
