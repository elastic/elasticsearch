/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;

import com.google.cloud.storage.StorageClass;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Locale;

import static fixture.gcs.TestUtils.createServiceAccount;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;

@SuppressForbidden(reason = "use a http server")
public class GoogleCloudStorageBlobStoreStorageClassTests extends ESTestCase {

    private static final String BUCKET = "bucket";

    private static HttpServer httpServer;
    private static ClusterService clusterService;

    @BeforeClass
    public static void startServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        clusterService = ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool());
    }

    @AfterClass
    public static void stopServer() {
        httpServer.stop(0);
        httpServer = null;
        clusterService = null;
    }

    private GoogleCloudStorageHttpHandler handler;

    @Before
    public void configureHandler() {
        handler = new GoogleCloudStorageHttpHandler(BUCKET);
        httpServer.createContext("/", handler);
        httpServer.createContext("/token", new FakeOAuth2HttpHandler());
    }

    @After
    public void removeHandler() {
        httpServer.removeContext("/");
        httpServer.removeContext("/token");
    }

    private String httpServerUrl() {
        final InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    private BlobContainer buildContainer(
        @Nullable String dataStorageClass,
        @Nullable String metadataStorageClass,
        long largeBlobThreshold
    ) {
        final String client = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        final Settings.Builder clientSettings = Settings.builder();
        clientSettings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace(client).getKey(), httpServerUrl());
        clientSettings.put(TOKEN_URI_SETTING.getConcreteSettingForNamespace(client).getKey(), httpServerUrl() + "/token");
        clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(client).getKey(), 0);

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile(CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(client).getKey(), createServiceAccount(random()));
        clientSettings.setSecureSettings(secureSettings);

        final GoogleCloudStorageService service = new GoogleCloudStorageService(clusterService, TestProjectResolvers.DEFAULT_PROJECT_ONLY);
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(clientSettings.build()));

        final GoogleCloudStorageBlobStore blobStore = new GoogleCloudStorageBlobStore(
            ProjectId.DEFAULT,
            BUCKET,
            client,
            "repo",
            service,
            BigArrays.NON_RECYCLING_INSTANCE,
            randomIntBetween(1, 8) * 1024,
            BackoffPolicy.noBackoff(),
            new GcsRepositoryStatsCollector(),
            dataStorageClass,
            metadataStorageClass
        ) {
            @Override
            long getLargeBlobThresholdInBytes() {
                return largeBlobThreshold;
            }
        };

        return blobStore.blobContainer(BlobPath.EMPTY);
    }

    public void testDataStorageClassSentOnSingleBlobUpload() throws IOException {
        final String dataStorageClass = randomAllowedStorageClass();
        final BlobContainer container = buildContainer(dataStorageClass, null, ByteSizeValue.ofMb(5).getBytes());
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertStorageClass(dataStorageClass, blobName);
    }

    public void testMetadataStorageClassSentOnSingleBlobUpload() throws IOException {
        final String metadataStorageClass = randomAllowedStorageClass();
        final BlobContainer container = buildContainer(null, metadataStorageClass, ByteSizeValue.ofMb(5).getBytes());
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertStorageClass(metadataStorageClass, blobName);
    }

    public void testDataStorageClassSentOnResumableUpload() throws IOException {
        final String dataStorageClass = randomAllowedStorageClass();
        // use a smaller large blob threshold than the blob size to force the resumable upload path
        final BlobContainer container = buildContainer(dataStorageClass, null, 128);
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new BytesArray(randomByteArrayOfLength(512)), false);

        assertStorageClass(dataStorageClass, blobName);
    }

    public void testMetaDataStorageClassSentOnResumableUpload() throws IOException {
        final String metadataStorageClass = randomAllowedStorageClass();
        // use a smaller large blob threshold than the blob size to force the resumable upload path
        final BlobContainer container = buildContainer(null, metadataStorageClass, 128);
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_METADATA, blobName, new BytesArray(randomByteArrayOfLength(512)), false);

        assertStorageClass(metadataStorageClass, blobName);
    }

    public void testNoStorageClassSentForNonSnapshotPurpose() throws IOException {
        final BlobContainer container = buildContainer(
            randomAllowedStorageClass(),
            randomAllowedStorageClass(),
            ByteSizeValue.ofMb(5).getBytes()
        );
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.CLUSTER_STATE, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertStorageClass(null, blobName);
    }

    public void testNoStorageClassSentWhenNothingConfigured() throws IOException {
        final BlobContainer container = buildContainer(null, null, ByteSizeValue.ofMb(5).getBytes());
        final String blobName = randomIdentifier();

        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, new BytesArray(randomByteArrayOfLength(64)), false);

        assertStorageClass(null, blobName);
    }

    public void testDataStorageClassSentOnCopyBlob() throws IOException {
        final String dataStorageClass = randomAllowedStorageClass();
        final BlobContainer container = buildContainer(dataStorageClass, null, ByteSizeValue.ofMb(5).getBytes());
        final String sourceBlobName = randomIdentifier();
        final String destBlobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(64);

        // write the source with a non-snapshot purpose so it carries no storage class
        container.writeBlob(OperationPurpose.CLUSTER_STATE, sourceBlobName, new BytesArray(data), false);
        assertStorageClass(null, sourceBlobName);

        // copy with SNAPSHOT_DATA purpose so the destination is assigned the configured data storage class
        container.copyBlob(OperationPurpose.SNAPSHOT_DATA, container, sourceBlobName, destBlobName, data.length);

        assertStorageClass(dataStorageClass, destBlobName);
    }

    public void testMetaDataStorageClassSentOnCopyBlob() throws IOException {
        final String metadataStorageClass = randomAllowedStorageClass();
        final BlobContainer container = buildContainer(null, metadataStorageClass, ByteSizeValue.ofMb(5).getBytes());
        final String sourceBlobName = randomIdentifier();
        final String destBlobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(64);

        // write the source with a non-snapshot purpose so it carries no storage class
        container.writeBlob(OperationPurpose.CLUSTER_STATE, sourceBlobName, new BytesArray(data), false);
        assertStorageClass(null, sourceBlobName);

        // copy with SNAPSHOT_METADATA purpose so the destination is assigned the configured metadata storage class
        container.copyBlob(OperationPurpose.SNAPSHOT_METADATA, container, sourceBlobName, destBlobName, data.length);

        assertStorageClass(metadataStorageClass, destBlobName);
    }

    public void testInvalidStorageClassIsRejected() {
        final BlobStoreException e = expectThrows(
            BlobStoreException.class,
            () -> GoogleCloudStorageBlobStore.initStorageClass("not-a-real-class")
        );
        assertTrue(e.getMessage().contains("not an allowed GCS Storage Class"));
    }

    private void assertStorageClass(@Nullable String expected, String blobName) {
        assertEquals(expected, handler.getBlobStorageClass(blobName));
    }

    private static String randomAllowedStorageClass() {
        return randomFrom(
            GoogleCloudStorageBlobStore.ALLOWED_STORAGE_CLASSES_BY_LOWER_NAME.values().stream().map(StorageClass::toString).toList()
        );
    }
}
