/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.cloud.storage.StorageClass;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GoogleCloudBlobStoreTests extends ESTestCase {

    // --- initStorageClass ---

    public void testInitStorageClassReturnsNullForBlankInput() {
        assertNull(GoogleCloudStorageBlobStore.initStorageClass(null));
        assertNull(GoogleCloudStorageBlobStore.initStorageClass(""));
        assertNull(GoogleCloudStorageBlobStore.initStorageClass("   "));
    }

    public void testInitStorageClassIsCaseInsensitive() {
        for (String input : List.of("standard", "STANDARD", "Standard")) {
            assertSame(
                "Expected StorageClass.STANDARD for input: " + input,
                StorageClass.STANDARD,
                GoogleCloudStorageBlobStore.initStorageClass(input)
            );
        }
        for (String input : List.of("nearline", "NEARLINE", "Nearline")) {
            assertSame(
                "Expected StorageClass.NEARLINE for input: " + input,
                StorageClass.NEARLINE,
                GoogleCloudStorageBlobStore.initStorageClass(input)
            );
        }
        for (String input : List.of("coldline", "COLDLINE", "Coldline")) {
            assertSame(
                "Expected StorageClass.COLDLINE for input: " + input,
                StorageClass.COLDLINE,
                GoogleCloudStorageBlobStore.initStorageClass(input)
            );
        }
    }

    public void testInitStorageClassTrimsWhitespace() {
        assertSame(StorageClass.STANDARD, GoogleCloudStorageBlobStore.initStorageClass("  standard  "));
        assertSame(StorageClass.NEARLINE, GoogleCloudStorageBlobStore.initStorageClass("\tnearline\t"));
        assertSame(StorageClass.COLDLINE, GoogleCloudStorageBlobStore.initStorageClass(" Coldline "));
    }

    public void testInitStorageClassThrowsForUnknownValue() {
        // `archive` is a real GCS storage class but is intentionally not allowed for snapshot repositories
        for (String input : List.of("archive", "Archive", "multi_regional", "regional", "invalid")) {
            BlobStoreException ex = expectThrows(BlobStoreException.class, () -> GoogleCloudStorageBlobStore.initStorageClass(input));
            assertThat(ex.getMessage(), containsString(input));
            assertThat(ex.getMessage(), containsString("not an allowed GCS Storage Class"));
        }
    }

    // --- resolveStorageClass ---

    public void testResolveStorageClassReturnsDataClassForSnapshotData() {
        final GoogleCloudStorageBlobStore store = newBlobStore("standard", "nearline");
        assertSame(StorageClass.STANDARD, store.resolveStorageClass(OperationPurpose.SNAPSHOT_DATA));
    }

    public void testResolveStorageClassReturnsMetadataClassForSnapshotMetadata() {
        final GoogleCloudStorageBlobStore store = newBlobStore("standard", "nearline");
        assertSame(StorageClass.NEARLINE, store.resolveStorageClass(OperationPurpose.SNAPSHOT_METADATA));
    }

    public void testResolveStorageClassReturnsNullWhenDataClassNotConfigured() {
        final GoogleCloudStorageBlobStore store = newBlobStore(null, "nearline");
        assertNull(store.resolveStorageClass(OperationPurpose.SNAPSHOT_DATA));
    }

    public void testResolveStorageClassReturnsNullWhenMetadataClassNotConfigured() {
        final GoogleCloudStorageBlobStore store = newBlobStore("standard", null);
        assertNull(store.resolveStorageClass(OperationPurpose.SNAPSHOT_METADATA));
    }

    public void testResolveStorageClassReturnsNullForAllNonSnapshotPurposes() {
        final GoogleCloudStorageBlobStore store = newBlobStore("standard", "nearline");
        for (OperationPurpose purpose : OperationPurpose.values()) {
            if (purpose == OperationPurpose.SNAPSHOT_DATA || purpose == OperationPurpose.SNAPSHOT_METADATA) {
                continue;
            }
            assertNull("Expected null storage class for purpose " + purpose, store.resolveStorageClass(purpose));
        }
    }

    // --- repository-level setting validation ---

    public void testValidStorageClassIsAccepted() {
        final String settingKey = randomFrom(
            GoogleCloudStorageRepository.DATA_STORAGE_CLASS.getKey(),
            GoogleCloudStorageRepository.METADATA_STORAGE_CLASS.getKey()
        );
        final String value = randomFrom("standard", "nearline", "coldline", "COLDLINE", " NearLine ");
        // should construct without throwing
        createRepositoryWithSetting(settingKey, value);
    }

    public void testInvalidStorageClassIsRejected() {
        final String settingKey = randomFrom(
            GoogleCloudStorageRepository.DATA_STORAGE_CLASS.getKey(),
            GoogleCloudStorageRepository.METADATA_STORAGE_CLASS.getKey()
        );
        final RepositoryException e = expectThrows(RepositoryException.class, () -> createRepositoryWithSetting(settingKey, "archive"));
        assertThat(e.getMessage(), containsString(settingKey));
        assertThat(e.getMessage(), containsString("not an allowed GCS Storage Class"));
    }

    private GoogleCloudStorageBlobStore newBlobStore(@Nullable String dataStorageClass, @Nullable String metadataStorageClass) {
        final GoogleCloudStorageService service = mock(GoogleCloudStorageService.class);
        final GoogleCloudStorageClientSettings clientSettings = mock(GoogleCloudStorageClientSettings.class);
        when(clientSettings.getTenaciousRetriesEnabled()).thenReturn(randomBoolean());
        when(service.clientSettings(any(), any())).thenReturn(clientSettings);
        return new GoogleCloudStorageBlobStore(
            ProjectId.DEFAULT,
            "test-bucket",
            "test-client",
            "test-repo",
            service,
            BigArrays.NON_RECYCLING_INSTANCE,
            randomIntBetween(1, 8) * 1024,
            BackoffPolicy.noBackoff(),
            mock(GcsRepositoryStatsCollector.class),
            dataStorageClass,
            metadataStorageClass
        );
    }

    private GoogleCloudStorageRepository createRepositoryWithSetting(String settingKey, String value) {
        final GoogleCloudStorageService storageService = mock(GoogleCloudStorageService.class);
        final GoogleCloudStorageClientSettings clientSettings = mock(GoogleCloudStorageClientSettings.class);
        when(clientSettings.getTenaciousRetriesEnabled()).thenReturn(randomBoolean());
        when(storageService.clientSettings(any(), any())).thenReturn(clientSettings);
        return new GoogleCloudStorageRepository(
            randomProjectIdOrDefault(),
            new RepositoryMetadata(
                randomIdentifier(),
                GoogleCloudStorageRepository.TYPE,
                Settings.builder()
                    .put(GoogleCloudStorageRepository.BUCKET.getKey(), randomIdentifier())
                    .put(GoogleCloudStorageRepository.BASE_PATH.getKey(), randomIdentifier())
                    .put(settingKey, value)
                    .build()
            ),
            NamedXContentRegistry.EMPTY,
            storageService,
            BlobStoreTestUtil.mockClusterService(),
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(GcsRepositoryStatsCollector.class),
            SnapshotMetrics.NOOP
        );
    }
}
