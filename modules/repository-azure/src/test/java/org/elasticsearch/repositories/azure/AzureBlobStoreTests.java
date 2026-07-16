/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import com.azure.storage.blob.models.AccessTier;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;

public class AzureBlobStoreTests extends ESTestCase {

    // --- initAccessTier ---

    public void testInitAccessTierReturnsNullForBlankInput() {
        assertNull(AzureBlobStore.initAccessTier(null));
        assertNull(AzureBlobStore.initAccessTier(""));
        assertNull(AzureBlobStore.initAccessTier("   "));
    }

    public void testInitAccessTierIsCaseInsensitive() {
        for (String input : List.of("hot", "HOT", "Hot")) {
            assertSame("Expected same AccessTier.HOT instance for input: " + input, AccessTier.HOT, AzureBlobStore.initAccessTier(input));
        }
        for (String input : List.of("cool", "COOL", "Cool")) {
            assertSame("Expected same AccessTier.COOL instance for input: " + input, AccessTier.COOL, AzureBlobStore.initAccessTier(input));
        }
        for (String input : List.of("cold", "COLD", "Cold")) {
            assertSame("Expected same AccessTier.COLD instance for input: " + input, AccessTier.COLD, AzureBlobStore.initAccessTier(input));
        }
    }

    public void testInitAccessTierTrimsWhitespace() {
        assertSame(AccessTier.HOT, AzureBlobStore.initAccessTier("  hot  "));
        assertSame(AccessTier.COOL, AzureBlobStore.initAccessTier("\tcool\t"));
        assertSame(AccessTier.COLD, AzureBlobStore.initAccessTier(" Cold "));
    }

    public void testInitAccessTierReturnsNullForWhitespaceOnlyAfterTrim() {
        assertNull(AzureBlobStore.initAccessTier("   "));
    }

    public void testInitAccessTierThrowsForUnknownValue() {
        for (String input : List.of("archive", "Archive", "premium", "P4", "invalid")) {
            BlobStoreException ex = expectThrows(BlobStoreException.class, () -> AzureBlobStore.initAccessTier(input));
            assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString(input));
        }
    }

    // --- resolveAccessTier ---

    public void testResolveAccessTierReturnsDataTierForSnapshotData() {
        final AzureBlobStore store = newBlobStore("hot", "cool");
        assertEquals(Optional.of(AccessTier.HOT), store.resolveAccessTier(OperationPurpose.SNAPSHOT_DATA));
    }

    public void testResolveAccessTierReturnsMetadataTierForSnapshotMetadata() {
        final AzureBlobStore store = newBlobStore("hot", "cool");
        assertEquals(Optional.of(AccessTier.COOL), store.resolveAccessTier(OperationPurpose.SNAPSHOT_METADATA));
    }

    public void testResolveAccessTierReturnsEmptyWhenDataTierNotConfigured() {
        final AzureBlobStore store = newBlobStore(null, "cool");
        assertEquals(Optional.empty(), store.resolveAccessTier(OperationPurpose.SNAPSHOT_DATA));
    }

    public void testResolveAccessTierReturnsEmptyWhenMetadataTierNotConfigured() {
        final AzureBlobStore store = newBlobStore("hot", null);
        assertEquals(Optional.empty(), store.resolveAccessTier(OperationPurpose.SNAPSHOT_METADATA));
    }

    public void testResolveAccessTierReturnsEmptyForAllNonSnapshotPurposes() {
        final AzureBlobStore store = newBlobStore("hot", "cool");
        for (OperationPurpose purpose : OperationPurpose.values()) {
            if (purpose == OperationPurpose.SNAPSHOT_DATA || purpose == OperationPurpose.SNAPSHOT_METADATA) {
                continue;
            }
            assertEquals("Expected empty tier for purpose " + purpose, Optional.empty(), store.resolveAccessTier(purpose));
        }
    }

    private static AzureBlobStore newBlobStore(@Nullable String dataAccessTier, @Nullable String metadataAccessTier) {
        final AzureStorageService service = mock(AzureStorageService.class);
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "test",
            AzureRepository.TYPE,
            Settings.builder().put(AzureRepository.Repository.CONTAINER_SETTING.getKey(), "test-container").build()
        );
        return new AzureBlobStore(
            ProjectId.DEFAULT,
            metadata,
            service,
            BigArrays.NON_RECYCLING_INSTANCE,
            RepositoriesMetrics.NOOP,
            dataAccessTier,
            metadataAccessTier
        );
    }
}
