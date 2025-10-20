/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexMetaDataGenerationsTests extends ESTestCase {

    public void testBuildUniqueIdentifierWithAllFieldsPresent() {
        String indexUUID = randomAlphanumericOfLength(randomIntBetween(10, 64));
        String historyUUID = randomAlphanumericOfLength(randomIntBetween(10, 64));
        long settingsVersion = randomLong();
        long mappingVersion = randomLong();
        long aliasesVersion = randomLong();

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndexUUID()).thenReturn(indexUUID);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(IndexMetadata.SETTING_HISTORY_UUID, historyUUID).build());
        when(indexMetadata.getSettingsVersion()).thenReturn(settingsVersion);
        when(indexMetadata.getMappingVersion()).thenReturn(mappingVersion);
        when(indexMetadata.getAliasesVersion()).thenReturn(aliasesVersion);

        String result = IndexMetaDataGenerations.buildUniqueIdentifier(indexMetadata);
        assertEquals(indexUUID + "-" + historyUUID + "-" + settingsVersion + "-" + mappingVersion + "-" + aliasesVersion, result);
    }

    public void testBuildUniqueIdentifierWithMissingHistoryUUID() {
        String indexUUID = randomAlphanumericOfLength(randomIntBetween(10, 64));
        long settingsVersion = randomLong();
        long mappingVersion = randomLong();
        long aliasesVersion = randomLong();

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndexUUID()).thenReturn(indexUUID);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().build());
        when(indexMetadata.getSettingsVersion()).thenReturn(settingsVersion);
        when(indexMetadata.getMappingVersion()).thenReturn(mappingVersion);
        when(indexMetadata.getAliasesVersion()).thenReturn(aliasesVersion);

        String result = IndexMetaDataGenerations.buildUniqueIdentifier(indexMetadata);
        assertEquals(indexUUID + "-_na_-" + settingsVersion + "-" + mappingVersion + "-" + aliasesVersion, result);
    }

    public void testConvertBlobIdToIndexUUIDReturnsIndexUUID() {
        // May include the - symbol, which as of 9.3.0 is the same symbol as the delimiter
        String indexUUID = randomUUID();
        String randomSetting = randomAlphaOfLength(randomIntBetween(5, 10));
        long settingsVersion = randomNonNegativeLong();
        long mappingsVersion = randomNonNegativeLong();
        long aliasesVersion = randomNonNegativeLong();
        String uniqueIdentifier = indexUUID + "-" + randomSetting + "-" + settingsVersion + "-" + mappingsVersion + "-" + aliasesVersion;
        String blobId = randomAlphanumericOfLength(randomIntBetween(5, 10));

        // Creates the lookup map
        SnapshotId snapshotId = new SnapshotId("snapshot", randomUUID());
        IndexId indexId = new IndexId("index", indexUUID);
        Map<SnapshotId, Map<IndexId, String>> lookup = Map.of(
            snapshotId, Map.of(indexId, uniqueIdentifier)
        );

        IndexMetaDataGenerations generations = new IndexMetaDataGenerations(
            lookup,
            Map.of(uniqueIdentifier, blobId)
        );
        assertEquals(indexUUID, generations.convertBlobIdToIndexUUID(blobId));
    }

    public void testConvertBlobIdToIndexUUIDReturnsNullWhenBlobIdIsNotFound() {
        IndexMetaDataGenerations generations = new IndexMetaDataGenerations(
            Map.of(),
            Map.of()
        );
        assertNull(generations.convertBlobIdToIndexUUID(randomAlphanumericOfLength(randomIntBetween(5, 10))));
    }

    /**
     * A helper function that tests whether an IAE is thrown when a malformed IndexMetadata Identifier is passed into
     * the {@code convertBlobIdToIndexUUID} function
     * @param indexUUID The indexUUID
     * @param uniqueIdentifier The malformed identifier
     * @param blobId The blobId
     */
    private void testMalformedIndexMetadataIdentifierInternalThrowsIAE(String indexUUID, String uniqueIdentifier, String blobId) {
        // Creates the lookup map
        SnapshotId snapshotId = new SnapshotId("snapshot", randomUUID());
        IndexId indexId = new IndexId("index", indexUUID);
        Map<SnapshotId, Map<IndexId, String>> lookup = Map.of(
            snapshotId, Map.of(indexId, uniqueIdentifier)
        );

        IndexMetaDataGenerations malformedGenerations = new IndexMetaDataGenerations(
            lookup,
            Map.of(uniqueIdentifier, blobId)
        );
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> malformedGenerations.convertBlobIdToIndexUUID(blobId)
        );
        assertTrue(ex.getMessage().contains("Error parsing the IndexMetadata identifier"));
    }

    public void testConvertBlobIdToIndexUUIDThrowsIllegalArgumentExceptionWhenMalformedIndexMetadataIdentifierIsMissingIndexUUID() {
        String indexUUID = randomUUID();
        String randomSetting = randomAlphaOfLength(randomIntBetween(5, 10));
        long settingsVersion = randomNonNegativeLong();
        long mappingsVersion = randomNonNegativeLong();
        long aliasesVersion = randomNonNegativeLong();
        // Build the unique identifier without including the index uuid
        String uniqueIdentifier = randomSetting + "-" + settingsVersion + "-" + mappingsVersion + "-" + aliasesVersion;
        String blobId = randomAlphanumericOfLength(randomIntBetween(5, 10));
        testMalformedIndexMetadataIdentifierInternalThrowsIAE(indexUUID, uniqueIdentifier, blobId);
    }

    public void testConvertBlobIdToIndexUUIDThrowsIllegalArgumentExceptionWhenMalformedIndexMetadataIdentifierIsMissingSettings() {
        String indexUUID = randomUUID();
        long settingsVersion = randomNonNegativeLong();
        long mappingsVersion = randomNonNegativeLong();
        long aliasesVersion = randomNonNegativeLong();
        // Build the unique identifier without including the settings
        String uniqueIdentifier = indexUUID + "-" + settingsVersion + "-" + mappingsVersion + "-" + aliasesVersion;
        String blobId = randomAlphanumericOfLength(randomIntBetween(5, 10));
        testMalformedIndexMetadataIdentifierInternalThrowsIAE(indexUUID, uniqueIdentifier, blobId);
    }

    public void testConvertBlobIdToIndexUUIDThrowsIllegalArgumentExceptionWhenMalformedIndexMetadataIdentifierIsMissingSettingsVersion() {
        String indexUUID = randomUUID();
        String randomSetting = randomAlphaOfLength(randomIntBetween(5, 10));
        long mappingsVersion = randomNonNegativeLong();
        long aliasesVersion = randomNonNegativeLong();
        // Build the unique identifier without including the settings version
        String uniqueIdentifier = indexUUID + "-" + randomSetting + "-" + mappingsVersion + "-" + aliasesVersion;
        String blobId = randomAlphanumericOfLength(randomIntBetween(5, 10));
        testMalformedIndexMetadataIdentifierInternalThrowsIAE(indexUUID, uniqueIdentifier, blobId);
    }

    public void testConvertBlobIdToIndexUUIDThrowsIllegalArgumentExceptionWhenMalformedIndexMetadataIdentifierHasIncorrectTypes() {
        String indexUUID = randomUUID();
        String randomSetting = randomAlphaOfLength(randomIntBetween(5, 10));
        long settingsVersion = randomNonNegativeLong();
        long mappingsVersion = randomNonNegativeLong();
        String aliasesVersion = randomAlphaOfLength(randomIntBetween(5, 10));
        String uniqueIdentifier = indexUUID + "-" + randomSetting + "-" + settingsVersion + "-" + mappingsVersion + "-" + aliasesVersion;
        String blobId = randomAlphanumericOfLength(randomIntBetween(5, 10));
        testMalformedIndexMetadataIdentifierInternalThrowsIAE(indexUUID, uniqueIdentifier, blobId);
    }
}
