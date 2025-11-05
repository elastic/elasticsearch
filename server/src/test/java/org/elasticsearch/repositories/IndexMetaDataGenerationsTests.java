/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexMetaDataGenerationsTests extends ESTestCase {

    public void testIndexMetaDataGenerations() {
        Map<String, String> identifiers = new HashMap<>();
        Map<IndexId, String> lookupInternal = new HashMap<>();

        int numberOfMetadataIdentifiers = randomIntBetween(5, 10);
        for (int i = 0; i < numberOfMetadataIdentifiers; i++) {
            String indexUUID = generateUUID();
            String metaIdentifier = generateMetaIdentifier(indexUUID);
            String blobUUID = randomAlphanumericOfLength(randomIntBetween(5, 10));
            identifiers.put(metaIdentifier, blobUUID);

            IndexId indexId = new IndexId(randomAlphanumericOfLength(10), indexUUID);
            lookupInternal.put(indexId, metaIdentifier);
        }

        SnapshotId snapshotId = new SnapshotId(randomAlphanumericOfLength(10), randomUUID());
        Map<SnapshotId, Map<IndexId, String>> lookup = Map.of(snapshotId, lookupInternal);

        IndexMetaDataGenerations generations = new IndexMetaDataGenerations(lookup, identifiers);

        assertEquals(lookup, generations.lookup);
        assertEquals(identifiers, generations.identifiers);
    }

    public void testBuildUniqueIdentifierWithAllFieldsPresent() {
        String indexUUID = generateUUID();
        String historyUUID = randomAlphanumericOfLength(randomIntBetween(10, 64));
        long settingsVersion = randomLong();
        long mappingVersion = randomLong();
        long aliasesVersion = randomLong();

        IndexMetadata indexMetadata = createIndexMetadata(indexUUID, historyUUID, settingsVersion, mappingVersion, aliasesVersion);

        String result = IndexMetaDataGenerations.buildUniqueIdentifier(indexMetadata);
        assertEquals(indexUUID + "-" + historyUUID + "-" + settingsVersion + "-" + mappingVersion + "-" + aliasesVersion, result);
    }

    public void testBuildUniqueIdentifierWithMissingHistoryUUID() {
        String indexUUID = generateUUID();
        long settingsVersion = randomLong();
        long mappingVersion = randomLong();
        long aliasesVersion = randomLong();

        IndexMetadata indexMetadata = createIndexMetadata(indexUUID, null, settingsVersion, mappingVersion, aliasesVersion);

        String result = IndexMetaDataGenerations.buildUniqueIdentifier(indexMetadata);
        assertEquals(indexUUID + "-_na_-" + settingsVersion + "-" + mappingVersion + "-" + aliasesVersion, result);
    }

    public void testGetBlobIdToIndexUuidMap() {
        String indexUUID = generateUUID();
        String randomSetting = randomAlphaOfLength(randomIntBetween(5, 10));
        long settingsVersion = randomNonNegativeLong();
        long mappingsVersion = randomNonNegativeLong();
        long aliasesVersion = randomNonNegativeLong();
        String uniqueIdentifier = indexUUID + "-" + randomSetting + "-" + settingsVersion + "-" + mappingsVersion + "-" + aliasesVersion;
        String blobId = randomAlphanumericOfLength(randomIntBetween(5, 10));

        // Creates the lookup map
        SnapshotId snapshotId = new SnapshotId("snapshot", randomUUID());
        IndexId indexId = new IndexId("index", indexUUID);
        Map<SnapshotId, Map<IndexId, String>> lookup = Map.of(snapshotId, Map.of(indexId, uniqueIdentifier));

        IndexMetaDataGenerations generations = new IndexMetaDataGenerations(lookup, Map.of(uniqueIdentifier, blobId));

        Map<String, String> expectedBlobIdToindexUuidMap = Map.of(blobId, indexUUID);
        assertEquals(expectedBlobIdToindexUuidMap, generations.getBlobIdToIndexUuidMap());
    }

    public void testGetBlobIdToIndexUuidMapWithNoIdentifierMap() {
        IndexMetaDataGenerations generations = new IndexMetaDataGenerations(Map.of(), Map.of());
        assertEquals(Collections.emptyMap(), generations.getBlobIdToIndexUuidMap());
    }

    private String generateUUID() {
        return UUIDs.randomBase64UUID(random());
    }

    private String generateMetaIdentifier(String indexUUID) {
        String historyUUID = generateUUID();
        long settingsVersion = randomLong();
        long mappingVersion = randomLong();
        long aliasesVersion = randomLong();
        return indexUUID + "-" + historyUUID + "-" + settingsVersion + "-" + mappingVersion + "-" + aliasesVersion;
    }

    private IndexMetadata createIndexMetadata(
        String indexUUID,
        String historyUUID,
        long settingsVersion,
        long mappingVersion,
        long aliasesVersion
    ) {
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        Settings.Builder settingsBuilder = Settings.builder();
        if (historyUUID != null) {
            settingsBuilder.put(IndexMetadata.SETTING_HISTORY_UUID, historyUUID);
        }
        when(indexMetadata.getIndexUUID()).thenReturn(indexUUID);
        when(indexMetadata.getSettings()).thenReturn(settingsBuilder.build());
        when(indexMetadata.getSettingsVersion()).thenReturn(settingsVersion);
        when(indexMetadata.getMappingVersion()).thenReturn(mappingVersion);
        when(indexMetadata.getAliasesVersion()).thenReturn(aliasesVersion);
        return indexMetadata;
    }
}
