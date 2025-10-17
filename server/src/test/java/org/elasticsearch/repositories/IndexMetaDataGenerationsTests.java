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
import org.elasticsearch.test.ESTestCase;

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
        assertEquals(indexUUID + "/" + historyUUID + "/" + settingsVersion + "/" + mappingVersion + "/" + aliasesVersion, result);

        // Then test parseUUIDFromUniqueIdentifier
        assertEquals(indexUUID, IndexMetaDataGenerations.parseUUIDFromUniqueIdentifier(result));
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
        assertEquals(indexUUID + "/_na_/" + settingsVersion + "/" + mappingVersion + "/" + aliasesVersion, result);
    }

    public void testParseUUIDFromUniqueIdentifierWithNullInput() {
        assertEquals("", IndexMetaDataGenerations.parseUUIDFromUniqueIdentifier(null));
    }

    public void testParseUUIDFromUniqueIdentifierWithEmptyString() {
        assertEquals("", IndexMetaDataGenerations.parseUUIDFromUniqueIdentifier(""));
    }

    public void testParseUUIDFromUniqueIdentifierWithoutDelimiter() {
        String uuid = randomAlphanumericOfLength(randomIntBetween(10, 64));
        assertEquals(uuid, IndexMetaDataGenerations.parseUUIDFromUniqueIdentifier(uuid));
    }
}
