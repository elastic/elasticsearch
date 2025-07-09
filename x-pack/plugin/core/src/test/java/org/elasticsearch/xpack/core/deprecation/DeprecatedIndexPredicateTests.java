/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING;

public class DeprecatedIndexPredicateTests extends ESTestCase {
    public void testReindexIsNotRequiredOnNewIndex() {
        IndexMetadata indexMetadata = Mockito.mock(IndexMetadata.class);
        Mockito.when(indexMetadata.getCreationVersion()).thenReturn(IndexVersion.current());
        Mockito.when(indexMetadata.isSearchableSnapshot()).thenReturn(false);
        Mockito.when(indexMetadata.getSettings()).thenReturn(Settings.builder().build());

        boolean reindexRequired = DeprecatedIndexPredicate.reindexRequired(indexMetadata, false, false);
        assertFalse(reindexRequired);
    }

    public void testReindexIsRequiredOnOldIndex() {
        IndexVersion previousVersion = IndexVersion.getMinimumCompatibleIndexVersion(IndexVersion.current().id());

        IndexMetadata indexMetadata = Mockito.mock(IndexMetadata.class);
        Mockito.when(indexMetadata.getCreationVersion()).thenReturn(previousVersion);
        Mockito.when(indexMetadata.isSystem()).thenReturn(false);
        Mockito.when(indexMetadata.isSearchableSnapshot()).thenReturn(false);
        Mockito.when(indexMetadata.getSettings()).thenReturn(Settings.builder().build());

        boolean reindexRequired = DeprecatedIndexPredicate.reindexRequired(indexMetadata, false, false);
        assertTrue(reindexRequired);
    }

    public void testReindexIsNotRequiredOnSystemIndex() {
        IndexVersion previousVersion = IndexVersion.getMinimumCompatibleIndexVersion(IndexVersion.current().id());

        IndexMetadata indexMetadata = Mockito.mock(IndexMetadata.class);
        Mockito.when(indexMetadata.getCreationVersion()).thenReturn(previousVersion);
        Mockito.when(indexMetadata.isSystem()).thenReturn(true);
        Mockito.when(indexMetadata.isSearchableSnapshot()).thenReturn(false);
        Mockito.when(indexMetadata.getSettings()).thenReturn(Settings.builder().build());

        boolean reindexRequired = DeprecatedIndexPredicate.reindexRequired(indexMetadata, false, false);
        assertFalse(reindexRequired);
    }

    public void testReindexIsRequiredOnSystemIndexWhenExplicitlyIncluded() {
        IndexVersion previousVersion = IndexVersion.getMinimumCompatibleIndexVersion(IndexVersion.current().id());

        IndexMetadata indexMetadata = Mockito.mock(IndexMetadata.class);
        Mockito.when(indexMetadata.getCreationVersion()).thenReturn(previousVersion);
        Mockito.when(indexMetadata.isSystem()).thenReturn(true);
        Mockito.when(indexMetadata.isSearchableSnapshot()).thenReturn(false);
        Mockito.when(indexMetadata.getSettings()).thenReturn(Settings.builder().build());

        boolean reindexRequired = DeprecatedIndexPredicate.reindexRequired(indexMetadata, false, true);
        assertTrue(reindexRequired);
    }

    public void testReindexIsNotRequiredOnOldSearchableSnapshot() {
        IndexVersion previousVersion = IndexVersion.getMinimumCompatibleIndexVersion(IndexVersion.current().id());

        IndexMetadata indexMetadata = Mockito.mock(IndexMetadata.class);
        Mockito.when(indexMetadata.getCreationVersion()).thenReturn(previousVersion);
        Mockito.when(indexMetadata.isSystem()).thenReturn(false);
        Mockito.when(indexMetadata.isSearchableSnapshot()).thenReturn(true);
        Mockito.when(indexMetadata.getSettings()).thenReturn(Settings.builder().build());

        boolean reindexRequired = DeprecatedIndexPredicate.reindexRequired(indexMetadata, false, false);
        assertFalse(reindexRequired);
    }

    public void testReindexIsNotRequiredOnBlockedIndex() {
        IndexVersion previousVersion = IndexVersion.getMinimumCompatibleIndexVersion(IndexVersion.current().id());

        IndexMetadata indexMetadata = Mockito.mock(IndexMetadata.class);
        Mockito.when(indexMetadata.getCreationVersion()).thenReturn(previousVersion);
        Mockito.when(indexMetadata.isSystem()).thenReturn(false);
        Mockito.when(indexMetadata.isSearchableSnapshot()).thenReturn(false);
        Mockito.when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(VERIFIED_READ_ONLY_SETTING.getKey(), true).build());

        boolean reindexRequired = DeprecatedIndexPredicate.reindexRequired(indexMetadata, false, false);
        assertFalse(reindexRequired);
    }

    public void testReindexIsRequiredOnBlockedIndexWhenExplicitlyIncluded() {
        IndexVersion previousVersion = IndexVersion.getMinimumCompatibleIndexVersion(IndexVersion.current().id());

        IndexMetadata indexMetadata = Mockito.mock(IndexMetadata.class);
        Mockito.when(indexMetadata.getCreationVersion()).thenReturn(previousVersion);
        Mockito.when(indexMetadata.isSystem()).thenReturn(false);
        Mockito.when(indexMetadata.isSearchableSnapshot()).thenReturn(false);
        Mockito.when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(VERIFIED_READ_ONLY_SETTING.getKey(), true).build());

        boolean reindexRequired = DeprecatedIndexPredicate.reindexRequired(indexMetadata, true, false);
        assertTrue(reindexRequired);
    }
}
