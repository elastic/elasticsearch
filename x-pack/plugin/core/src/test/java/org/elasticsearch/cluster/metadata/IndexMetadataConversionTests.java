/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.elasticsearch.test.VersionUtils.randomIndexCompatibleVersion;
import static org.hamcrest.Matchers.equalTo;

public class IndexMetadataConversionTests extends ESTestCase {

    public void testConvertSearchableSnapshotSettings() {
        IndexMetadata src = newIndexMeta("foo", Settings.EMPTY);
        IndexMetadata indexMetadata = IndexMetadataVerifier.convertSharedCacheTierPreference(src);
        assertSame(indexMetadata, src);

        // A full_copy searchable snapshot (settings should be untouched)
        src = newIndexMeta(
            "foo",
            Settings.builder()
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE)
                .put(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.getKey(), false)
                .put("index.routing.allocation.include._tier", "data_hot")
                .put("index.routing.allocation.exclude._tier", "data_warm")
                .put("index.routing.allocation.require._tier", "data_hot")
                .put("index.routing.allocation.include._tier_preference", "data_cold")
                .build()
        );
        indexMetadata = IndexMetadataVerifier.convertSharedCacheTierPreference(src);
        assertSame(indexMetadata, src);

        // A shared_cache searchable snapshot with valid settings (metadata should be untouched)
        src = newIndexMeta(
            "foo",
            Settings.builder()
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE)
                .put(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.getKey(), false)
                .put("index.routing.allocation.include._tier_preference", "data_frozen")
                .build()
        );
        indexMetadata = IndexMetadataVerifier.convertSharedCacheTierPreference(src);
        assertSame(indexMetadata, src);

        // A shared_cache searchable snapshot (should have its settings converted)
        src = newIndexMeta(
            "foo",
            Settings.builder()
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE)
                .put(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.getKey(), true)
                .put("index.routing.allocation.include._tier", "data_hot")
                .put("index.routing.allocation.exclude._tier", "data_warm")
                .put("index.routing.allocation.require._tier", "data_hot")
                .put("index.routing.allocation.include._tier_preference", "data_frozen,data_cold")
                .build()
        );
        indexMetadata = IndexMetadataVerifier.convertSharedCacheTierPreference(src);
        assertNotSame(indexMetadata, src);
        Settings newSettings = indexMetadata.getSettings();
        assertNull(newSettings.get("index.routing.allocation.include._tier"));
        assertNull(newSettings.get("index.routing.allocation.exclude._tier"));
        assertNull(newSettings.get("index.routing.allocation.require._tier"));
        assertThat(newSettings.get("index.routing.allocation.include._tier_preference"), equalTo("data_frozen"));
    }

    public void testRemoveSingleTierAllocationFilters() {
        IndexMetadata src = newIndexMeta(
            "foo",
            Settings.builder()
                .put("index.routing.allocation.include._tier", "data_hot")
                .put("index.routing.allocation.exclude._tier", "data_warm")
                .put("index.routing.allocation.require._tier", "data_hot")
                .put("index.routing.allocation.include._tier_preference", "data_cold")
                .build()
        );
        IndexMetadata indexMetadata = IndexMetadataVerifier.removeTierFiltering(src);
        assertNotSame(indexMetadata, src);

        Settings newSettings = indexMetadata.getSettings();
        assertNull(newSettings.get("index.routing.allocation.include._tier"));
        assertNull(newSettings.get("index.routing.allocation.exclude._tier"));
        assertNull(newSettings.get("index.routing.allocation.require._tier"));
        assertThat(newSettings.get("index.routing.allocation.include._tier_preference"), equalTo("data_cold"));

        src = newIndexMeta(
            "foo",
            Settings.builder()
                .put("index.routing.allocation.include._tier_preference", "data_cold")
                .put("index.number_of_shards", randomIntBetween(1, 10))
                .build()
        );
        indexMetadata = IndexMetadataVerifier.removeTierFiltering(src);
        assertSame(indexMetadata, src);
    }

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        final Settings settings = indexSettings(randomIndexCompatibleVersion(random()), between(1, 5), between(0, 5)).put(
            IndexMetadata.SETTING_CREATION_DATE,
            randomNonNegativeLong()
        ).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random())).put(indexSettings).build();
        final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(name).settings(settings);
        if (randomBoolean()) {
            indexMetadataBuilder.state(IndexMetadata.State.CLOSE);
        }
        return indexMetadataBuilder.build();
    }
}
