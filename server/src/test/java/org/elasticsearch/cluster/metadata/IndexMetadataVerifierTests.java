/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.Collections;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.test.index.IndexVersionUtils.getPreviousVersion;
import static org.elasticsearch.test.index.IndexVersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.equalTo;

public class IndexMetadataVerifierTests extends ESTestCase {

    public void testArchiveBrokenIndexSettings() {
        IndexMetadataVerifier service = getIndexMetadataVerifier();
        IndexMetadata src = newIndexMeta("foo", Settings.EMPTY);
        IndexMetadata indexMetadata = service.archiveOrDeleteBrokenIndexSettings(src);
        assertSame(indexMetadata, src);

        src = newIndexMeta("foo", Settings.builder().put("index.refresh_interval", "-200").build());
        indexMetadata = service.archiveOrDeleteBrokenIndexSettings(src);
        assertNotSame(indexMetadata, src);
        assertEquals("-200", indexMetadata.getSettings().get("archived.index.refresh_interval"));

        src = newIndexMeta("foo", Settings.builder().put("index.codec", "best_compression1").build());
        indexMetadata = service.archiveOrDeleteBrokenIndexSettings(src);
        assertNotSame(indexMetadata, src);
        assertEquals("best_compression1", indexMetadata.getSettings().get("archived.index.codec"));

        src = newIndexMeta("foo", Settings.builder().put("index.refresh.interval", "-1").build());
        indexMetadata = service.archiveOrDeleteBrokenIndexSettings(src);
        assertNotSame(indexMetadata, src);
        assertEquals("-1", indexMetadata.getSettings().get("archived.index.refresh.interval"));

        src = newIndexMeta("foo", indexMetadata.getSettings()); // double archive?
        indexMetadata = service.archiveOrDeleteBrokenIndexSettings(src);
        assertSame(indexMetadata, src);
    }

    public void testDeleteBrokenSystemIndexSettings() {
        IndexMetadataVerifier service = getIndexMetadataVerifier();
        IndexMetadata src = newSystemIndexMeta("foo", Settings.EMPTY);
        IndexMetadata indexMetadata = service.archiveOrDeleteBrokenIndexSettings(src);
        assertSame(indexMetadata, src);

        src = newSystemIndexMeta("foo", Settings.builder().put("index.refresh_interval", "-200").build());
        indexMetadata = service.archiveOrDeleteBrokenIndexSettings(src);
        assertNotSame(indexMetadata, src);
        assertNull(indexMetadata.getSettings().get("archived.index.refresh_interval"));
        assertNull(indexMetadata.getSettings().get("index.refresh_interval"));

        // previously archived settings are removed
        src = newSystemIndexMeta("foo", Settings.builder().put("archived.index.refresh_interval", "200").build());
        indexMetadata = service.archiveOrDeleteBrokenIndexSettings(src);
        assertNotSame(indexMetadata, src);
        assertNull(indexMetadata.getSettings().get("archived.index.refresh_interval"));

        src = newSystemIndexMeta("foo", Settings.builder().put("index.codec", "best_compression1").build());
        indexMetadata = service.archiveOrDeleteBrokenIndexSettings(src);
        assertNotSame(indexMetadata, src);
        assertNull(indexMetadata.getSettings().get("archived.index.codec"));
        assertNull(indexMetadata.getSettings().get("index.codec"));

        src = newSystemIndexMeta("foo", Settings.builder().put("index.refresh.interval", "-1").build());
        indexMetadata = service.archiveOrDeleteBrokenIndexSettings(src);
        assertNotSame(indexMetadata, src);
        assertNull(indexMetadata.getSettings().get("archived.index.refresh.interval"));
        assertNull(indexMetadata.getSettings().get("index.refresh.interval"));

        src = newSystemIndexMeta("foo", indexMetadata.getSettings()); // double archive?
        indexMetadata = service.archiveOrDeleteBrokenIndexSettings(src);
        assertSame(indexMetadata, src);
    }

    public void testCustomSimilarity() {
        IndexMetadataVerifier service = getIndexMetadataVerifier();
        IndexMetadata src = newIndexMeta(
            "foo",
            Settings.builder()
                .put("index.similarity.my_similarity.type", "DFR")
                .put("index.similarity.my_similarity.after_effect", "l")
                .build()
        );
        // The random IndexMetadata.SETTING_VERSION_CREATED in IndexMetadata can be as low as MINIMUM_READONLY_COMPATIBLE
        service.verifyIndexMetadata(src, IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersions.MINIMUM_READONLY_COMPATIBLE);
    }

    public void testIncompatibleVersion() {
        IndexMetadataVerifier service = getIndexMetadataVerifier();
        IndexVersion minCompat = IndexVersions.MINIMUM_READONLY_COMPATIBLE;
        IndexVersion indexCreated = IndexVersion.fromId(randomIntBetween(1000099, minCompat.id() - 1));
        final IndexMetadata metadata = newIndexMeta(
            "foo",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexCreated).build()
        );
        String message = expectThrows(
            IllegalStateException.class,
            () -> service.verifyIndexMetadata(metadata, IndexVersions.MINIMUM_COMPATIBLE, IndexVersions.MINIMUM_READONLY_COMPATIBLE)
        ).getMessage();
        assertThat(
            message,
            equalTo(
                "The index [foo/"
                    + metadata.getIndexUUID()
                    + "] has current compatibility version ["
                    + indexCreated.toReleaseVersion()
                    + "] "
                    + "but the minimum compatible version is ["
                    + IndexVersions.MINIMUM_COMPATIBLE.toReleaseVersion()
                    + "]. It should be re-indexed in Elasticsearch "
                    + (Version.CURRENT.major - 1)
                    + ".x before upgrading to "
                    + Build.current().version()
                    + "."
            )
        );

        indexCreated = randomVersionBetween(random(), IndexVersions.MINIMUM_COMPATIBLE, IndexVersion.current());
        IndexMetadata goodMeta = newIndexMeta("foo", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexCreated).build());
        service.verifyIndexMetadata(goodMeta, IndexVersions.MINIMUM_COMPATIBLE, IndexVersions.MINIMUM_READONLY_COMPATIBLE);
    }

    public void testReadOnlyVersionCompatibility() {
        var service = getIndexMetadataVerifier();
        var randomBlock = randomFrom(IndexMetadata.SETTING_BLOCKS_WRITE, IndexMetadata.SETTING_READ_ONLY);
        {
            var idxMetadata = newIndexMeta(
                "legacy",
                Settings.builder()
                    .put(randomBlock, randomBoolean())
                    .put(MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey(), randomBoolean())
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.fromId(6080099))
                    .build()
            );
            String message = expectThrows(
                IllegalStateException.class,
                () -> service.verifyIndexMetadata(idxMetadata, IndexVersions.MINIMUM_COMPATIBLE, IndexVersions.MINIMUM_READONLY_COMPATIBLE)
            ).getMessage();
            assertThat(
                message,
                equalTo(
                    "The index [legacy/"
                        + idxMetadata.getIndexUUID()
                        + "] has current compatibility version [6.8.0] but the minimum compatible version is ["
                        + IndexVersions.MINIMUM_COMPATIBLE.toReleaseVersion()
                        + "]. It should be re-indexed in Elasticsearch "
                        + (Version.CURRENT.major - 1)
                        + ".x before upgrading to "
                        + Build.current().version()
                        + "."
                )
            );
        }
        var indexCreated = randomVersionBetween(
            random(),
            IndexVersions.MINIMUM_READONLY_COMPATIBLE,
            getPreviousVersion(IndexVersions.MINIMUM_COMPATIBLE)
        );
        {
            var idxMetadata = newIndexMeta(
                "regular",
                Settings.builder()
                    .put(randomBlock, true)
                    .put(MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey(), true)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, indexCreated)
                    .build()
            );
            service.verifyIndexMetadata(idxMetadata, IndexVersions.MINIMUM_COMPATIBLE, IndexVersions.MINIMUM_READONLY_COMPATIBLE);
        }
        {
            var settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexCreated);
            if (randomBoolean()) {
                settings.put(MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey(), randomBoolean());
            }
            if (randomBoolean()) {
                settings.put(randomBlock, false);
            }

            var idxMetadata = newIndexMeta("regular-no-write-block", settings.build());
            String message = expectThrows(
                IllegalStateException.class,
                () -> service.verifyIndexMetadata(idxMetadata, IndexVersions.MINIMUM_COMPATIBLE, IndexVersions.MINIMUM_READONLY_COMPATIBLE)
            ).getMessage();
            assertThat(
                message,
                equalTo(
                    "The index [regular-no-write-block/"
                        + idxMetadata.getIndexUUID()
                        + "] created in version ["
                        + indexCreated.toReleaseVersion()
                        + "] with current compatibility version ["
                        + indexCreated.toReleaseVersion()
                        + "] must be marked as read-only using the setting [index.blocks.write] set to [true] before upgrading to "
                        + Build.current().version()
                        + "."
                )
            );
        }
        {
            var settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexCreated);
            if (randomBoolean()) {
                settings.put(MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey(), false);
            }
            if (randomBoolean()) {
                settings.put(randomBlock, randomBoolean());
            }

            var idxMetadata = newIndexMeta("regular-not-read-only-verified", settings.build());
            String message = expectThrows(
                IllegalStateException.class,
                () -> service.verifyIndexMetadata(idxMetadata, IndexVersions.MINIMUM_COMPATIBLE, IndexVersions.MINIMUM_READONLY_COMPATIBLE)
            ).getMessage();
            assertThat(
                message,
                equalTo(
                    "The index [regular-not-read-only-verified/"
                        + idxMetadata.getIndexUUID()
                        + "] created in version ["
                        + indexCreated.toReleaseVersion()
                        + "] with current compatibility version ["
                        + indexCreated.toReleaseVersion()
                        + "] must be marked as read-only using the setting [index.blocks.write] set to [true] before upgrading to "
                        + Build.current().version()
                        + "."
                )
            );
        }
        {
            var idxMetadata = newIndexMeta(
                "searchable-snapshot",
                Settings.builder()
                    .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, indexCreated)
                    .put(INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE)
                    .build()
            );
            service.verifyIndexMetadata(idxMetadata, IndexVersions.MINIMUM_COMPATIBLE, IndexVersions.MINIMUM_READONLY_COMPATIBLE);
        }
        {
            var idxMetadata = newIndexMeta(
                "archive",
                Settings.builder()
                    .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.fromId(6080099))
                    .put(IndexMetadata.SETTING_VERSION_COMPATIBILITY, indexCreated)
                    .build()
            );
            service.verifyIndexMetadata(idxMetadata, IndexVersions.MINIMUM_COMPATIBLE, IndexVersions.MINIMUM_READONLY_COMPATIBLE);
        }
        {
            var idxMetadata = newIndexMeta(
                "archive-no-write-block",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.fromId(6080099))
                    .put(IndexMetadata.SETTING_VERSION_COMPATIBILITY, indexCreated)
                    .build()
            );
            String message = expectThrows(
                IllegalStateException.class,
                () -> service.verifyIndexMetadata(idxMetadata, IndexVersions.MINIMUM_COMPATIBLE, IndexVersions.MINIMUM_READONLY_COMPATIBLE)
            ).getMessage();
            assertThat(
                message,
                equalTo(
                    "The index [archive-no-write-block/"
                        + idxMetadata.getIndexUUID()
                        + "] created in version [6.8.0] with current compatibility version ["
                        + indexCreated.toReleaseVersion()
                        + "] must be marked as read-only using the setting [index.blocks.write] set to [true] before upgrading to "
                        + Build.current().version()
                        + "."
                )
            );
        }
    }

    private IndexMetadataVerifier getIndexMetadataVerifier() {
        return new IndexMetadataVerifier(
            Settings.EMPTY,
            null,
            xContentRegistry(),
            new MapperRegistry(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), MapperPlugin.NOOP_FIELD_FILTER),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            null,
            MapperMetrics.NOOP
        );
    }

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        return newIndexMetaBuilder(name, indexSettings).build();
    }

    public static IndexMetadata newSystemIndexMeta(String name, Settings indexSettings) {
        return newIndexMetaBuilder(name, indexSettings).system(true).build();
    }

    private static IndexMetadata.Builder newIndexMetaBuilder(String name, Settings indexSettings) {
        final Settings settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(random()), between(1, 5), between(0, 5)).put(
            IndexMetadata.SETTING_CREATION_DATE,
            randomNonNegativeLong()
        ).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random())).put(indexSettings).build();
        final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(name).settings(settings);
        if (randomBoolean()) {
            indexMetadataBuilder.state(IndexMetadata.State.CLOSE);
        }
        return indexMetadataBuilder;
    }

}
