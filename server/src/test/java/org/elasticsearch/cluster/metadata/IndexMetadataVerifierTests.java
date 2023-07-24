/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.Collections;

import static org.elasticsearch.test.VersionUtils.randomIndexCompatibleVersion;
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
        service.verifyIndexMetadata(src, IndexVersion.MINIMUM_COMPATIBLE);
    }

    public void testIncompatibleVersion() {
        IndexMetadataVerifier service = getIndexMetadataVerifier();
        IndexVersion minCompat = IndexVersion.MINIMUM_COMPATIBLE;
        IndexVersion indexCreated = IndexVersion.fromId(randomIntBetween(1000099, minCompat.id() - 1));
        final IndexMetadata metadata = newIndexMeta(
            "foo",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexCreated.id()).build()
        );
        String message = expectThrows(
            IllegalStateException.class,
            () -> service.verifyIndexMetadata(metadata, IndexVersion.MINIMUM_COMPATIBLE)
        ).getMessage();
        assertThat(
            message,
            equalTo(
                "The index [foo/"
                    + metadata.getIndexUUID()
                    + "] has current compatibility version ["
                    + indexCreated
                    + "] "
                    + "but the minimum compatible version is ["
                    + minCompat
                    + "]."
                    + " It should be re-indexed in Elasticsearch "
                    + (Version.CURRENT.major - 1)
                    + ".x before upgrading to "
                    + Version.CURRENT
                    + "."
            )
        );

        indexCreated = IndexVersionUtils.randomVersionBetween(random(), minCompat, IndexVersion.current());
        IndexMetadata goodMeta = newIndexMeta(
            "foo",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexCreated.id()).build()
        );
        service.verifyIndexMetadata(goodMeta, IndexVersion.MINIMUM_COMPATIBLE);
    }

    private IndexMetadataVerifier getIndexMetadataVerifier() {
        return new IndexMetadataVerifier(
            Settings.EMPTY,
            null,
            xContentRegistry(),
            new MapperRegistry(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), MapperPlugin.NOOP_FIELD_FILTER),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            null
        );
    }

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        return newIndexMetaBuilder(name, indexSettings).build();
    }

    public static IndexMetadata newSystemIndexMeta(String name, Settings indexSettings) {
        return newIndexMetaBuilder(name, indexSettings).system(true).build();
    }

    private static IndexMetadata.Builder newIndexMetaBuilder(String name, Settings indexSettings) {
        final Settings settings = indexSettings(randomIndexCompatibleVersion(random()), between(1, 5), between(0, 5)).put(
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
