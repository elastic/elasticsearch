/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.List;

public class StatelessIndexSettingProviderTests extends ESTestCase {

    private static Settings provide(Settings nodeSettings, Settings indexSettings) {
        Settings.Builder out = Settings.builder();
        new StatelessIndexSettingProvider(nodeSettings).provideAdditionalSettings(
            "index",
            null,
            null,
            null,
            Instant.ofEpochMilli(0),
            indexSettings,
            List.of(),
            IndexVersion.current(),
            out
        );
        return out.build();
    }

    public void testEnablesPerFieldFilesAndRegionSizedCompoundByDefault() {
        Settings out = provide(Settings.EMPTY, Settings.EMPTY);
        assertTrue(IndexSettings.INDEX_PER_FIELD_FILES_SETTING.get(out));
        // default region size is 16 MB
        assertEquals(ByteSizeValue.ofMb(16).getStringRep(), out.get(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey()));
    }

    public void testCompoundFormatTracksConfiguredRegionSize() {
        Settings nodeSettings = Settings.builder().put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "32mb").build();
        Settings out = provide(nodeSettings, Settings.EMPTY);
        assertEquals(ByteSizeValue.ofMb(32).getStringRep(), out.get(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey()));
    }

    public void testExplicitPerFieldFilesFalseSuppressesBothDefaults() {
        Settings indexSettings = Settings.builder().put(IndexSettings.INDEX_PER_FIELD_FILES_SETTING.getKey(), false).build();
        Settings out = provide(Settings.EMPTY, indexSettings);
        // we must not re-set per_field_files, and must not lower the compound threshold when it is off
        assertNull(out.get(IndexSettings.INDEX_PER_FIELD_FILES_SETTING.getKey()));
        assertNull(out.get(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey()));
    }

    public void testExplicitCompoundFormatIsKept() {
        Settings indexSettings = Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), "1gb").build();
        Settings out = provide(Settings.EMPTY, indexSettings);
        // per_field_files still defaulted on, but the explicit compound_format is not overridden
        assertTrue(IndexSettings.INDEX_PER_FIELD_FILES_SETTING.get(out));
        assertNull(out.get(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey()));
    }
}
