/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.sliceindexing;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.plugins.Plugin;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

/**
 * Test-only plugin that provides the {@link IndexSettings#SLICE_VALIDATED} setting for clusters
 * that use {@link IndexSettings#SLICE_ENABLED} without the DiskBBQ x-pack plugin.
 * Mirrors the logic in {@code ESIntegTestCase.AlwaysValidateSlicePlugin}.
 * <p>
 * When DiskBBQ is present it registers its own {@code SliceIndexingValidationProvider}, so this
 * plugin skips registration to avoid a duplicate-setting conflict.
 */
public class SliceIndexingValidationPlugin extends Plugin {

    private static final boolean DISK_BBQ_PRESENT = diskBbqPresent();

    private static boolean diskBbqPresent() {
        try {
            Class.forName("org.elasticsearch.xpack.diskbbq.DiskBBQPlugin");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        if (DISK_BBQ_PRESENT) {
            return List.of();
        }
        return List.of(new SliceIndexingValidationProvider());
    }

    private static final class SliceIndexingValidationProvider implements IndexSettingProvider {
        @Override
        public void provideAdditionalSettings(
            String indexName,
            String dataStreamName,
            IndexMode templateIndexMode,
            ProjectMetadata projectMetadata,
            Instant resolvedAt,
            Settings indexTemplateAndCreateRequestSettings,
            List<CompressedXContent> combinedTemplateMappings,
            IndexVersion indexVersion,
            Settings.Builder additionalSettings
        ) {
            if (IndexSettings.SLICE_ENABLED.get(indexTemplateAndCreateRequestSettings)) {
                additionalSettings.put(IndexSettings.SLICE_VALIDATED.getKey(), "true");
            }
        }
    }
}
