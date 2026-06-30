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
 * Test-only plugin that validates slice indexing by providing the {@link IndexSettings#SLICE_VALIDATED} setting
 * for clusters that use {@link IndexSettings#SLICE_ENABLED} without the DiskBBQ x-pack plugin.
 * Mirrors the logic in {@code ESIntegTestCase.AlwaysValidateSlicePlugin}.
 */
public class SliceIndexingValidationPlugin extends Plugin {

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
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
