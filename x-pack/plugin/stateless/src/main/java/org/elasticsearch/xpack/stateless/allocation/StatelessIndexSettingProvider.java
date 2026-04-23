/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class StatelessIndexSettingProvider implements IndexSettingProvider {

    public StatelessIndexSettingProvider() {}

    @Override
    public void provideAdditionalSettings(
        String indexName,
        @Nullable String dataStreamName,
        IndexMode templateIndexMode,
        ProjectMetadata projectMetadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings,
        IndexVersion indexVersion,
        Settings.Builder additionalSettings
    ) {
        // TODO find a prover way to bypass index template validation
        if (Objects.equals(indexName, "validate-index-name") == false) {
            additionalSettings.put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), StatelessPlugin.NAME);
        }
    }
}
