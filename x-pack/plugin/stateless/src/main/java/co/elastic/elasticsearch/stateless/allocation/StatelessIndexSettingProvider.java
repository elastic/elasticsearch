/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.allocation;

import co.elastic.elasticsearch.stateless.Stateless;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class StatelessIndexSettingProvider implements IndexSettingProvider {

    @Override
    public Settings getAdditionalIndexSettings(
        String indexName,
        String dataStreamName,
        boolean timeSeries,
        Metadata metadata,
        Instant resolvedAt,
        Settings allSettings,
        List<CompressedXContent> combinedTemplateMappings
    ) {
        Settings.Builder settings = Settings.builder();
        // TODO find a prover way to bypass index template validation
        if (Objects.equals(indexName, "validate-index-name") == false) {
            settings.put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), Stateless.NAME);
        }
        if (allSettings.hasValue(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()) == false) {
            settings.put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(5));
        }
        return settings.build();
    }
}
