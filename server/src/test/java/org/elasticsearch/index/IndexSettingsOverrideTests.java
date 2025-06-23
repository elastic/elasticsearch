/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING;

public class IndexSettingsOverrideTests extends ESTestCase {

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        return IndexMetadata.builder(name)
            .settings(indexSettings(IndexVersion.current(), randomIntBetween(1, 3), randomIntBetween(1, 3)).put(indexSettings))
            .build();
    }

    public void testStatelessMinRefreshIntervalOverride() {
        assumeTrue(
            "This test depends on system property configured in build.gradle",
            Boolean.parseBoolean(
                System.getProperty(IndexSettings.RefreshIntervalValidator.STATELESS_ALLOW_INDEX_REFRESH_INTERVAL_OVERRIDE, "false")
            )
        );
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), "stateless")
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
                .put(SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersions.V_8_10_0.id() + 1)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(TimeValue.timeValueSeconds(1), settings.getRefreshInterval());
    }
}
