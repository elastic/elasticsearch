/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING;

@ESTestCase.WithoutSecurityManager
@SuppressForbidden(reason = "manipulates system properties for testing")
public class IndexSettingsOverrideTests extends ESTestCase {

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        return IndexMetadata.builder(name)
            .settings(indexSettings(IndexVersion.current(), randomIntBetween(1, 3), randomIntBetween(1, 3)).put(indexSettings))
            .build();
    }

    @BeforeClass
    public static void setSystemProperty() {
        System.setProperty(IndexSettings.RefreshIntervalValidator.STATELESS_ALLOW_INDEX_REFRESH_INTERVAL_OVERRIDE, "true");
    }

    public void testStatelessMinRefreshIntervalOverride() {
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

    @AfterClass
    public static void clearSystemProperty() {
        System.clearProperty(IndexSettings.RefreshIntervalValidator.STATELESS_ALLOW_INDEX_REFRESH_INTERVAL_OVERRIDE);
    }
}
