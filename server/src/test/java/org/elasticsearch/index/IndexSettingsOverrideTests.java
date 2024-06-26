/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING;

@ESTestCase.WithoutSecurityManager
public class IndexSettingsOverrideTests extends ESTestCase {

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        return IndexMetadata.builder(name)
            .settings(indexSettings(IndexVersion.current(), randomIntBetween(1, 3), randomIntBetween(1, 3)).put(indexSettings))
            .build();
    }

    public void testStatelessMinRefreshIntervalOverride() {
        System.setProperty(IndexSettings.RefreshIntervalValidator.STATELESS_ALLOW_INDEX_REFRESH_INTERVAL_OVERRIDE, "true");
        try (var mockLog = MockLog.capture(IndexSettings.RefreshIntervalValidator.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "warnings",
                    IndexSettings.RefreshIntervalValidator.class.getCanonicalName(),
                    Level.WARN,
                    "Overriding `index.refresh_interval` setting to 2s"
                )
            );
            IndexMetadata metadata = newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), "stateless")
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "2s")
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersions.V_8_10_0.id() + 1)
                    .build()
            );
            IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
            assertEquals(TimeValue.timeValueSeconds(2), settings.getRefreshInterval());
            mockLog.assertAllExpectationsMatched();
        } finally {
            System.clearProperty(IndexSettings.RefreshIntervalValidator.STATELESS_ALLOW_INDEX_REFRESH_INTERVAL_OVERRIDE);
        }
    }
}
