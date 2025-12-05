/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.function.Consumer;

public class SkipperSettingsTests extends ESTestCase {

    public void testSkipperSettingDefaults() {
        assumeTrue("Skipper feature flag is not enabled", IndexSettings.DOC_VALUES_SKIPPER);
        {
            IndexSettings indexSettings = settings(IndexVersion.current(), b -> {});
            assertFalse(indexSettings.useDocValuesSkipper());
        }
        {
            IndexSettings indexSettings = settings(IndexVersions.STANDARD_INDEXES_USE_SKIPPERS, b -> {});
            assertTrue(indexSettings.useDocValuesSkipper());
        }
        {
            IndexSettings indexSettings = settings(IndexVersions.SKIPPER_DEFAULTS_ONLY_ON_TSDB, b -> {});
            assertFalse(indexSettings.useDocValuesSkipper());
        }
    }

    public void testTSDBSkipperSettingDefaults() {
        assumeTrue("Skipper feature flag is not enabled", IndexSettings.DOC_VALUES_SKIPPER);
        {
            IndexSettings indexSettings = settings(IndexVersion.current(), b -> {
                b.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName());
                b.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "path");
            });
            assertTrue(indexSettings.useDocValuesSkipper());
        }
        {
            IndexSettings indexSettings = settings(IndexVersions.SKIPPERS_ENABLED_BY_DEFAULT, b -> {
                b.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName());
                b.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "path");
            });
            assertTrue(indexSettings.useDocValuesSkipper());
        }
        {
            IndexVersion nonSkipperVersion = IndexVersionUtils.randomPreviousCompatibleVersion(
                random(),
                IndexVersions.SKIPPERS_ENABLED_BY_DEFAULT
            );
            IndexSettings indexSettings = settings(nonSkipperVersion, b -> {
                b.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName());
                b.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "path");
            });
            assertFalse(indexSettings.useDocValuesSkipper());
        }
    }

    public void testLogsDBSkipperSettingDefaults() {
        assumeTrue("Skipper feature flag is not enabled", IndexSettings.DOC_VALUES_SKIPPER);
        {
            IndexSettings indexSettings = settings(
                IndexVersion.current(),
                b -> { b.put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()); }
            );
            assertFalse(indexSettings.useDocValuesSkipper());
        }
        {
            IndexSettings indexSettings = settings(IndexVersions.STANDARD_INDEXES_USE_SKIPPERS, b -> {
                b.put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName());
            });
            assertTrue(indexSettings.useDocValuesSkipper());
        }
        {
            IndexSettings indexSettings = settings(IndexVersions.SKIPPER_DEFAULTS_ONLY_ON_TSDB, b -> {
                b.put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName());
            });
            assertFalse(indexSettings.useDocValuesSkipper());
        }
    }

    private static IndexSettings settings(IndexVersion version, Consumer<Settings.Builder> settingsConsumer) {
        Settings.Builder builder = Settings.builder();
        builder.put(IndexMetadata.SETTING_VERSION_CREATED, version);
        builder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        builder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1);
        settingsConsumer.accept(builder);
        return new IndexSettings(IndexMetadata.builder("test").settings(builder.build()).build(), builder.build());
    }
}
