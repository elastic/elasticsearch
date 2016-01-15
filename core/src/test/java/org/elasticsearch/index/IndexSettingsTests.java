/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class IndexSettingsTests extends ESTestCase {


    public void testRunListener() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version).put(IndexMetaData.SETTING_INDEX_UUID, "0xdeadbeef").build();
        final AtomicInteger integer = new AtomicInteger(0);
        Setting<Integer> integerSetting = Setting.intSetting("index.test.setting.int", -1, true, Setting.Scope.INDEX);
        IndexMetaData metaData = newIndexMeta("index", theSettings);
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        settings.addSetting(integerSetting);
        settings.addSettingsUpdateConsumer(integerSetting, integer::set);

        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("0xdeadbeef", settings.getUUID());

        assertFalse(settings.updateIndexMetaData(metaData));
        assertEquals(metaData.getSettings().getAsMap(), settings.getSettings().getAsMap());
        assertEquals(0, integer.get());
        assertTrue(settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(theSettings).put("index.test.setting.int", 42).build())));
        assertEquals(42, integer.get());
    }

    public void testMergedSettingsArePassed() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version)
                .put(IndexMetaData.SETTING_INDEX_UUID, "0xdeadbeef").build();
        final AtomicInteger integer = new AtomicInteger(0);
        final StringBuilder builder = new StringBuilder();
        Setting<Integer> integerSetting = Setting.intSetting("index.test.setting.int", -1, true, Setting.Scope.INDEX);
        Setting<String> notUpdated = new Setting<>("index.not.updated", "", Function.identity(), true, Setting.Scope.INDEX);

        IndexSettings settings = new IndexSettings(newIndexMeta("index", theSettings), Settings.EMPTY);
        settings.addSetting(integerSetting);
        settings.addSetting(notUpdated);
        settings.addSettingsUpdateConsumer(integerSetting, integer::set);
        settings.addSettingsUpdateConsumer(notUpdated, builder::append);
        assertEquals(0, integer.get());
        assertEquals("", builder.toString());
        IndexMetaData newMetaData = newIndexMeta("index", Settings.builder().put(settings.getIndexMetaData().getSettings()).put("index.test.setting.int", 42).build());
        assertTrue(settings.updateIndexMetaData(newMetaData));
        assertSame(settings.getIndexMetaData(), newMetaData);
        assertEquals(42, integer.get());
        assertEquals("", builder.toString());
        integer.set(0);
        assertTrue(settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(settings.getIndexMetaData().getSettings()).put("index.not.updated", "boom").build())));
        assertEquals("boom", builder.toString());
        assertEquals("not updated - we preserve the old settings", 0, integer.get());

    }

    public void testSettingsConsistency() {
        Version version = VersionUtils.getPreviousVersion();
        IndexMetaData metaData = newIndexMeta("index", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("_na_", settings.getUUID());
        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put("index.test.setting.int", 42).build()));
            fail("version has changed");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().startsWith("version mismatch on settings update expected: "));
        }

        metaData = newIndexMeta("index", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetaData.SETTING_INDEX_UUID, "0xdeadbeef").build());
        settings = new IndexSettings(metaData, Settings.EMPTY);
        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put("index.test.setting.int", 42).build()));
            fail("uuid missing/change");
        } catch (IllegalArgumentException ex) {
            assertEquals("uuid mismatch on settings update expected: 0xdeadbeef but was: _na_", ex.getMessage());
        }
        assertEquals(metaData.getSettings().getAsMap(), settings.getSettings().getAsMap());
    }


    public void testNodeSettingsAreContained() {
        final int numShards = randomIntBetween(1, 10);
        final int numReplicas = randomIntBetween(0, 10);
        Settings theSettings = Settings.settingsBuilder().
                put("index.foo.bar", 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards).build();

        Settings nodeSettings = Settings.settingsBuilder().put("index.foo.bar", 43).build();
        final AtomicInteger indexValue = new AtomicInteger(0);
        Setting<Integer> integerSetting = Setting.intSetting("index.foo.bar", -1, true, Setting.Scope.INDEX);
        IndexSettings settings = new IndexSettings(newIndexMeta("index", theSettings), nodeSettings);
        settings.addSetting(integerSetting);
        settings.addSettingsUpdateConsumer(integerSetting, indexValue::set);
        assertEquals(numReplicas, settings.getNumberOfReplicas());
        assertEquals(numShards, settings.getNumberOfShards());
        assertEquals(0, indexValue.get());

        assertTrue(settings.updateIndexMetaData(newIndexMeta("index", Settings.settingsBuilder().
                put("index.foo.bar", 42)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numReplicas + 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards).build())));

        assertEquals(42, indexValue.get());
        assertSame(nodeSettings, settings.getNodeSettings());

        assertTrue(settings.updateIndexMetaData(newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numReplicas + 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards).build())));
        assertEquals(43, indexValue.get());

    }

    public static IndexMetaData newIndexMeta(String name, Settings indexSettings) {
        Settings build = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(indexSettings)
                .build();
        IndexMetaData metaData = IndexMetaData.builder(name).settings(build).build();
        return metaData;
    }


    public void testUpdateDurability() {
        IndexMetaData metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), "async")
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertEquals(Translog.Durability.ASYNC, settings.getTranslogDurability());
        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), "request").build()));
        assertEquals(Translog.Durability.REQUEST, settings.getTranslogDurability());

        metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build());
        settings = new IndexSettings(metaData, Settings.EMPTY);
        assertEquals(Translog.Durability.REQUEST, settings.getTranslogDurability()); // test default
    }

    public void testIsWarmerEnabled() {
        IndexMetaData metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_WARMER_ENABLED_SETTING.getKey(), false)
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertFalse(settings.isWarmerEnabled());
        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_WARMER_ENABLED_SETTING.getKey(), "true").build()));
        assertTrue(settings.isWarmerEnabled());
        metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build());
        settings = new IndexSettings(metaData, Settings.EMPTY);
        assertTrue(settings.isWarmerEnabled());
    }

    public void testRefreshInterval() {
        String refreshInterval = getRandomTimeString();
        IndexMetaData metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval)
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertEquals(TimeValue.parseTimeValue(refreshInterval, new TimeValue(1, TimeUnit.DAYS), IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()), settings.getRefreshInterval());
        String newRefreshInterval = getRandomTimeString();
        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), newRefreshInterval).build()));
        assertEquals(TimeValue.parseTimeValue(newRefreshInterval, new TimeValue(1, TimeUnit.DAYS), IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()), settings.getRefreshInterval());
    }

    private String getRandomTimeString() {
        int refreshIntervalInt= randomFrom(-1, Math.abs(randomInt()));
        String refreshInterval =  Integer.toString(refreshIntervalInt);
        if (refreshIntervalInt >= 0) {
            refreshInterval += randomFrom("s", "ms", "h");
        }
        return refreshInterval;
    }

    public void testMaxResultWindow() {
        IndexMetaData metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), 15)
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertEquals(15, settings.getMaxResultWindow());
        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), 42).build()));
        assertEquals(42, settings.getMaxResultWindow());
        settings.updateIndexMetaData(newIndexMeta("index", Settings.EMPTY));
        assertEquals(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxResultWindow());

        metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build());
        settings = new IndexSettings(metaData, Settings.EMPTY);
        assertEquals(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxResultWindow());
    }

    public void testGCDeletesSetting() {
        TimeValue gcDeleteSetting = new TimeValue(Math.abs(randomInt()), TimeUnit.MILLISECONDS);
        IndexMetaData metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), gcDeleteSetting.getStringRep())
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertEquals(TimeValue.parseTimeValue(gcDeleteSetting.getStringRep(), new TimeValue(1, TimeUnit.DAYS), IndexSettings.INDEX_GC_DELETES_SETTING.getKey()).getMillis(), settings.getGcDeletesInMillis());
        TimeValue newGCDeleteSetting = new TimeValue(Math.abs(randomInt()), TimeUnit.MILLISECONDS);
        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), newGCDeleteSetting.getStringRep()).build()));
        assertEquals(TimeValue.parseTimeValue(newGCDeleteSetting.getStringRep(), new TimeValue(1, TimeUnit.DAYS), IndexSettings.INDEX_GC_DELETES_SETTING.getKey()).getMillis(), settings.getGcDeletesInMillis());

        try {
            settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), new TimeValue(-1, TimeUnit.MILLISECONDS)).build()));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testIsTTLPurgeDisabled() {
        IndexMetaData metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_TTL_DISABLE_PURGE_SETTING.getKey(), false)
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertFalse(settings.isTTLPurgeDisabled());
        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_TTL_DISABLE_PURGE_SETTING.getKey(), "true").build()));
        assertTrue(settings.isTTLPurgeDisabled());

        settings.updateIndexMetaData(newIndexMeta("index", Settings.EMPTY));
        assertFalse("reset to default", settings.isTTLPurgeDisabled());

        metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build());
        settings = new IndexSettings(metaData, Settings.EMPTY);
        assertFalse(settings.isTTLPurgeDisabled());
    }

    public void testTranslogFlushSizeThreshold() {
        ByteSizeValue translogFlushThresholdSize = new ByteSizeValue(Math.abs(randomInt()));
        ByteSizeValue actualValue = ByteSizeValue.parseBytesSizeValue(translogFlushThresholdSize.toString(), IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTTING.getKey());
        IndexMetaData metaData = newIndexMeta("index", Settings.settingsBuilder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTTING.getKey(), translogFlushThresholdSize.toString())
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertEquals(actualValue, settings.getFlushThresholdSize());
        ByteSizeValue newTranslogFlushThresholdSize = new ByteSizeValue(Math.abs(randomInt()));
        ByteSizeValue actualNewTranslogFlushThresholdSize = ByteSizeValue.parseBytesSizeValue(newTranslogFlushThresholdSize.toString(), IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTTING.getKey());
        settings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTTING.getKey(), newTranslogFlushThresholdSize.toString()).build()));
        assertEquals(actualNewTranslogFlushThresholdSize, settings.getFlushThresholdSize());
    }
}
