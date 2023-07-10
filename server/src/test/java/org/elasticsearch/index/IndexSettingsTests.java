/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.elasticsearch.cluster.node.DiscoveryNode.STATELESS_ENABLED_SETTING_NAME;
import static org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING;
import static org.elasticsearch.index.IndexSettings.DEFAULT_REFRESH_INTERVAL;
import static org.elasticsearch.index.IndexSettings.INDEX_FAST_REFRESH_SETTING;
import static org.elasticsearch.index.IndexSettings.STATELESS_DEFAULT_REFRESH_INTERVAL;
import static org.elasticsearch.index.IndexSettings.TIME_SERIES_END_TIME;
import static org.elasticsearch.index.IndexSettings.TIME_SERIES_START_TIME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.object.HasToString.hasToString;

public class IndexSettingsTests extends ESTestCase {

    public void testRunListener() {
        IndexVersion version = IndexVersionUtils.getPreviousVersion();
        Settings theSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version.id())
            .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef")
            .build();
        final AtomicInteger integer = new AtomicInteger(0);
        Setting<Integer> integerSetting = Setting.intSetting("index.test.setting.int", -1, Property.Dynamic, Property.IndexScope);
        IndexMetadata metadata = newIndexMeta("index", theSettings);
        IndexSettings settings = newIndexSettings(newIndexMeta("index", theSettings), Settings.EMPTY, integerSetting);
        settings.getScopedSettings().addSettingsUpdateConsumer(integerSetting, integer::set);

        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("0xdeadbeef", settings.getUUID());

        assertFalse(settings.updateIndexMetadata(metadata));
        assertEquals(metadata.getSettings(), settings.getSettings());
        assertEquals(0, integer.get());
        assertTrue(
            settings.updateIndexMetadata(
                newIndexMeta("index", Settings.builder().put(theSettings).put("index.test.setting.int", 42).build())
            )
        );
        assertEquals(42, integer.get());
    }

    public void testSettingsUpdateValidator() {
        IndexVersion version = IndexVersionUtils.getPreviousVersion();
        Settings theSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version.id())
            .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef")
            .build();
        final AtomicInteger integer = new AtomicInteger(0);
        Setting<Integer> integerSetting = Setting.intSetting("index.test.setting.int", -1, Property.Dynamic, Property.IndexScope);
        IndexMetadata metadata = newIndexMeta("index", theSettings);
        IndexSettings settings = newIndexSettings(newIndexMeta("index", theSettings), Settings.EMPTY, integerSetting);
        settings.getScopedSettings().addSettingsUpdateConsumer(integerSetting, integer::set, (i) -> {
            if (i == 42) throw new AssertionError("boom");
        });

        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("0xdeadbeef", settings.getUUID());

        assertFalse(settings.updateIndexMetadata(metadata));
        assertEquals(metadata.getSettings(), settings.getSettings());
        assertEquals(0, integer.get());
        expectThrows(
            IllegalArgumentException.class,
            () -> settings.updateIndexMetadata(
                newIndexMeta("index", Settings.builder().put(theSettings).put("index.test.setting.int", 42).build())
            )
        );
        assertTrue(
            settings.updateIndexMetadata(
                newIndexMeta("index", Settings.builder().put(theSettings).put("index.test.setting.int", 41).build())
            )
        );
        assertEquals(41, integer.get());
    }

    public void testMergedSettingsArePassed() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef")
            .build();
        final AtomicInteger integer = new AtomicInteger(0);
        final StringBuilder builder = new StringBuilder();
        Setting<Integer> integerSetting = Setting.intSetting("index.test.setting.int", -1, Property.Dynamic, Property.IndexScope);
        Setting<String> notUpdated = new Setting<>("index.not.updated", "", Function.identity(), Property.Dynamic, Property.IndexScope);

        IndexSettings settings = newIndexSettings(newIndexMeta("index", theSettings), Settings.EMPTY, integerSetting, notUpdated);
        settings.getScopedSettings().addSettingsUpdateConsumer(integerSetting, integer::set);
        settings.getScopedSettings().addSettingsUpdateConsumer(notUpdated, builder::append);
        assertEquals(0, integer.get());
        assertEquals("", builder.toString());
        IndexMetadata newMetadata = newIndexMeta(
            "index",
            Settings.builder().put(settings.getIndexMetadata().getSettings()).put("index.test.setting.int", 42).build()
        );
        assertTrue(settings.updateIndexMetadata(newMetadata));
        assertSame(settings.getIndexMetadata(), newMetadata);
        assertEquals(42, integer.get());
        assertEquals("", builder.toString());
        integer.set(0);
        assertTrue(
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder().put(settings.getIndexMetadata().getSettings()).put("index.not.updated", "boom").build()
                )
            )
        );
        assertEquals("boom", builder.toString());
        assertEquals("not updated - we preserve the old settings", 0, integer.get());

    }

    public void testSettingsConsistency() {
        IndexVersion version = IndexVersionUtils.getPreviousVersion();
        IndexMetadata metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version.id()).build());
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("_na_", settings.getUUID());
        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put("index.test.setting.int", 42).build()
                )
            );
            fail("version has changed");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().startsWith("version mismatch on settings update expected: "));
        }

        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, version.id())
                        .put(IndexMetadata.SETTING_VERSION_COMPATIBILITY, IndexVersion.current().id())
                        .put("index.test.setting.int", 42)
                        .build()
                )
            );
            fail("version has changed");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().startsWith("compatibility version mismatch on settings update expected: "));
        }

        // use version number that is unknown
        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.fromId(999999)).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(IndexVersion.fromId(999999), settings.getIndexVersionCreated());
        assertEquals("_na_", settings.getUUID());
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.fromId(999999))
                    .put("index.test.setting.int", 42)
                    .build()
            )
        );

        metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef")
                .build()
        );
        settings = new IndexSettings(metadata, Settings.EMPTY);
        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put("index.test.setting.int", 42).build()
                )
            );
            fail("uuid missing/change");
        } catch (IllegalArgumentException ex) {
            assertEquals("uuid mismatch on settings update expected: 0xdeadbeef but was: _na_", ex.getMessage());
        }
        assertEquals(metadata.getSettings(), settings.getSettings());
    }

    public IndexSettings newIndexSettings(IndexMetadata metadata, Settings nodeSettings, Setting<?>... settings) {
        Set<Setting<?>> settingSet = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        if (settings.length > 0) {
            settingSet.addAll(Arrays.asList(settings));
        }
        return new IndexSettings(metadata, nodeSettings, new IndexScopedSettings(Settings.EMPTY, settingSet));
    }

    public void testNodeSettingsAreContained() {
        final int numShards = randomIntBetween(1, 10);
        final int numReplicas = randomIntBetween(0, 10);
        Settings theSettings = indexSettings(numShards, numReplicas).put("index.foo.bar", 0).build();

        Settings nodeSettings = Settings.builder().put("index.foo.bar", 43).build();
        final AtomicInteger indexValue = new AtomicInteger(0);
        Setting<Integer> integerSetting = Setting.intSetting("index.foo.bar", -1, Property.Dynamic, Property.IndexScope);
        IndexSettings settings = newIndexSettings(newIndexMeta("index", theSettings), nodeSettings, integerSetting);
        settings.getScopedSettings().addSettingsUpdateConsumer(integerSetting, indexValue::set);
        assertEquals(numReplicas, settings.getNumberOfReplicas());
        assertEquals(numShards, settings.getNumberOfShards());
        assertEquals(0, indexValue.get());

        assertTrue(
            settings.updateIndexMetadata(newIndexMeta("index", indexSettings(numShards, numReplicas + 1).put("index.foo.bar", 42).build()))
        );

        assertEquals(42, indexValue.get());
        assertSame(nodeSettings, settings.getNodeSettings());

        assertTrue(settings.updateIndexMetadata(newIndexMeta("index", indexSettings(numShards, numReplicas + 1).build())));
        assertEquals(43, indexValue.get());
    }

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        return IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 1).put(indexSettings)).build();
    }

    public void testUpdateDurability() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), "async")
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(Translog.Durability.ASYNC, settings.getTranslogDurability());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), "request").build())
        );
        assertEquals(Translog.Durability.REQUEST, settings.getTranslogDurability());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(Translog.Durability.REQUEST, settings.getTranslogDurability()); // test default
    }

    public void testIsWarmerEnabled() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_WARMER_ENABLED_SETTING.getKey(), false)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertFalse(settings.isWarmerEnabled());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_WARMER_ENABLED_SETTING.getKey(), "true").build())
        );
        assertTrue(settings.isWarmerEnabled());
        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertTrue(settings.isWarmerEnabled());
    }

    public void testRefreshInterval() {
        String refreshInterval = getRandomTimeString();
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(
            TimeValue.parseTimeValue(
                refreshInterval,
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()
            ),
            settings.getRefreshInterval()
        );
        String newRefreshInterval = getRandomTimeString();
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), newRefreshInterval).build())
        );
        assertEquals(
            TimeValue.parseTimeValue(
                newRefreshInterval,
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()
            ),
            settings.getRefreshInterval()
        );
    }

    public void testDefaultRefreshInterval() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(DEFAULT_REFRESH_INTERVAL, settings.getRefreshInterval());
    }

    public void testStatelessDefaultRefreshInterval() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), "stateless")
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(STATELESS_DEFAULT_REFRESH_INTERVAL, settings.getRefreshInterval());
    }

    public void testStatelessFastRefreshDefaultRefreshInterval() {
        IndexMetadata metadata = IndexMetadata.builder("index")
            .system(true)
            .settings(
                indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), "stateless")
                    .put(INDEX_FAST_REFRESH_SETTING.getKey(), true)
                    .build()
            )
            .build();
        IndexSettings settings = new IndexSettings(metadata, Settings.builder().put(STATELESS_ENABLED_SETTING_NAME, true).build());
        assertEquals(DEFAULT_REFRESH_INTERVAL, settings.getRefreshInterval());
    }

    private String getRandomTimeString() {
        int refreshIntervalInt = randomFrom(-1, Math.abs(randomInt()));
        String refreshInterval = Integer.toString(refreshIntervalInt);
        if (refreshIntervalInt >= 0) {
            refreshInterval += randomFrom("s", "ms", "h");
        }
        return refreshInterval;
    }

    public void testMaxResultWindow() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), 15)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(15, settings.getMaxResultWindow());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), 42).build())
        );
        assertEquals(42, settings.getMaxResultWindow());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxResultWindow());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxResultWindow());
    }

    public void testMaxInnerResultWindow() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), 200)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(200, settings.getMaxInnerResultWindow());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), 50).build())
        );
        assertEquals(50, settings.getMaxInnerResultWindow());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxInnerResultWindow());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxInnerResultWindow());
    }

    public void testMaxDocvalueFields() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey(), 200)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(200, settings.getMaxDocvalueFields());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey(), 50).build())
        );
        assertEquals(50, settings.getMaxDocvalueFields());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxDocvalueFields());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxDocvalueFields());
    }

    public void testMaxScriptFields() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey(), 100)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(100, settings.getMaxScriptFields());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey(), 20).build())
        );
        assertEquals(20, settings.getMaxScriptFields());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxScriptFields());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxScriptFields());
    }

    public void testMaxRegexLengthSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey(), 99)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(99, settings.getMaxRegexLength());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey(), 101).build())
        );
        assertEquals(101, settings.getMaxRegexLength());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(IndexSettings.MAX_REGEX_LENGTH_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxRegexLength());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(IndexSettings.MAX_REGEX_LENGTH_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxRegexLength());
    }

    public void testGCDeletesSetting() {
        TimeValue gcDeleteSetting = new TimeValue(Math.abs(randomInt()), TimeUnit.MILLISECONDS);
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), gcDeleteSetting.getStringRep())
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(
            TimeValue.parseTimeValue(
                gcDeleteSetting.getStringRep(),
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_GC_DELETES_SETTING.getKey()
            ).getMillis(),
            settings.getGcDeletesInMillis()
        );
        TimeValue newGCDeleteSetting = new TimeValue(Math.abs(randomInt()), TimeUnit.MILLISECONDS);
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder().put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), newGCDeleteSetting.getStringRep()).build()
            )
        );
        assertEquals(
            TimeValue.parseTimeValue(
                newGCDeleteSetting.getStringRep(),
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_GC_DELETES_SETTING.getKey()
            ).getMillis(),
            settings.getGcDeletesInMillis()
        );
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        IndexSettings.INDEX_GC_DELETES_SETTING.getKey(),
                        (randomBoolean() ? -1 : new TimeValue(-1, TimeUnit.MILLISECONDS)).toString()
                    )
                    .build()
            )
        );
        assertEquals(-1, settings.getGcDeletesInMillis());
    }

    public void testTranslogFlushSizeThreshold() {
        ByteSizeValue translogFlushThresholdSize = ByteSizeValue.ofBytes(Math.abs(randomInt()));
        ByteSizeValue actualValue = ByteSizeValue.parseBytesSizeValue(
            translogFlushThresholdSize.getBytes() + "B",
            IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey()
        );
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), translogFlushThresholdSize.getBytes() + "B")
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(actualValue, settings.getFlushThresholdSize(ByteSizeValue.ofTb(1)));
        ByteSizeValue newTranslogFlushThresholdSize = ByteSizeValue.ofBytes(Math.abs(randomInt()));
        ByteSizeValue actualNewTranslogFlushThresholdSize = ByteSizeValue.parseBytesSizeValue(
            newTranslogFlushThresholdSize.getBytes() + "B",
            IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey()
        );
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), newTranslogFlushThresholdSize.getBytes() + "B")
                    .build()
            )
        );
        assertEquals(actualNewTranslogFlushThresholdSize, settings.getFlushThresholdSize(ByteSizeValue.ofTb(1)));
    }

    public void testTranslogGenerationSizeThreshold() {
        final ByteSizeValue size = ByteSizeValue.ofBytes(Math.abs(randomInt()));
        final String key = IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING.getKey();
        final ByteSizeValue actualValue = ByteSizeValue.parseBytesSizeValue(size.getBytes() + "B", key);
        final IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(key, size.getBytes() + "B").build()
        );
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(actualValue, settings.getGenerationThresholdSize());
        final ByteSizeValue newSize = ByteSizeValue.ofBytes(Math.abs(randomInt()));
        final ByteSizeValue actual = ByteSizeValue.parseBytesSizeValue(newSize.getBytes() + "B", key);
        settings.updateIndexMetadata(newIndexMeta("index", Settings.builder().put(key, newSize.getBytes() + "B").build()));
        assertEquals(actual, settings.getGenerationThresholdSize());
    }

    public void testPrivateSettingsValidation() {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_CREATION_DATE, System.currentTimeMillis()).build();
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);

        {
            // validation should fail since we are not ignoring private settings
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> indexScopedSettings.validate(settings, randomBoolean())
            );
            assertThat(e, hasToString(containsString("unknown setting [index.creation_date]")));
        }

        {
            // validation should fail since we are not ignoring private settings
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> indexScopedSettings.validate(settings, randomBoolean(), false, randomBoolean())
            );
            assertThat(e, hasToString(containsString("unknown setting [index.creation_date]")));
        }

        // nothing should happen since we are ignoring private settings
        indexScopedSettings.validate(settings, randomBoolean(), true, randomBoolean());
    }

    public void testArchivedSettingsValidation() {
        final Settings settings = Settings.builder()
            .put(AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX + "foo", System.currentTimeMillis())
            .build();
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);

        {
            // validation should fail since we are not ignoring archived settings
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> indexScopedSettings.validate(settings, randomBoolean())
            );
            assertThat(e, hasToString(containsString("unknown setting [archived.foo]")));
        }

        {
            // validation should fail since we are not ignoring archived settings
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> indexScopedSettings.validate(settings, randomBoolean(), randomBoolean(), false)
            );
            assertThat(e, hasToString(containsString("unknown setting [archived.foo]")));
        }

        // nothing should happen since we are ignoring archived settings
        indexScopedSettings.validate(settings, randomBoolean(), randomBoolean(), true);
    }

    public void testArchiveBrokenIndexSettings() {
        Settings settings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(Settings.EMPTY, e -> {
            assert false : "should not have been invoked, no unknown settings";
        }, (e, ex) -> { assert false : "should not have been invoked, no invalid settings"; });
        assertSame(settings, Settings.EMPTY);
        settings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(
            Settings.builder().put("index.refresh_interval", "-200").build(),
            e -> {
                assert false : "should not have been invoked, no invalid settings";
            },
            (e, ex) -> {
                assertThat(e.getKey(), equalTo("index.refresh_interval"));
                assertThat(e.getValue(), equalTo("-200"));
                assertThat(ex, hasToString(containsString("failed to parse setting [index.refresh_interval] with value [-200]")));
            }
        );
        assertEquals("-200", settings.get("archived.index.refresh_interval"));
        assertNull(settings.get("index.refresh_interval"));

        Settings prevSettings = settings; // no double archive
        settings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(prevSettings, e -> {
            assert false : "should not have been invoked, no unknown settings";
        }, (e, ex) -> { assert false : "should not have been invoked, no invalid settings"; });
        assertSame(prevSettings, settings);

        settings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(
            Settings.builder()
                .put("index.version.created", Version.CURRENT.id) // private setting
                .put("index.unknown", "foo")
                .put("index.refresh_interval", "2s")
                .build(),
            e -> {
                assertThat(e.getKey(), equalTo("index.unknown"));
                assertThat(e.getValue(), equalTo("foo"));
            },
            (e, ex) -> { assert false : "should not have been invoked, no invalid settings"; }
        );

        assertEquals("foo", settings.get("archived.index.unknown"));
        assertEquals(Integer.toString(Version.CURRENT.id), settings.get("index.version.created"));
        assertEquals("2s", settings.get("index.refresh_interval"));
    }

    public void testQueryDefaultField() {
        IndexSettings index = newIndexSettings(newIndexMeta("index", Settings.EMPTY), Settings.EMPTY);
        assertThat(index.getDefaultFields(), equalTo(Collections.singletonList("*")));
        index = newIndexSettings(
            newIndexMeta("index", Settings.EMPTY),
            Settings.builder().put("index.query.default_field", "body").build()
        );
        assertThat(index.getDefaultFields(), equalTo(Collections.singletonList("body")));
        index.updateIndexMetadata(newIndexMeta("index", Settings.builder().putList("index.query.default_field", "body", "title").build()));
        assertThat(index.getDefaultFields(), equalTo(Arrays.asList("body", "title")));
    }

    public void testUpdateSoftDeletesFails() {
        IndexScopedSettings settings = new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> settings.updateSettings(
                Settings.builder().put("index.soft_deletes.enabled", randomBoolean()).build(),
                Settings.builder(),
                Settings.builder(),
                "index"
            )
        );
        assertThat(error.getMessage(), equalTo("final index setting [index.soft_deletes.enabled], not updateable"));
    }

    public void testSoftDeletesDefaultSetting() {
        // enabled by default on 7.0+ or later
        Version createdVersion = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, createdVersion).build();
        assertTrue(IndexSettings.INDEX_SOFT_DELETES_SETTING.get(settings));
    }

    public void testCustomDataPathDeprecated() {
        final Settings settings = Settings.builder().put(IndexMetadata.INDEX_DATA_PATH_SETTING.getKey(), "my-custom-dir").build();
        IndexMetadata metadata = newIndexMeta("test", settings);
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(indexSettings.hasCustomDataPath(), is(true));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { IndexMetadata.INDEX_DATA_PATH_SETTING });
    }

    public void testNoTimeRange() {
        final Settings originalSettings = Settings.builder().build();
        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test", originalSettings), Settings.EMPTY);
        assertNull(indexSettings.getTimestampBounds());
    }

    public void testUpdateTimeSeriesTimeRange() {
        long endTime = System.currentTimeMillis();
        long startTime = endTime - TimeUnit.DAYS.toMillis(1);
        final Settings originalSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .put(TIME_SERIES_START_TIME.getKey(), startTime)
            .put(TIME_SERIES_END_TIME.getKey(), endTime)
            .build();
        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test", originalSettings), Settings.EMPTY);
        assertEquals(startTime, indexSettings.getTimestampBounds().startTime());
        assertEquals(endTime, indexSettings.getTimestampBounds().endTime());

        Settings endTimeBackwards = Settings.builder()
            .put(originalSettings)
            .put(TIME_SERIES_END_TIME.getKey(), endTime - randomLongBetween(1, 1000))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> indexSettings.updateIndexMetadata(newIndexMeta("test", endTimeBackwards))
        );
        assertThat(e.getMessage(), Matchers.containsString("index.time_series.end_time must be larger than current value"));

        long newEndTime = endTime + randomLongBetween(1, 1000);
        Settings endTimeForwards = Settings.builder().put(originalSettings).put(TIME_SERIES_END_TIME.getKey(), newEndTime).build();
        indexSettings.updateIndexMetadata(newIndexMeta("test", endTimeForwards));

        assertEquals(startTime, indexSettings.getTimestampBounds().startTime());
        assertEquals(newEndTime, indexSettings.getTimestampBounds().endTime());
    }

    public void testTimeSeriesTimeBoundary() {
        long startTime = System.currentTimeMillis();
        long endTime = startTime - randomLongBetween(1, 1000);
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .put(TIME_SERIES_START_TIME.getKey(), startTime)
            .put(TIME_SERIES_END_TIME.getKey(), endTime)
            .build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newIndexMeta("test", settings));
        assertThat(e.getMessage(), Matchers.containsString("index.time_series.end_time must be larger than index.time_series.start_time"));
    }

    public void testSame() {
        final var indexSettingKey = "index.example.setting";
        final var archivedSettingKey = "archived.example.setting";
        final var otherSettingKey = "other.example.setting";

        final var builder = Settings.builder();
        if (randomBoolean()) {
            builder.put(indexSettingKey, randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            builder.put(archivedSettingKey, randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            builder.put(otherSettingKey, randomAlphaOfLength(10));
        }
        final var settings = builder.build();
        assertTrue(IndexSettings.same(settings, Settings.builder().put(settings).build()));

        final var differentIndexSettingBuilder = Settings.builder().put(settings);
        if (settings.hasValue(indexSettingKey) && randomBoolean()) {
            differentIndexSettingBuilder.putNull(indexSettingKey);
        } else {
            differentIndexSettingBuilder.put(indexSettingKey, randomAlphaOfLength(11));
        }
        assertFalse(IndexSettings.same(settings, differentIndexSettingBuilder.build()));

        final var differentArchivedSettingBuilder = Settings.builder().put(settings);
        if (settings.hasValue(archivedSettingKey) && randomBoolean()) {
            differentArchivedSettingBuilder.putNull(archivedSettingKey);
        } else {
            differentArchivedSettingBuilder.put(archivedSettingKey, randomAlphaOfLength(11));
        }
        assertFalse(IndexSettings.same(settings, differentArchivedSettingBuilder.build()));

        final var differentOtherSettingBuilder = Settings.builder().put(settings);
        if (settings.hasValue(otherSettingKey) && randomBoolean()) {
            differentOtherSettingBuilder.putNull(otherSettingKey);
        } else {
            differentOtherSettingBuilder.put(otherSettingKey, randomAlphaOfLength(11));
        }
        assertTrue(IndexSettings.same(settings, differentOtherSettingBuilder.build()));
    }
}
