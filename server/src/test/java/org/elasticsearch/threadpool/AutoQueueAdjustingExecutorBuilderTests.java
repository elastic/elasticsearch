/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.containsString;

public class AutoQueueAdjustingExecutorBuilderTests extends ESThreadPoolTestCase {

    public void testValidatingMinMaxSettings() {
        Settings settings = Settings.builder()
            .put("thread_pool.test.min_queue_size", randomIntBetween(30, 100))
            .put("thread_pool.test.max_queue_size", randomIntBetween(1, 25))
            .build();
        try {
            new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 15, 1, 100, 10);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Failed to parse value"));
        }

        settings = Settings.builder().put("thread_pool.test.min_queue_size", 10).put("thread_pool.test.max_queue_size", 9).build();
        try {
            new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 15, 1, 100, 2000).getSettings(settings);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Failed to parse value [10] for setting [thread_pool.test.min_queue_size] must be <= 9");
        }

        settings = Settings.builder().put("thread_pool.test.min_queue_size", 11).put("thread_pool.test.max_queue_size", 10).build();
        try {
            new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 15, 1, 100, 2000).getSettings(settings);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Failed to parse value [11] for setting [thread_pool.test.min_queue_size] must be <= 10");
        }

        settings = Settings.builder().put("thread_pool.test.min_queue_size", 101).build();
        try {
            new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 15, 100, 100, 2000).getSettings(settings);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Failed to parse value [101] for setting [thread_pool.test.min_queue_size] must be <= 100");
        }

        settings = Settings.builder().put("thread_pool.test.max_queue_size", 99).build();
        try {
            new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 15, 100, 100, 2000).getSettings(settings);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Failed to parse value [100] for setting [thread_pool.test.min_queue_size] must be <= 99");
        }

        assertSettingDeprecationsAndWarnings(getDeprecatedSettingsForSettingNames("thread_pool.test.max_queue_size"));
    }

    public void testSetLowerSettings() {
        Settings settings = Settings.builder()
            .put("thread_pool.test.min_queue_size", 10)
            .put("thread_pool.test.max_queue_size", 10)
            .build();
        AutoQueueAdjustingExecutorBuilder test = new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 1000, 1000, 1000, 2000);
        AutoQueueAdjustingExecutorBuilder.AutoExecutorSettings s = test.getSettings(settings);
        assertEquals(10, s.maxQueueSize);
        assertEquals(10, s.minQueueSize);

        assertSettingDeprecationsAndWarnings(
            getDeprecatedSettingsForSettingNames("thread_pool.test.min_queue_size", "thread_pool.test.max_queue_size")
        );
    }

    public void testSetHigherSettings() {
        Settings settings = Settings.builder()
            .put("thread_pool.test.min_queue_size", 2000)
            .put("thread_pool.test.max_queue_size", 3000)
            .build();
        AutoQueueAdjustingExecutorBuilder test = new AutoQueueAdjustingExecutorBuilder(settings, "test", 1, 1000, 1000, 1000, 2000);
        AutoQueueAdjustingExecutorBuilder.AutoExecutorSettings s = test.getSettings(settings);
        assertEquals(3000, s.maxQueueSize);
        assertEquals(2000, s.minQueueSize);

        assertSettingDeprecationsAndWarnings(
            getDeprecatedSettingsForSettingNames("thread_pool.test.min_queue_size", "thread_pool.test.max_queue_size")
        );
    }

    private Setting<?>[] getDeprecatedSettingsForSettingNames(String... settingNames) {
        return Arrays.stream(settingNames)
            .map(settingName -> Setting.intSetting(settingName, randomInt(), Setting.Property.Deprecated))
            .toArray(Setting[]::new);
    }

}
