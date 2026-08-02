/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.elasticsearch.index.MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING;
import static org.elasticsearch.index.MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING;
import static org.hamcrest.Matchers.containsString;

public class MergeSchedulerSettingsTests extends ESTestCase {
    private static class MockAppender extends AbstractAppender {
        public boolean sawUpdateMaxThreadCount;
        public boolean sawUpdateAutoThrottle;

        MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null);
        }

        @Override
        public void append(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            if (event.getLevel() == Level.TRACE && event.getLoggerName().endsWith("lucene.iw")) {
            }
            if (event.getLevel() == Level.DEBUG
                && message.contains("updating [index.merge.scheduler.max_thread_count] from [10000] to [1]")) {
                sawUpdateMaxThreadCount = true;
            }
            if (event.getLevel() == Level.DEBUG
                && message.contains("updating [index.merge.scheduler.auto_throttle] from [true] to [false]")) {
                sawUpdateAutoThrottle = true;
            }
        }

        @Override
        public boolean ignoreExceptions() {
            return false;
        }

    }

    public void testUpdateAutoThrottleSettings() throws Exception {
        MockAppender mockAppender = new MockAppender("testUpdateAutoThrottleSettings");
        mockAppender.start();
        final Logger settingsLogger = LogManager.getLogger("org.elasticsearch.common.settings.IndexScopedSettings");
        Loggers.addAppender(settingsLogger, mockAppender);
        Loggers.setLevel(settingsLogger, Level.TRACE);
        try {
            Settings.Builder builder = indexSettings(IndexVersion.current(), 1, 0).put(
                MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(),
                "2"
            )
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), "2")
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "2")
                .put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), "true");
            IndexSettings settings = new IndexSettings(newIndexMeta("index", builder.build()), Settings.EMPTY);
            assertEquals(settings.getMergeSchedulerConfig().isAutoThrottle(), true);
            builder.put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), "false");
            settings.updateIndexMetadata(newIndexMeta("index", builder.build()));
            // Make sure we log the change:
            assertTrue(mockAppender.sawUpdateAutoThrottle);
            assertEquals(settings.getMergeSchedulerConfig().isAutoThrottle(), false);
        } finally {
            Loggers.removeAppender(settingsLogger, mockAppender);
            mockAppender.stop();
            Loggers.setLevel(settingsLogger, (Level) null);
        }
    }

    // #6882: make sure we can change index.merge.scheduler.max_thread_count live
    public void testUpdateMergeMaxThreadCount() throws Exception {
        MockAppender mockAppender = new MockAppender("testUpdateAutoThrottleSettings");
        mockAppender.start();
        final Logger settingsLogger = LogManager.getLogger("org.elasticsearch.common.settings.IndexScopedSettings");
        Loggers.addAppender(settingsLogger, mockAppender);
        Loggers.setLevel(settingsLogger, Level.TRACE);
        try {
            Settings.Builder builder = indexSettings(IndexVersion.current(), 1, 0).put(
                MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(),
                "2"
            )
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), "2")
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "10000")
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "10000");
            IndexSettings settings = new IndexSettings(newIndexMeta("index", builder.build()), Settings.EMPTY);
            assertEquals(settings.getMergeSchedulerConfig().getMaxMergeCount(), 10000);
            assertEquals(settings.getMergeSchedulerConfig().getMaxThreadCount(), 10000);
            settings.updateIndexMetadata(newIndexMeta("index", builder.build()));
            assertFalse(mockAppender.sawUpdateMaxThreadCount);
            builder.put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1");
            settings.updateIndexMetadata(newIndexMeta("index", builder.build()));
            // Make sure we log the change:
            assertTrue(mockAppender.sawUpdateMaxThreadCount);
        } finally {
            Loggers.removeAppender(settingsLogger, mockAppender);
            mockAppender.stop();
            Loggers.setLevel(settingsLogger, (Level) null);
        }
    }

    private static IndexMetadata createMetadata(int maxThreadCount, int maxMergeCount) {
        Settings.Builder builder = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current());
        if (maxThreadCount != -1) {
            builder.put(MAX_THREAD_COUNT_SETTING.getKey(), maxThreadCount);
        }
        if (maxMergeCount != -1) {
            builder.put(MAX_MERGE_COUNT_SETTING.getKey(), maxMergeCount);
        }
        return newIndexMeta("index", builder.build());
    }

    public void testMaxThreadAndMergeCount() {
        // Explicit inverted pairs are rejected at the create/update API boundary, but IndexSettings
        // construction must still succeed (and clamp) so cluster-state application cannot hang.
        IndexSettings settings = new IndexSettings(createMetadata(10, 4), Settings.EMPTY);
        assertEquals(4, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(4, settings.getMergeSchedulerConfig().getMaxMergeCount());

        settings = new IndexSettings(createMetadata(4, 10), Settings.EMPTY);
        assertEquals(4, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(10, settings.getMergeSchedulerConfig().getMaxMergeCount());

        settings.updateIndexMetadata(createMetadata(15, 20));
        assertEquals(15, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(20, settings.getMergeSchedulerConfig().getMaxMergeCount());

        settings.updateIndexMetadata(createMetadata(40, 50));
        assertEquals(40, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(50, settings.getMergeSchedulerConfig().getMaxMergeCount());

        settings.updateIndexMetadata(createMetadata(40, -1));
        assertEquals(40, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(45, settings.getMergeSchedulerConfig().getMaxMergeCount());

        settings.updateIndexMetadata(createMetadata(40, 30));
        assertEquals(30, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(30, settings.getMergeSchedulerConfig().getMaxMergeCount());
    }

    public void testValidateExplicitMaxThreadAndMergeCount() {
        MergeSchedulerConfig.validateExplicitMaxThreadAndMergeCount(createMetadata(-1, 3).getSettings());
        MergeSchedulerConfig.validateExplicitMaxThreadAndMergeCount(createMetadata(4, -1).getSettings());
        MergeSchedulerConfig.validateExplicitMaxThreadAndMergeCount(createMetadata(4, 10).getSettings());

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> MergeSchedulerConfig.validateExplicitMaxThreadAndMergeCount(createMetadata(10, 4).getSettings())
        );
        assertThat(
            exc.getMessage(),
            containsString("[" + MAX_THREAD_COUNT_SETTING.getKey() + "] (= 10) must be <= [" + MAX_MERGE_COUNT_SETTING.getKey() + "] (= 4)")
        );
    }

    public void testDefaultMaxThreadCountHonorsNodeProcessors() {
        // On multicore hosts, setting node.processors to 1 must win over Runtime.availableProcessors().
        assumeTrue(
            "need more than 2 processors so the unrestricted default would differ from node.processors=1",
            Runtime.getRuntime().availableProcessors() > 2
        );
        IndexSettings settings = new IndexSettings(
            createMetadata(-1, -1),
            Settings.builder().put(NODE_PROCESSORS_SETTING.getKey(), 1).build()
        );
        assertEquals(1, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(6, settings.getMergeSchedulerConfig().getMaxMergeCount());
    }

    public void testClampsWhenNodeDefaultExceedsExplicitMaxMergeCount() {
        final int processors = Runtime.getRuntime().availableProcessors();
        final int defaultThreadCount = Math.max(1, processors / 2);
        assumeTrue("need default max_thread_count > 1 to exercise the node-default overshoot", defaultThreadCount > 1);
        final int maxMergeCount = defaultThreadCount - 1;

        IndexSettings settings = new IndexSettings(
            createMetadata(-1, maxMergeCount),
            Settings.builder().put(NODE_PROCESSORS_SETTING.getKey(), processors).build()
        );
        assertEquals(maxMergeCount, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(maxMergeCount, settings.getMergeSchedulerConfig().getMaxMergeCount());

        settings.updateIndexMetadata(createMetadata(-1, 1));
        assertEquals(1, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(1, settings.getMergeSchedulerConfig().getMaxMergeCount());
    }
}
