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
import org.elasticsearch.test.MockLog;

import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.elasticsearch.index.MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING;
import static org.elasticsearch.index.MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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
        // Inverted pairs must still construct successfully (and clamp) so cluster-state application cannot hang.
        IndexSettings settings = new IndexSettings(createMetadata(10, 4), Settings.EMPTY);
        assertEquals(4, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(4, settings.getMergeSchedulerConfig().getMaxMergeCount());
        assertTrue(settings.getMergeSchedulerConfig().getAppliedCounts().isMaxThreadCountClamped());
        assertEquals(10, settings.getMergeSchedulerConfig().getAppliedCounts().requestedMaxThreadCount());

        settings = new IndexSettings(createMetadata(4, 10), Settings.EMPTY);
        assertEquals(4, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(10, settings.getMergeSchedulerConfig().getMaxMergeCount());
        assertFalse(settings.getMergeSchedulerConfig().getAppliedCounts().isMaxThreadCountClamped());

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
        assertTrue(settings.getMergeSchedulerConfig().getAppliedCounts().isMaxThreadCountClamped());
    }

    public void testDefaultMaxThreadCountIndependentOfNodeProcessors() {
        assumeTrue(
            "need more than 2 processors so availableProcessors()/2 differs from node.processors=1",
            Runtime.getRuntime().availableProcessors() > 2
        );
        final int expected = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        IndexSettings settings = new IndexSettings(
            createMetadata(-1, -1),
            Settings.builder().put(NODE_PROCESSORS_SETTING.getKey(), 1).build()
        );
        assertEquals(expected, settings.getMergeSchedulerConfig().getMaxThreadCount());
        assertEquals(expected + 5, settings.getMergeSchedulerConfig().getMaxMergeCount());
    }

    public void testClampsWhenDefaultExceedsExplicitMaxMergeCount() {
        // max_merge_count=1 exercises the invariant on every host; on hosts with default > 1 it also clamps.
        IndexSettings settings = new IndexSettings(createMetadata(-1, 1), Settings.EMPTY);
        assertThat(
            settings.getMergeSchedulerConfig().getMaxThreadCount(),
            lessThanOrEqualTo(settings.getMergeSchedulerConfig().getMaxMergeCount())
        );
        assertEquals(1, settings.getMergeSchedulerConfig().getMaxMergeCount());
    }

    public void testConstructionDoesNotWarnWhenClamping() {
        MockLog.assertThatLogger(() -> {
            IndexSettings settings = new IndexSettings(createMetadata(10, 4), Settings.EMPTY);
            assertTrue(settings.getMergeSchedulerConfig().getAppliedCounts().isMaxThreadCountClamped());
            new IndexSettings(createMetadata(-1, 1), Settings.EMPTY);
        },
            IndexSettings.class,
            new MockLog.UnseenEventExpectation(
                "no warn on throwaway IndexSettings construction",
                IndexSettings.class.getCanonicalName(),
                Level.WARN,
                "*" + MAX_THREAD_COUNT_SETTING.getKey() + "*"
            )
        );
    }

    public void testWarnsWhenMaxThreadCountIsClampedOnSettingsUpdate() {
        IndexSettings settings = new IndexSettings(createMetadata(4, 10), Settings.EMPTY);
        MockLog.assertThatLogger(() -> {
            settings.updateIndexMetadata(createMetadata(40, 30));
            assertEquals(30, settings.getMergeSchedulerConfig().getMaxThreadCount());
            assertEquals(30, settings.getMergeSchedulerConfig().getMaxMergeCount());
        },
            IndexSettings.class,
            new MockLog.SeenEventExpectation(
                "warn on update when max_thread_count is clamped",
                IndexSettings.class.getCanonicalName(),
                Level.WARN,
                "[" + MAX_THREAD_COUNT_SETTING.getKey() + "] (= 40) exceeds [" + MAX_MERGE_COUNT_SETTING.getKey() + "] (= 30); using 30"
            )
        );
    }
}
