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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.MockLog;

import static org.elasticsearch.index.MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING;
import static org.elasticsearch.index.MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING;

/**
 * Verifies that opening an index with an inverted merge-scheduler pair clamps and warns once
 * from {@code IndicesService.createIndex}.
 */
public class MergeSchedulerClampTests extends ESSingleNodeTestCase {

    public void testCreateIndexWarnsWhenMaxThreadCountClamped() {
        final Settings settings = indexSettings(1, 0).put(MAX_THREAD_COUNT_SETTING.getKey(), 10)
            .put(MAX_MERGE_COUNT_SETTING.getKey(), 4)
            .build();
        MockLog.assertThatLogger(() -> {
            IndexService indexService = createIndex("merge-scheduler-clamp", settings);
            assertEquals(4, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxThreadCount());
            assertEquals(4, indexService.getIndexSettings().getMergeSchedulerConfig().getMaxMergeCount());
            assertTrue(indexService.getIndexSettings().getMergeSchedulerConfig().getAppliedCounts().isMaxThreadCountClamped());
        },
            IndexSettings.class,
            new MockLog.SeenEventExpectation(
                "warn when createIndex clamps max_thread_count",
                IndexSettings.class.getCanonicalName(),
                Level.WARN,
                "[" + MAX_THREAD_COUNT_SETTING.getKey() + "] (= 10) exceeds [" + MAX_MERGE_COUNT_SETTING.getKey() + "] (= 4); using 4"
            )
        );
    }
}
