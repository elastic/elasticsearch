/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.engine.AbstractEngineTestCase;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.junit.After;

/**
 * Verifies merge-read abort wiring on {@link IndexDirectory}, including the path where
 * {@link IndexEngine}'s merge scheduler close signals abort before the directory is closed.
 */
public class IndexEngineMergeAbortTests extends AbstractEngineTestCase {

    @After
    public void assertWarningHeaders() {
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testMergeSchedulerCloseSignalsAbortMergeReads() throws Exception {
        Settings nodeSettings = Settings.builder().put(StatelessPlugin.STATELESS_ENABLED.getKey(), true).build();
        EngineConfig config = indexConfigWithIndexDirectory(Settings.EMPTY, nodeSettings);
        IndexDirectory indexDirectory = IndexDirectory.unwrapDirectory(config.getStore().directory());
        try (IndexEngine engine = newIndexEngine(config)) {
            engine.index(randomDoc(String.valueOf(0)));
            engine.flush();
        }
        assertTrue(indexDirectory.shouldAbortMergeReads());
    }

    public void testConcurrentMergeSchedulerCloseSignalsAbortMergeReads() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(StatelessPlugin.STATELESS_ENABLED.getKey(), true)
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), false)
            .build();
        EngineConfig config = indexConfigWithIndexDirectory(Settings.EMPTY, nodeSettings);
        IndexDirectory indexDirectory = IndexDirectory.unwrapDirectory(config.getStore().directory());
        try (IndexEngine engine = newIndexEngine(config)) {
            engine.index(randomDoc(String.valueOf(0)));
            engine.flush();
        }
        assertTrue(indexDirectory.shouldAbortMergeReads());
    }
}
