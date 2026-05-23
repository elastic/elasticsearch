/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Periodic background task that scans for expired staged semantic_text data on each index.
 * Runs at the {@code index.semantic_text.staged_ttl} interval and is disabled when the TTL
 * is set to 0 or -1.
 */
public class AsyncStagedSemanticCleanupTask extends AbstractAsyncTask {

    private static final Logger logger = LogManager.getLogger(AsyncStagedSemanticCleanupTask.class);

    private final IndexSettings indexSettings;

    public AsyncStagedSemanticCleanupTask(IndexSettings indexSettings, ThreadPool threadPool) {
        super(logger, threadPool, threadPool.generic(), TimeValue.timeValueMillis(indexSettings.getSemanticTextStagedTtlMillis()), true);
        this.indexSettings = indexSettings;
        rescheduleIfNecessary();
    }

    @Override
    protected boolean mustReschedule() {
        return indexSettings.getSemanticTextStagedTtlMillis() > 0;
    }

    @Override
    protected void runInternal() {
        long ttlMillis = indexSettings.getSemanticTextStagedTtlMillis();
        if (ttlMillis <= 0) {
            return;
        }
        logger.debug("Running staged semantic_text cleanup with TTL [{}ms]", ttlMillis);
        // Actual cleanup logic will scan for expired _staged entries
        // and clear them. For now this is a placeholder.
    }
}
