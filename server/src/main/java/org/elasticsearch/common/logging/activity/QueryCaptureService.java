/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

/**
 * Runs best-effort query capture on a bounded worker queue without blocking the search completion thread.
 */
public final class QueryCaptureService {
    private static final Logger logger = LogManager.getLogger(QueryCaptureService.class);
    private final Executor executor;

    /** Uses the node's single capture worker, shared by all searches. */
    public QueryCaptureService(ThreadPool threadPool) {
        executor = threadPool.executor(ThreadPool.Names.QUERY_CAPTURE);
    }

    /**
     * Submits capture work, retaining the response before the caller can release it. A null response represents a failed query.
     * Rejected captures are dropped; the executor records rejections in its thread-pool statistics.
     * The runnable must only read the response and must not retain borrowed resources after returning.
     */
    public void execute(Runnable capture, @Nullable RefCounted response) {
        if (response != null) {
            response.incRef();
        }
        // EsThreadPoolExecutor delivers rejection to AbstractRunnable and calls onAfter on both execution and rejection.
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                capture.run();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("query capture processing failed", e);
            }

            @Override
            public void onRejection(Exception e) {
                // Capture is best effort: do not block, run on the caller, or log once per dropped query.
            }

            @Override
            public void onAfter() {
                if (response != null) {
                    response.decRef();
                }
            }
        });
    }
}
