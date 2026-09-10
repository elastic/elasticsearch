/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.textstructure.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ExecutorService;

import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;
import static org.elasticsearch.common.util.concurrent.EsExecutors.allocatedProcessors;

/**
 * workaround for https://github.com/elastic/elasticsearch/issues/97916
 * TODO delete this entire class when we can
 */
public class TextStructExecutor {
    private final ThrottledTaskRunner analysisRunner;

    @Inject
    public TextStructExecutor(ThreadPool threadPool, Settings settings) {
        this.analysisRunner = new ThrottledTaskRunner("find_structure", maxConcurrentAnalyses(settings), threadPool.generic());
    }

    /**
     * Structure analysis is CPU-bound and single-threaded per request, holding its whole expanded sample on
     * the heap for the duration. Admitting more analyses than there are processors therefore buys no
     * throughput and multiplies peak heap by the number of extra requests.
     */
    private static int maxConcurrentAnalyses(Settings settings) {
        return allocatedProcessors(settings);
    }

    /**
     * when the workaround is removed, change the value in each consuming class's constructor passes to the super constructor from
     * DIRECT_EXECUTOR_SERVICE back to threadpool.generic() so that we continue to fork off of the transport thread.
     */
    ExecutorService handledTransportActionExecutorService() {
        return DIRECT_EXECUTOR_SERVICE;
    }

    /**
     * when the workaround is removed, change the callers of this function to
     * {@link ActionListener#completeWith(ActionListener, CheckedSupplier)}.
     */
    <T> void execute(ActionListener<T> listener, CheckedSupplier<T, Exception> supplier) {
        analysisRunner.enqueueTask(ActionListener.wrap(releasable -> {
            try (releasable) {
                ActionListener.completeWith(listener, supplier);
            }
        }, listener::onFailure));
    }
}
