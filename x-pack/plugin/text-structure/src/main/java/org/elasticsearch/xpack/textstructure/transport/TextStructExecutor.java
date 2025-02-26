/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.textstructure.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ExecutorService;

import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;

/**
 * workaround for https://github.com/elastic/elasticsearch/issues/97916
 * TODO delete this entire class when we can
 */
public class TextStructExecutor {
    private final ThreadPool threadPool;

    @Inject
    public TextStructExecutor(ThreadPool threadPool) {
        this.threadPool = threadPool;
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
        threadPool.generic().execute(ActionRunnable.supply(listener, supplier));
    }
}
