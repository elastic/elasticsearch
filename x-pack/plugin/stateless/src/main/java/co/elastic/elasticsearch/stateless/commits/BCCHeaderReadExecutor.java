/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

import static co.elastic.elasticsearch.stateless.Stateless.SHARD_READ_THREAD_POOL;

/**
 * Executor that limits concurrent BCC header reads to the shard read thread pool size
 * to prevent memory exhaustion when processing referenced BCCs during recovery operations.
 */
public class BCCHeaderReadExecutor implements Executor {
    private final Logger logger = LogManager.getLogger(BCCHeaderReadExecutor.class);

    private final ThrottledTaskRunner throttledFetchExecutor;

    public BCCHeaderReadExecutor(ThreadPool threadPool) {
        this.throttledFetchExecutor = new ThrottledTaskRunner(
            BCCHeaderReadExecutor.class.getCanonicalName(),
            // With this limit we don't hurt reading performance, but we avoid OOMing if
            // the latest BCC references too many BCCs.
            threadPool.info(SHARD_READ_THREAD_POOL).getMax(),
            threadPool.generic()
        );
    }

    @Override
    public void execute(Runnable command) {
        throttledFetchExecutor.enqueueTask(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    command.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to read a BCC header", e);
            }

            @Override
            public String toString() {
                return command.toString();
            }
        });
    }
}
