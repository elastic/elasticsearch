/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.threadpool.ThreadPool;

public final class SearchUtils {

    /**
     * Configure the maximum lucene query clause count based on the size of the search thread pool and maximum heap
     */
    public static void configureMaxClauses(ThreadPool threadPool) {
        int searchThreadPoolSize = threadPool.info(ThreadPool.Names.SEARCH).getMax();
        long heapSize = JvmStats.jvmStats().getMem().getHeapMax().getGb();
        configureMaxClauses(searchThreadPoolSize, heapSize);
    }

    static void configureMaxClauses(long threadPoolSize, long heapInGb) {
        if (threadPoolSize <= 0 || heapInGb <= 0) {
            return;     // If we don't know how to size things, keep the lucene default
        }

        int maxClauseCount = Math.toIntExact(heapInGb * 65_536 / threadPoolSize);
        IndexSearcher.setMaxClauseCount(maxClauseCount);
    }

    private SearchUtils() {}
}
