/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.index;

import java.io.IOException;

/**
 * Allows pkg private access
 */
public class OneMergeHelper {
    private OneMergeHelper() {}
    public static String getSegmentName(MergePolicy.OneMerge merge) {
        return merge.info != null ? merge.info.info.name : "_na_";
    }

    /**
     * The current MB per second rate limit for this merge.
     **/
    public static double getMbPerSec(Thread thread, MergePolicy.OneMerge merge) {
        if (thread instanceof ConcurrentMergeScheduler.MergeThread) {
            return ((ConcurrentMergeScheduler.MergeThread) thread).rateLimiter.getMBPerSec();
        }
        assert false: "this is not merge thread";
        return Double.POSITIVE_INFINITY;
    }

    /**
     * Returns total bytes written by this merge.
     **/
    public static long getTotalBytesWritten(Thread thread,
                                            MergePolicy.OneMerge merge) throws IOException {
        /**
         * TODO: The number of bytes written during the merge should be accessible in OneMerge.
         */
        if (thread instanceof ConcurrentMergeScheduler.MergeThread) {
            return ((ConcurrentMergeScheduler.MergeThread) thread).rateLimiter
                .getTotalBytesWritten();
        }
        assert false: "this is not merge thread";
        return merge.totalBytesSize();
    }


}
