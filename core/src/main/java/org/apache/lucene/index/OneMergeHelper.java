/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
