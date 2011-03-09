/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An extension to the {@link ConcurrentMergeScheduler} that provides tracking on merge times, total
 * and current merges.
 */
public class TrackingConcurrentMergeScheduler extends ConcurrentMergeScheduler {

    private final ESLogger logger;

    private final AtomicLong totalMerges = new AtomicLong();
    private final AtomicLong totalMergeTime = new AtomicLong();
    private final AtomicLong currentMerges = new AtomicLong();

    public TrackingConcurrentMergeScheduler(ESLogger logger) {
        super();
        this.logger = logger;
    }

    public long totalMerges() {
        return totalMerges.get();
    }

    public long totalMergeTime() {
        return totalMergeTime.get();
    }

    public long currentMerges() {
        return currentMerges.get();
    }

    @Override protected void doMerge(MergePolicy.OneMerge merge) throws IOException {
        long time = System.currentTimeMillis();
        currentMerges.incrementAndGet();
        if (logger.isTraceEnabled()) {
            logger.trace("merge [{}] starting...", merge.info.name);
        }
        try {
            super.doMerge(merge);
        } finally {
            currentMerges.decrementAndGet();
            totalMerges.incrementAndGet();
            long took = System.currentTimeMillis() - time;
            totalMergeTime.addAndGet(took);
            if (took > 20000) { // if more than 20 seconds, DEBUG log it
                logger.debug("merge [{}] done, took [{}]", merge.info.name, TimeValue.timeValueMillis(took));
            } else if (logger.isTraceEnabled()) {
                logger.trace("merge [{}] done, took [{}]", merge.info.name, TimeValue.timeValueMillis(took));
            }
        }
    }
}