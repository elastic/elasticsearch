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
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * An extension to the {@link ConcurrentMergeScheduler} that provides tracking on merge times, total
 * and current merges.
 */
public class TrackingConcurrentMergeScheduler extends ConcurrentMergeScheduler {

    private final ESLogger logger;

    private final MeanMetric totalMerges = new MeanMetric();
    private final CounterMetric currentMerges = new CounterMetric();

    public TrackingConcurrentMergeScheduler(ESLogger logger) {
        super();
        this.logger = logger;
    }

    public long totalMerges() {
        return totalMerges.count();
    }

    public long totalMergeTime() {
        return totalMerges.sum();
    }

    public long currentMerges() {
        return currentMerges.count();
    }

    @Override protected void doMerge(MergePolicy.OneMerge merge) throws IOException {
        long time = System.currentTimeMillis();
        currentMerges.inc();
        if (logger.isTraceEnabled()) {
            logger.trace("merge [{}] starting...", merge.info.name);
        }
        try {
            super.doMerge(merge);
        } finally {
            currentMerges.dec();
            long took = System.currentTimeMillis() - time;
            totalMerges.inc(took);
            if (took > 20000) { // if more than 20 seconds, DEBUG log it
                logger.debug("merge [{}] done, took [{}]", merge.info.name, TimeValue.timeValueMillis(took));
            } else if (logger.isTraceEnabled()) {
                logger.trace("merge [{}] done, took [{}]", merge.info.name, TimeValue.timeValueMillis(took));
            }
        }
    }
}