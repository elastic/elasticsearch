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

package org.elasticsearch.ingest;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;

/**
 * <p>Metrics to measure ingest actions.
 * <p>This counts measure documents and timings for a given scope.
 * The scope is determined by the calling code. For example you can use this class to count all documents across all pipeline,
 * or you can use this class to count documents for a given pipeline or a specific processor.
 * This class does not make assumptions about it's given scope.
 */
class IngestMetric {

    /**
     * The time it takes to complete the measured item.
     */
    private final MeanMetric ingestTime = new MeanMetric();
    /**
     * The current count of things being measure. Should most likely ever be 0 or 1.
     * Useful when aggregating multiple metrics to see how many things are in flight.
     */
    private final CounterMetric ingestCurrent = new CounterMetric();
    /**
     * The ever increasing count of things being measured
     */
    private final CounterMetric ingestCount = new CounterMetric();
    /**
     * The only increasing count of failures
     */
    private final CounterMetric ingestFailed = new CounterMetric();

    /**
     * Call this prior to the ingest action.
     */
    void preIngest() {
        ingestCurrent.inc();
    }

    /**
     * Call this after the performing the ingest action, even if the action failed.
     * @param ingestTimeInMillis The time it took to perform the action.
     */
    void postIngest(long ingestTimeInMillis) {
        ingestCurrent.dec();
        ingestTime.inc(ingestTimeInMillis);
        ingestCount.inc();
    }

    /**
     * Call this if the ingest action failed.
     */
    void ingestFailed() {
        ingestFailed.inc();
    }

    /**
     * <p>Add two sets of metrics together.
     * <p><strong>Note -</strong> this method does <strong>not</strong> add the current count values.
     * The current count value is ephemeral and requires a increase/decrease operation pairs to keep the value correct.
     *
     * @param metrics The metric to add.
     */
    void add(IngestMetric metrics) {
        ingestCount.inc(metrics.ingestCount.count());
        ingestTime.inc(metrics.ingestTime.sum());
        ingestFailed.inc(metrics.ingestFailed.count());
    }

    /**
     * Creates a serializable representation for these metrics.
     */
    IngestStats.Stats createStats() {
        return new IngestStats.Stats(ingestCount.count(), ingestTime.sum(), ingestCurrent.count(), ingestFailed.count());
    }
}
