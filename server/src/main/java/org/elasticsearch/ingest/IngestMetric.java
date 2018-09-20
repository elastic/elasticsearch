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

class IngestMetric {

    private final MeanMetric ingestMetric = new MeanMetric();
    private final CounterMetric ingestCurrent = new CounterMetric();
    private final CounterMetric ingestCount = new CounterMetric();
    private final CounterMetric ingestFailed = new CounterMetric();

    void preIngest() {
        ingestCurrent.inc();
    }

    void postIngest(long ingestTimeInMillis) {
        ingestCurrent.dec();
        ingestMetric.inc(ingestTimeInMillis);
        ingestCount.inc();
    }

    void ingestFailed() {
        ingestFailed.inc();
    }

    void add(IngestMetric metrics){
        ingestCount.inc(metrics.ingestCount.count());
        ingestMetric.inc(metrics.ingestMetric.sum());
        ingestFailed.inc(metrics.ingestFailed.count());
    }

    IngestStats.Stats createStats() {
        return new IngestStats.Stats(ingestCount.count(), ingestMetric.sum(), ingestCurrent.count(), ingestFailed.count());
    }
}
