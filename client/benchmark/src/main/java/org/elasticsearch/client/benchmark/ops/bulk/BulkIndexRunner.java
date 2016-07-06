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
package org.elasticsearch.client.benchmark.ops.bulk;

import org.elasticsearch.client.benchmark.metrics.MetricsRecord;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class BulkIndexRunner implements Runnable {
    private static final ESLogger logger = ESLoggerFactory.getLogger(BulkIndexRunner.class.getName());

    private final BlockingQueue<List<String>> bulkData;
    private final BulkRequestExecutor bulkRequestExecutor;
    private final List<MetricsRecord> metrics;

    public BulkIndexRunner(BlockingQueue<List<String>> bulkData, BulkRequestExecutor bulkRequestExecutor) {
        this.bulkData = bulkData;
        this.bulkRequestExecutor = bulkRequestExecutor;
        this.metrics = new ArrayList<>();
    }

    @Override
    public void run() {
        boolean interrupted = false;
        // we'll defer interruptions until the queue is drained.
        while ((interrupted && bulkData.isEmpty()) == false) {
            boolean success = false;
            List<String> currentBulk = null;
            try {
                currentBulk = bulkData.take();
            } catch (InterruptedException e) {
                interrupted = true;
            }
            // Yes, this approach is prone to coordinated omission *but* we have to consider that we want to benchmark a closed system
            // with backpressure here instead of an open system. So this is actually correct in this case.
            long start = System.nanoTime();
            try {
                success = bulkRequestExecutor.bulkIndex(currentBulk);
            } catch (Exception ex) {
                logger.warn("Error while executing bulk request", ex);
            }
            long stop = System.nanoTime();
            //TODO dm: Warmup (for ES we should run the benchmark multiple times anyway but we also must warmup the client process too)
            metrics.add(new MetricsRecord("bulk", start, stop, success));
        }
        // restore interrupt flag - we can only ever exit the loop via an interrupt
        Thread.currentThread().interrupt();
    }

    public List<MetricsRecord> getMetrics() {
        return Collections.unmodifiableList(metrics);
    }
}
