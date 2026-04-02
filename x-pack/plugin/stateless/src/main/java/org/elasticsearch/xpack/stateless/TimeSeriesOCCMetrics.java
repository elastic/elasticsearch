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

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.metric.LongAsyncCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * This class tracks the number of indexing and delete operations on time series indices as well as the number of operations that used OCC.
 * The exposed metrics are pre-aggregated per node.
 */
class TimeSeriesOCCMetrics extends AbstractLifecycleComponent implements IndexingOperationListener {
    static final String TSDB_OPERATIONS_TOTAL_METRIC = "es.indices.time_series.operations.operations.total";
    static final String TSDB_OCC_OPERATIONS_TOTAL_METRIC = "es.indices.time_series.operations.occ.total";

    private final LongAdder totalOps = new LongAdder();
    private final LongAdder occOps = new LongAdder();
    private final LongAsyncCounter totalOpsCounter;
    private final LongAsyncCounter occOpsCounter;

    TimeSeriesOCCMetrics(MeterRegistry meterRegistry) {
        this.totalOpsCounter = meterRegistry.registerLongAsyncCounter(
            TSDB_OPERATIONS_TOTAL_METRIC,
            "Total number of indexing and delete operations on time series indices",
            "ops",
            () -> new LongWithAttributes(totalOps.sum())
        );
        this.occOpsCounter = meterRegistry.registerLongAsyncCounter(
            TSDB_OCC_OPERATIONS_TOTAL_METRIC,
            "Total number of indexing and delete operations on time series indices where OCC was used",
            "ops",
            () -> new LongWithAttributes(occOps.sum())
        );
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        totalOps.increment();
        if (index.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            occOps.increment();
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        totalOps.increment();
        if (delete.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            occOps.increment();
        }
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {
        for (LongAsyncCounter counter : List.of(totalOpsCounter, occOpsCounter)) {
            try {
                counter.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
