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

package co.elastic.elasticsearch.stateless.engine;

import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class MergeMetrics {

    public static final String MERGE_SEGMENTS_SIZE = "es.merge.segments.size";
    public static final String MERGE_DOCS_TOTAL = "es.merge.docs.total";
    public static final String MERGE_TIME_IN_MILLIS = "es.merge.time";
    public static MergeMetrics NOOP = new MergeMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    private final LongCounter mergeSizeInBytes;
    private final LongCounter mergeNumDocs;
    private final LongHistogram mergeTimeInMillis;

    public MergeMetrics(MeterRegistry meterRegistry) {
        mergeSizeInBytes = meterRegistry.registerLongCounter(MERGE_SEGMENTS_SIZE, "Total size of segments merged", "bytes");
        mergeNumDocs = meterRegistry.registerLongCounter(MERGE_DOCS_TOTAL, "Total number of documents merged", "documents");
        mergeTimeInMillis = meterRegistry.registerLongHistogram(MERGE_TIME_IN_MILLIS, "Merge time in milliseconds", "milliseconds");
    }

    public void markMergeMetrics(MergePolicy.OneMerge currentMerge, long tookMillis) {
        mergeSizeInBytes.incrementBy(currentMerge.totalBytesSize());
        mergeNumDocs.incrementBy(currentMerge.totalNumDocs());
        mergeTimeInMillis.record(tookMillis);
    }
}
