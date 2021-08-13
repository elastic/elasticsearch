/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class TransformIndexerStatsTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            TransformIndexerStatsTests::randomStats,
            TransformIndexerStatsTests::toXContent,
            TransformIndexerStats::fromXContent
        ).supportsUnknownFields(true).test();
    }

    public static TransformIndexerStats randomStats() {
        return new TransformIndexerStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomDouble(),
            randomDouble(),
            randomDouble()
        );
    }

    public static void toXContent(TransformIndexerStats stats, XContentBuilder builder) throws IOException {
        builder.startObject();
        if (randomBoolean()) {
            builder.field(TransformIndexerStats.PAGES_PROCESSED.getPreferredName(), stats.getPagesProcessed());
            builder.field(TransformIndexerStats.DOCUMENTS_PROCESSED.getPreferredName(), stats.getDocumentsProcessed());
            builder.field(TransformIndexerStats.DOCUMENTS_INDEXED.getPreferredName(), stats.getDocumentsIndexed());
            builder.field(TransformIndexerStats.DOCUMENTS_DELETED.getPreferredName(), stats.getDocumentsDeleted());
            builder.field(TransformIndexerStats.TRIGGER_COUNT.getPreferredName(), stats.getTriggerCount());
            builder.field(TransformIndexerStats.INDEX_TIME_IN_MS.getPreferredName(), stats.getIndexTime());
            builder.field(TransformIndexerStats.INDEX_TOTAL.getPreferredName(), stats.getIndexTotal());
            builder.field(TransformIndexerStats.INDEX_FAILURES.getPreferredName(), stats.getIndexFailures());
            builder.field(TransformIndexerStats.SEARCH_TIME_IN_MS.getPreferredName(), stats.getSearchTime());
            builder.field(TransformIndexerStats.SEARCH_TOTAL.getPreferredName(), stats.getSearchTotal());
            builder.field(TransformIndexerStats.PROCESSING_TIME_IN_MS.getPreferredName(), stats.getProcessingTime());
            builder.field(TransformIndexerStats.PROCESSING_TOTAL.getPreferredName(), stats.getProcessingTotal());
            builder.field(TransformIndexerStats.DELETE_TIME_IN_MS.getPreferredName(), stats.getDeleteTime());
            builder.field(TransformIndexerStats.SEARCH_FAILURES.getPreferredName(), stats.getSearchFailures());
            builder.field(
                TransformIndexerStats.EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS.getPreferredName(),
                stats.getExpAvgCheckpointDurationMs()
            );
            builder.field(TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_INDEXED.getPreferredName(), stats.getExpAvgDocumentsIndexed());
            builder.field(
                TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_PROCESSED.getPreferredName(),
                stats.getExpAvgDocumentsProcessed()
            );
        } else {
            // a toXContent version which leaves out field with value 0 (simulating the case that an older version misses a field)
            xContentFieldIfNotZero(builder, TransformIndexerStats.PAGES_PROCESSED.getPreferredName(), stats.getPagesProcessed());
            xContentFieldIfNotZero(builder, TransformIndexerStats.DOCUMENTS_PROCESSED.getPreferredName(), stats.getDocumentsProcessed());
            xContentFieldIfNotZero(builder, TransformIndexerStats.DOCUMENTS_INDEXED.getPreferredName(), stats.getDocumentsIndexed());
            xContentFieldIfNotZero(builder, TransformIndexerStats.DOCUMENTS_DELETED.getPreferredName(), stats.getDocumentsDeleted());
            xContentFieldIfNotZero(builder, TransformIndexerStats.TRIGGER_COUNT.getPreferredName(), stats.getTriggerCount());
            xContentFieldIfNotZero(builder, TransformIndexerStats.INDEX_TIME_IN_MS.getPreferredName(), stats.getIndexTime());
            xContentFieldIfNotZero(builder, TransformIndexerStats.INDEX_TOTAL.getPreferredName(), stats.getIndexTotal());
            xContentFieldIfNotZero(builder, TransformIndexerStats.INDEX_FAILURES.getPreferredName(), stats.getIndexFailures());
            xContentFieldIfNotZero(builder, TransformIndexerStats.SEARCH_TIME_IN_MS.getPreferredName(), stats.getSearchTime());
            xContentFieldIfNotZero(builder, TransformIndexerStats.SEARCH_TOTAL.getPreferredName(), stats.getSearchTotal());
            xContentFieldIfNotZero(builder, TransformIndexerStats.PROCESSING_TIME_IN_MS.getPreferredName(), stats.getProcessingTime());
            xContentFieldIfNotZero(builder, TransformIndexerStats.PROCESSING_TOTAL.getPreferredName(), stats.getProcessingTotal());
            xContentFieldIfNotZero(builder, TransformIndexerStats.DELETE_TIME_IN_MS.getPreferredName(), stats.getDeleteTime());
            xContentFieldIfNotZero(builder, TransformIndexerStats.SEARCH_FAILURES.getPreferredName(), stats.getSearchFailures());
            xContentFieldIfNotZero(
                builder,
                TransformIndexerStats.EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS.getPreferredName(),
                stats.getExpAvgCheckpointDurationMs()
            );
            xContentFieldIfNotZero(
                builder,
                TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_INDEXED.getPreferredName(),
                stats.getExpAvgDocumentsIndexed()
            );
            xContentFieldIfNotZero(
                builder,
                TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_PROCESSED.getPreferredName(),
                stats.getExpAvgDocumentsProcessed()
            );
        }
        builder.endObject();
    }

    private static XContentBuilder xContentFieldIfNotZero(XContentBuilder builder, String name, long value) throws IOException {
        if (value > 0) {
            builder.field(name, value);
        }

        return builder;
    }

    private static XContentBuilder xContentFieldIfNotZero(XContentBuilder builder, String name, double value) throws IOException {
        if (value > 0.0) {
            builder.field(name, value);
        }

        return builder;
    }
}
