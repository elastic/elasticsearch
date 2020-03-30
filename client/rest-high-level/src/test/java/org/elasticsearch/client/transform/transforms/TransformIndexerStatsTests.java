/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.client.core.IndexerJobStats;
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
            randomDouble(),
            randomDouble(),
            randomDouble()
        );
    }

    public static void toXContent(TransformIndexerStats stats, XContentBuilder builder) throws IOException {
        builder.startObject();
        if (randomBoolean()) {
            builder.field(IndexerJobStats.NUM_PAGES.getPreferredName(), stats.getNumPages());
            builder.field(IndexerJobStats.NUM_INPUT_DOCUMENTS.getPreferredName(), stats.getNumDocuments());
            builder.field(IndexerJobStats.NUM_OUTPUT_DOCUMENTS.getPreferredName(), stats.getOutputDocuments());
            builder.field(IndexerJobStats.NUM_INVOCATIONS.getPreferredName(), stats.getNumInvocations());
            builder.field(IndexerJobStats.INDEX_TIME_IN_MS.getPreferredName(), stats.getIndexTime());
            builder.field(IndexerJobStats.INDEX_TOTAL.getPreferredName(), stats.getIndexTotal());
            builder.field(IndexerJobStats.INDEX_FAILURES.getPreferredName(), stats.getIndexFailures());
            builder.field(IndexerJobStats.SEARCH_TIME_IN_MS.getPreferredName(), stats.getSearchTime());
            builder.field(IndexerJobStats.SEARCH_TOTAL.getPreferredName(), stats.getSearchTotal());
            builder.field(IndexerJobStats.PROCESSING_TIME_IN_MS.getPreferredName(), stats.getProcessingTime());
            builder.field(IndexerJobStats.PROCESSING_TOTAL.getPreferredName(), stats.getProcessingTotal());
            builder.field(IndexerJobStats.SEARCH_FAILURES.getPreferredName(), stats.getSearchFailures());
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
            xContentFieldIfNotZero(builder, IndexerJobStats.NUM_PAGES.getPreferredName(), stats.getNumPages());
            xContentFieldIfNotZero(builder, IndexerJobStats.NUM_INPUT_DOCUMENTS.getPreferredName(), stats.getNumDocuments());
            xContentFieldIfNotZero(builder, IndexerJobStats.NUM_OUTPUT_DOCUMENTS.getPreferredName(), stats.getOutputDocuments());
            xContentFieldIfNotZero(builder, IndexerJobStats.NUM_INVOCATIONS.getPreferredName(), stats.getNumInvocations());
            xContentFieldIfNotZero(builder, IndexerJobStats.INDEX_TIME_IN_MS.getPreferredName(), stats.getIndexTime());
            xContentFieldIfNotZero(builder, IndexerJobStats.INDEX_TOTAL.getPreferredName(), stats.getIndexTotal());
            xContentFieldIfNotZero(builder, IndexerJobStats.INDEX_FAILURES.getPreferredName(), stats.getIndexFailures());
            xContentFieldIfNotZero(builder, IndexerJobStats.SEARCH_TIME_IN_MS.getPreferredName(), stats.getSearchTime());
            xContentFieldIfNotZero(builder, IndexerJobStats.SEARCH_TOTAL.getPreferredName(), stats.getSearchTotal());
            xContentFieldIfNotZero(builder, IndexerJobStats.PROCESSING_TIME_IN_MS.getPreferredName(), stats.getProcessingTime());
            xContentFieldIfNotZero(builder, IndexerJobStats.PROCESSING_TOTAL.getPreferredName(), stats.getProcessingTotal());
            xContentFieldIfNotZero(builder, IndexerJobStats.SEARCH_FAILURES.getPreferredName(), stats.getSearchFailures());
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
