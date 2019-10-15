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
                TransformIndexerStats::fromXContent)
                .supportsUnknownFields(true)
                .test();
    }

    public static TransformIndexerStats randomStats() {
        return new TransformIndexerStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble());
    }

    public static void toXContent(TransformIndexerStats stats, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(IndexerJobStats.NUM_PAGES.getPreferredName(), stats.getNumPages());
        builder.field(IndexerJobStats.NUM_INPUT_DOCUMENTS.getPreferredName(), stats.getNumDocuments());
        builder.field(IndexerJobStats.NUM_OUTPUT_DOCUMENTS.getPreferredName(), stats.getOutputDocuments());
        builder.field(IndexerJobStats.NUM_INVOCATIONS.getPreferredName(), stats.getNumInvocations());
        builder.field(IndexerJobStats.INDEX_TIME_IN_MS.getPreferredName(), stats.getIndexTime());
        builder.field(IndexerJobStats.INDEX_TOTAL.getPreferredName(), stats.getIndexTotal());
        builder.field(IndexerJobStats.INDEX_FAILURES.getPreferredName(), stats.getIndexFailures());
        builder.field(IndexerJobStats.SEARCH_TIME_IN_MS.getPreferredName(), stats.getSearchTime());
        builder.field(IndexerJobStats.SEARCH_TOTAL.getPreferredName(), stats.getSearchTotal());
        builder.field(IndexerJobStats.SEARCH_FAILURES.getPreferredName(), stats.getSearchFailures());
        builder.field(TransformIndexerStats.EXPONENTIAL_AVG_CHECKPOINT_DURATION_MS.getPreferredName(),
            stats.getExpAvgCheckpointDurationMs());
        builder.field(TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_INDEXED.getPreferredName(),
            stats.getExpAvgDocumentsIndexed());
        builder.field(TransformIndexerStats.EXPONENTIAL_AVG_DOCUMENTS_PROCESSED.getPreferredName(),
            stats.getExpAvgDocumentsProcessed());
        builder.endObject();
    }
}
