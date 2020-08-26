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

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.client.ml.NodeAttributesTests;
import org.elasticsearch.client.ml.dataframe.stats.AnalysisStats;
import org.elasticsearch.client.ml.dataframe.stats.AnalysisStatsNamedXContentProvider;
import org.elasticsearch.client.ml.dataframe.stats.classification.ClassificationStatsTests;
import org.elasticsearch.client.ml.dataframe.stats.common.DataCountsTests;
import org.elasticsearch.client.ml.dataframe.stats.common.MemoryUsageTests;
import org.elasticsearch.client.ml.dataframe.stats.outlierdetection.OutlierDetectionStatsTests;
import org.elasticsearch.client.ml.dataframe.stats.regression.RegressionStatsTests;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class DataFrameAnalyticsStatsTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new AnalysisStatsNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            DataFrameAnalyticsStatsTests::randomDataFrameAnalyticsStats,
            DataFrameAnalyticsStatsTests::toXContent,
            DataFrameAnalyticsStats::fromXContent)
            .supportsUnknownFields(true)
            .randomFieldsExcludeFilter(field -> field.startsWith("node.attributes") || field.startsWith("analysis_stats"))
            .test();
    }

    public static DataFrameAnalyticsStats randomDataFrameAnalyticsStats() {
        AnalysisStats analysisStats = randomBoolean() ? null :
            randomFrom(
                ClassificationStatsTests.createRandom(),
                OutlierDetectionStatsTests.createRandom(),
                RegressionStatsTests.createRandom()
            );

        return new DataFrameAnalyticsStats(
            randomAlphaOfLengthBetween(1, 10),
            randomFrom(DataFrameAnalyticsState.values()),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : createRandomProgress(),
            randomBoolean() ? null : DataCountsTests.createRandom(),
            randomBoolean() ? null : MemoryUsageTests.createRandom(),
            analysisStats,
            randomBoolean() ? null : NodeAttributesTests.createRandom(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20));
    }

    private static List<PhaseProgress> createRandomProgress() {
        int progressPhaseCount = randomIntBetween(3, 7);
        List<PhaseProgress> progress = new ArrayList<>(progressPhaseCount);
        for (int i = 0; i < progressPhaseCount; i++) {
            progress.add(new PhaseProgress(randomAlphaOfLength(20), randomIntBetween(0, 100)));
        }
        return progress;
    }

    public static void toXContent(DataFrameAnalyticsStats stats, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(DataFrameAnalyticsStats.ID.getPreferredName(), stats.getId());
        builder.field(DataFrameAnalyticsStats.STATE.getPreferredName(), stats.getState().value());
        if (stats.getFailureReason() != null) {
            builder.field(DataFrameAnalyticsStats.FAILURE_REASON.getPreferredName(), stats.getFailureReason());
        }
        if (stats.getProgress() != null) {
            builder.field(DataFrameAnalyticsStats.PROGRESS.getPreferredName(), stats.getProgress());
        }
        if (stats.getDataCounts() != null) {
            builder.field(DataFrameAnalyticsStats.DATA_COUNTS.getPreferredName(), stats.getDataCounts());
        }
        if (stats.getMemoryUsage() != null) {
            builder.field(DataFrameAnalyticsStats.MEMORY_USAGE.getPreferredName(), stats.getMemoryUsage());
        }
        if (stats.getAnalysisStats() != null) {
            builder.startObject("analysis_stats");
            builder.field(stats.getAnalysisStats().getName(), stats.getAnalysisStats());
            builder.endObject();
        }
        if (stats.getNode() != null) {
            builder.field(DataFrameAnalyticsStats.NODE.getPreferredName(), stats.getNode());
        }
        if (stats.getAssignmentExplanation() != null) {
            builder.field(DataFrameAnalyticsStats.ASSIGNMENT_EXPLANATION.getPreferredName(), stats.getAssignmentExplanation());
        }
        builder.endObject();
    }
}
