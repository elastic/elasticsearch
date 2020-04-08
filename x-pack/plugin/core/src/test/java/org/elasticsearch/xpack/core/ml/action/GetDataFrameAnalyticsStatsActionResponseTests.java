/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction.Response;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.stats.AnalysisStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.AnalysisStatsNamedWriteablesProvider;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsageTests;
import org.elasticsearch.xpack.core.ml.dataframe.stats.classification.ClassificationStatsTests;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCountsTests;
import org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection.OutlierDetectionStatsTests;
import org.elasticsearch.xpack.core.ml.dataframe.stats.regression.RegressionStatsTests;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class GetDataFrameAnalyticsStatsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.addAll(new AnalysisStatsNamedWriteablesProvider().getNamedWriteables());
        return new NamedWriteableRegistry(namedWriteables);
    }

    public static Response randomResponse(int listSize) {
        List<Response.Stats> analytics = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String failureReason = randomBoolean() ? null : randomAlphaOfLength(10);
            int progressSize = randomIntBetween(2, 5);
            List<PhaseProgress> progress = new ArrayList<>(progressSize);
            IntStream.of(progressSize).forEach(progressIndex -> progress.add(
                new PhaseProgress(randomAlphaOfLength(10), randomIntBetween(0, 100))));
            DataCounts dataCounts = randomBoolean() ? null : DataCountsTests.createRandom();
            MemoryUsage memoryUsage = randomBoolean() ? null : MemoryUsageTests.createRandom();
            AnalysisStats analysisStats = randomBoolean() ? null :
                randomFrom(
                    ClassificationStatsTests.createRandom(),
                    OutlierDetectionStatsTests.createRandom(),
                    RegressionStatsTests.createRandom()
                );
            Response.Stats stats = new Response.Stats(DataFrameAnalyticsConfigTests.randomValidId(),
                randomFrom(DataFrameAnalyticsState.values()), failureReason, progress, dataCounts, memoryUsage, analysisStats, null,
                randomAlphaOfLength(20));
            analytics.add(stats);
        }
        return new Response(new QueryPage<>(analytics, analytics.size(), GetDataFrameAnalyticsAction.Response.RESULTS_FIELD));
    }

    @Override
    protected Response createTestInstance() {
        return randomResponse(randomInt(10));
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    public void testStats_GivenNulls() {
        Response.Stats stats = new Response.Stats(randomAlphaOfLength(10),
            randomFrom(DataFrameAnalyticsState.values()),
            null,
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            null
        );

        assertThat(stats.getDataCounts(), equalTo(new DataCounts(stats.getId())));
        assertThat(stats.getMemoryUsage(), equalTo(new MemoryUsage(stats.getId())));
    }
}
