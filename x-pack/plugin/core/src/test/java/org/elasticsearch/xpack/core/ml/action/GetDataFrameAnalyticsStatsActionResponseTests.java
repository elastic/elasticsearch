/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction.Response;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.stats.AnalysisStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.AnalysisStatsNamedWriteablesProvider;
import org.elasticsearch.xpack.core.ml.dataframe.stats.classification.ValidationLoss;
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
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class GetDataFrameAnalyticsStatsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.addAll(new AnalysisStatsNamedWriteablesProvider().getNamedWriteables());
        return new NamedWriteableRegistry(namedWriteables);
    }

    public static Response randomResponse(int listSize) {
        return randomResponse(
            listSize,
            () -> randomBoolean()
                ? null
                : randomFrom(
                    ClassificationStatsTests.createRandom(),
                    OutlierDetectionStatsTests.createRandom(),
                    RegressionStatsTests.createRandom()));
    }

    private static Response randomResponse(int listSize, Supplier<AnalysisStats> analysisStatsSupplier) {
        List<Response.Stats> analytics = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String failureReason = randomBoolean() ? null : randomAlphaOfLength(10);
            int progressSize = randomIntBetween(2, 5);
            List<PhaseProgress> progress = new ArrayList<>(progressSize);
            IntStream.of(progressSize).forEach(progressIndex -> progress.add(
                new PhaseProgress(randomAlphaOfLength(10), randomIntBetween(0, 100))));
            DataCounts dataCounts = randomBoolean() ? null : DataCountsTests.createRandom();
            MemoryUsage memoryUsage = randomBoolean() ? null : MemoryUsageTests.createRandom();
            AnalysisStats analysisStats = analysisStatsSupplier.get();
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

    public void testVerbose() {
        String foldValuesFieldName = ValidationLoss.FOLD_VALUES.getPreferredName();
        // Create response for supervised analysis that is certain to contain fold_values field
        Response response =
            randomResponse(1, () -> randomFrom(ClassificationStatsTests.createRandom(), RegressionStatsTests.createRandom()));

        // VERBOSE param defaults to "false", fold values *not* outputted
        assertThat(Strings.toString(response), not(containsString(foldValuesFieldName)));

        // VERBOSE param explicitly set to "false", fold values *not* outputted
        assertThat(
            Strings.toString(response, new ToXContent.MapParams(Collections.singletonMap(Response.VERBOSE, "false"))),
            not(containsString(foldValuesFieldName)));

        // VERBOSE param explicitly set to "true", fold values outputted
        assertThat(
            Strings.toString(response, new ToXContent.MapParams(Collections.singletonMap(Response.VERBOSE, "true"))),
            containsString(foldValuesFieldName));
    }
}
