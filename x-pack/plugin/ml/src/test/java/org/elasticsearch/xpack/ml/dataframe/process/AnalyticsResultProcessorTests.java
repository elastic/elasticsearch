/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.dataframe.stats.classification.ClassificationStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.classification.ClassificationStatsTests;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection.OutlierDetectionStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection.OutlierDetectionStatsTests;
import org.elasticsearch.xpack.core.ml.dataframe.stats.regression.RegressionStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.regression.RegressionStatsTests;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;
import org.elasticsearch.xpack.ml.dataframe.process.results.TrainedModelDefinitionChunk;
import org.elasticsearch.xpack.ml.dataframe.stats.ProgressTracker;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsHolder;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsPersister;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AnalyticsResultProcessorTests extends ESTestCase {

    private static final String JOB_ID = "analytics-result-processor-tests";
    private static final String JOB_DESCRIPTION = "This describes the job of these tests";

    private AnalyticsProcess<AnalyticsResult> process;
    private DataFrameRowsJoiner dataFrameRowsJoiner;
    private StatsHolder statsHolder = new StatsHolder(
        ProgressTracker.fromZeroes(Collections.singletonList("analyzing"), false).report(),
        null,
        null,
        new DataCounts(JOB_ID)
    );
    private TrainedModelProvider trainedModelProvider;
    private DataFrameAnalyticsAuditor auditor;
    private StatsPersister statsPersister;
    private DataFrameAnalyticsConfig analyticsConfig;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpMocks() {
        process = mock(AnalyticsProcess.class);
        dataFrameRowsJoiner = mock(DataFrameRowsJoiner.class);
        trainedModelProvider = mock(TrainedModelProvider.class);
        auditor = mock(DataFrameAnalyticsAuditor.class);
        statsPersister = mock(StatsPersister.class);
        analyticsConfig = new DataFrameAnalyticsConfig.Builder().setId(JOB_ID)
            .setDescription(JOB_DESCRIPTION)
            .setSource(new DataFrameAnalyticsSource(new String[] { "my_source" }, null, null, null))
            .setDest(new DataFrameAnalyticsDest("my_dest", null))
            .setAnalysis(new Regression("foo"))
            .build();
    }

    public void testProcess_GivenNoResults() {
        givenDataFrameRows(0);
        givenProcessResults(Collections.emptyList());
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verify(dataFrameRowsJoiner).close();
        verifyNoMoreInteractions(dataFrameRowsJoiner);
    }

    public void testProcess_GivenEmptyResults() {
        givenDataFrameRows(2);
        givenProcessResults(Arrays.asList(AnalyticsResult.builder().build(), AnalyticsResult.builder().build()));
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verify(dataFrameRowsJoiner).close();
        Mockito.verifyNoMoreInteractions(dataFrameRowsJoiner);
        assertThat(statsHolder.getProgressTracker().getWritingResultsProgressPercent(), equalTo(100));
    }

    public void testProcess_GivenRowResults() {
        givenDataFrameRows(2);
        RowResults rowResults1 = mock(RowResults.class);
        RowResults rowResults2 = mock(RowResults.class);
        givenProcessResults(
            Arrays.asList(
                AnalyticsResult.builder().setRowResults(rowResults1).build(),
                AnalyticsResult.builder().setRowResults(rowResults2).build()
            )
        );
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        InOrder inOrder = Mockito.inOrder(dataFrameRowsJoiner);
        inOrder.verify(dataFrameRowsJoiner).processRowResults(rowResults1);
        inOrder.verify(dataFrameRowsJoiner).processRowResults(rowResults2);

        assertThat(statsHolder.getProgressTracker().getWritingResultsProgressPercent(), equalTo(100));
    }

    public void testProcess_GivenDataFrameRowsJoinerFails() {
        givenDataFrameRows(2);
        RowResults rowResults1 = mock(RowResults.class);
        RowResults rowResults2 = mock(RowResults.class);
        givenProcessResults(
            Arrays.asList(
                AnalyticsResult.builder().setRowResults(rowResults1).build(),
                AnalyticsResult.builder().setRowResults(rowResults2).build()
            )
        );

        doThrow(new RuntimeException("some failure")).when(dataFrameRowsJoiner).processRowResults(any(RowResults.class));

        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        assertThat(resultProcessor.getFailure(), equalTo("error processing results; some failure"));

        ArgumentCaptor<String> auditCaptor = ArgumentCaptor.forClass(String.class);
        verify(auditor).error(eq(JOB_ID), auditCaptor.capture());
        assertThat(auditCaptor.getValue(), containsString("Error processing results; some failure"));

        assertThat(statsHolder.getProgressTracker().getWritingResultsProgressPercent(), equalTo(0));
    }

    public void testCancel_GivenRowResults() {
        givenDataFrameRows(2);
        RowResults rowResults1 = mock(RowResults.class);
        RowResults rowResults2 = mock(RowResults.class);
        givenProcessResults(
            Arrays.asList(
                AnalyticsResult.builder().setRowResults(rowResults1).build(),
                AnalyticsResult.builder().setRowResults(rowResults2).build()
            )
        );
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.cancel();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verify(dataFrameRowsJoiner).cancel();
        verify(dataFrameRowsJoiner).close();
        Mockito.verifyNoMoreInteractions(dataFrameRowsJoiner, trainedModelProvider);
        assertThat(statsHolder.getProgressTracker().getWritingResultsProgressPercent(), equalTo(0));
    }

    public void testCancel_GivenModelChunk() {
        givenDataFrameRows(2);
        TrainedModelDefinitionChunk modelChunk = mock(TrainedModelDefinitionChunk.class);
        givenProcessResults(Arrays.asList(AnalyticsResult.builder().setTrainedModelDefinitionChunk(modelChunk).build()));
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.cancel();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verify(dataFrameRowsJoiner).cancel();
        verify(dataFrameRowsJoiner).close();
        Mockito.verifyNoMoreInteractions(dataFrameRowsJoiner, trainedModelProvider);
        assertThat(statsHolder.getProgressTracker().getWritingResultsProgressPercent(), equalTo(0));
    }

    public void testCancel_GivenPhaseProgress() {
        givenDataFrameRows(2);
        PhaseProgress phaseProgress = new PhaseProgress("analyzing", 18);
        givenProcessResults(Arrays.asList(AnalyticsResult.builder().setPhaseProgress(phaseProgress).build()));
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.cancel();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verify(dataFrameRowsJoiner).cancel();
        verify(dataFrameRowsJoiner).close();
        Mockito.verifyNoMoreInteractions(dataFrameRowsJoiner, trainedModelProvider);
        assertThat(statsHolder.getProgressTracker().getWritingResultsProgressPercent(), equalTo(0));

        Optional<PhaseProgress> testPhaseProgress = statsHolder.getProgressTracker()
            .report()
            .stream()
            .filter(p -> p.getPhase().equals(phaseProgress.getPhase()))
            .findAny();
        assertThat(testPhaseProgress.isPresent(), is(true));
        assertThat(testPhaseProgress.get().getProgressPercent(), equalTo(18));
    }

    public void testCancel_GivenMemoryUsage() {
        givenDataFrameRows(2);
        MemoryUsage memoryUsage = new MemoryUsage(analyticsConfig.getId(), Instant.now(), 1000L, MemoryUsage.Status.HARD_LIMIT, null);
        givenProcessResults(Arrays.asList(AnalyticsResult.builder().setMemoryUsage(memoryUsage).build()));
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.cancel();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verify(dataFrameRowsJoiner).cancel();
        verify(dataFrameRowsJoiner).close();
        Mockito.verifyNoMoreInteractions(dataFrameRowsJoiner, trainedModelProvider);
        assertThat(statsHolder.getProgressTracker().getWritingResultsProgressPercent(), equalTo(0));

        assertThat(statsHolder.getMemoryUsage(), equalTo(memoryUsage));
        verify(statsPersister).persistWithRetry(eq(memoryUsage), any());
    }

    public void testCancel_GivenOutlierDetectionStats() {
        givenDataFrameRows(2);
        OutlierDetectionStats outlierDetectionStats = OutlierDetectionStatsTests.createRandom();
        givenProcessResults(Arrays.asList(AnalyticsResult.builder().setOutlierDetectionStats(outlierDetectionStats).build()));
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.cancel();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verify(dataFrameRowsJoiner).cancel();
        verify(dataFrameRowsJoiner).close();
        Mockito.verifyNoMoreInteractions(dataFrameRowsJoiner, trainedModelProvider);
        assertThat(statsHolder.getProgressTracker().getWritingResultsProgressPercent(), equalTo(0));

        assertThat(statsHolder.getAnalysisStats(), equalTo(outlierDetectionStats));
        verify(statsPersister).persistWithRetry(eq(outlierDetectionStats), any());
    }

    public void testCancel_GivenClassificationStats() {
        givenDataFrameRows(2);
        ClassificationStats classificationStats = ClassificationStatsTests.createRandom();
        givenProcessResults(Arrays.asList(AnalyticsResult.builder().setClassificationStats(classificationStats).build()));
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.cancel();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verify(dataFrameRowsJoiner).cancel();
        verify(dataFrameRowsJoiner).close();
        Mockito.verifyNoMoreInteractions(dataFrameRowsJoiner, trainedModelProvider);
        assertThat(statsHolder.getProgressTracker().getWritingResultsProgressPercent(), equalTo(0));

        assertThat(statsHolder.getAnalysisStats(), equalTo(classificationStats));
        verify(statsPersister).persistWithRetry(eq(classificationStats), any());
    }

    public void testCancel_GivenRegressionStats() {
        givenDataFrameRows(2);
        RegressionStats regressionStats = RegressionStatsTests.createRandom();
        givenProcessResults(Arrays.asList(AnalyticsResult.builder().setRegressionStats(regressionStats).build()));
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.cancel();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verify(dataFrameRowsJoiner).cancel();
        verify(dataFrameRowsJoiner).close();
        Mockito.verifyNoMoreInteractions(dataFrameRowsJoiner, trainedModelProvider);
        assertThat(statsHolder.getProgressTracker().getWritingResultsProgressPercent(), equalTo(0));

        assertThat(statsHolder.getAnalysisStats(), equalTo(regressionStats));
        verify(statsPersister).persistWithRetry(eq(regressionStats), any());
    }

    private void givenProcessResults(List<AnalyticsResult> results) {
        when(process.readAnalyticsResults()).thenReturn(results.iterator());
    }

    private void givenDataFrameRows(int rows) {
        AnalyticsProcessConfig config = new AnalyticsProcessConfig(
            "job_id",
            rows,
            1,
            ByteSizeValue.ZERO,
            1,
            "ml",
            Collections.emptySet(),
            mock(DataFrameAnalysis.class),
            mock(ExtractedFields.class)
        );
        when(process.getConfig()).thenReturn(config);
    }

    private AnalyticsResultProcessor createResultProcessor() {
        return createResultProcessor(Collections.emptyList());
    }

    private AnalyticsResultProcessor createResultProcessor(List<ExtractedField> fieldNames) {
        return new AnalyticsResultProcessor(
            analyticsConfig,
            dataFrameRowsJoiner,
            statsHolder,
            trainedModelProvider,
            auditor,
            statsPersister,
            new ExtractedFields(fieldNames, Collections.emptyList(), Collections.emptyMap())
        );
    }
}
