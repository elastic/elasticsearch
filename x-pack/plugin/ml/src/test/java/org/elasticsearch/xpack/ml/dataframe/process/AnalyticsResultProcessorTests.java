/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask.ProgressTracker;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;
import org.junit.Before;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AnalyticsResultProcessorTests extends ESTestCase {

    private static final String JOB_ID = "analytics-result-processor-tests";

    private AnalyticsProcess<AnalyticsResult> process;
    private DataFrameRowsJoiner dataFrameRowsJoiner;
    private ProgressTracker progressTracker = new ProgressTracker();

    @Before
    @SuppressWarnings("unchecked")
    public void setUpMocks() {
        process = mock(AnalyticsProcess.class);
        dataFrameRowsJoiner = mock(DataFrameRowsJoiner.class);
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
        givenProcessResults(Arrays.asList(new AnalyticsResult(null, 50), new AnalyticsResult(null, 100)));
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        verify(dataFrameRowsJoiner).close();
        Mockito.verifyNoMoreInteractions(dataFrameRowsJoiner);
        assertThat(progressTracker.writingResultsPercent.get(), equalTo(100));
    }

    public void testProcess_GivenRowResults() {
        givenDataFrameRows(2);
        RowResults rowResults1 = mock(RowResults.class);
        RowResults rowResults2 = mock(RowResults.class);
        givenProcessResults(Arrays.asList(new AnalyticsResult(rowResults1, 50), new AnalyticsResult(rowResults2, 100)));
        AnalyticsResultProcessor resultProcessor = createResultProcessor();

        resultProcessor.process(process);
        resultProcessor.awaitForCompletion();

        InOrder inOrder = Mockito.inOrder(dataFrameRowsJoiner);
        inOrder.verify(dataFrameRowsJoiner).processRowResults(rowResults1);
        inOrder.verify(dataFrameRowsJoiner).processRowResults(rowResults2);

        assertThat(progressTracker.writingResultsPercent.get(), equalTo(100));
    }

    private void givenProcessResults(List<AnalyticsResult> results) {
        when(process.readAnalyticsResults()).thenReturn(results.iterator());
    }

    private void givenDataFrameRows(int rows) {
        AnalyticsProcessConfig config = new AnalyticsProcessConfig(
            rows, 1, ByteSizeValue.ZERO, 1, "ml", Collections.emptySet(), mock(DataFrameAnalysis.class));
        when(process.getConfig()).thenReturn(config);
    }

    private AnalyticsResultProcessor createResultProcessor() {
        return new AnalyticsResultProcessor(JOB_ID, dataFrameRowsJoiner, () -> false, progressTracker);
    }
}
