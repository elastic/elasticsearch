/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.process.results.MemoryUsageEstimationResult;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MemoryUsageEstimationProcessManagerTests extends ESTestCase {

    private static final String TASK_ID = "mem_est_123";
    private static final String CONFIG_ID = "dummy";
    private static final int NUM_ROWS = 100;
    private static final int NUM_COLS = 4;
    private static final MemoryUsageEstimationResult PROCESS_RESULT = new MemoryUsageEstimationResult(
        ByteSizeValue.parseBytesSizeValue("20kB", ""),
        ByteSizeValue.parseBytesSizeValue("10kB", "")
    );

    private ExecutorService executorServiceForProcess;
    private AnalyticsProcess<MemoryUsageEstimationResult> process;
    private AnalyticsProcessFactory<MemoryUsageEstimationResult> processFactory;
    private DataFrameDataExtractor dataExtractor;
    private DataFrameDataExtractorFactory dataExtractorFactory;
    private DataFrameAnalyticsConfig dataFrameAnalyticsConfig;
    private ActionListener<MemoryUsageEstimationResult> listener;
    private ArgumentCaptor<MemoryUsageEstimationResult> resultCaptor;
    private ArgumentCaptor<Exception> exceptionCaptor;
    private MemoryUsageEstimationProcessManager processManager;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpMocks() {
        executorServiceForProcess = mock(ExecutorService.class);
        process = mock(AnalyticsProcess.class);
        when(process.readAnalyticsResults()).thenReturn(List.of(PROCESS_RESULT).iterator());
        processFactory = mock(AnalyticsProcessFactory.class);
        when(processFactory.createAnalyticsProcess(any(), any(), anyBoolean(), any(), any())).thenReturn(process);
        dataExtractor = mock(DataFrameDataExtractor.class);
        when(dataExtractor.collectDataSummary()).thenReturn(new DataFrameDataExtractor.DataSummary(NUM_ROWS, NUM_COLS));
        dataExtractorFactory = mock(DataFrameDataExtractorFactory.class);
        when(dataExtractorFactory.newExtractor(anyBoolean())).thenReturn(dataExtractor);
        when(dataExtractorFactory.getExtractedFields()).thenReturn(mock(ExtractedFields.class));
        dataFrameAnalyticsConfig = DataFrameAnalyticsConfigTests.createRandom(CONFIG_ID);
        listener = mock(ActionListener.class);
        resultCaptor = ArgumentCaptor.forClass(MemoryUsageEstimationResult.class);
        exceptionCaptor = ArgumentCaptor.forClass(Exception.class);

        processManager = new MemoryUsageEstimationProcessManager(
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            executorServiceForProcess,
            processFactory
        );
    }

    public void testRunJob_EmptyDataFrame() {
        when(dataExtractor.collectDataSummary()).thenReturn(new DataFrameDataExtractor.DataSummary(0, NUM_COLS));

        processManager.runJobAsync(TASK_ID, dataFrameAnalyticsConfig, dataExtractorFactory, listener);

        verify(listener).onFailure(exceptionCaptor.capture());
        ElasticsearchException exception = (ElasticsearchException) exceptionCaptor.getValue();
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), containsString(TASK_ID));
        assertThat(exception.getMessage(), containsString("Unable to estimate memory usage"));

        verifyNoMoreInteractions(process, listener);
    }

    public void testRunJob_NoResults() throws Exception {
        when(process.readAnalyticsResults()).thenReturn(List.<MemoryUsageEstimationResult>of().iterator());

        processManager.runJobAsync(TASK_ID, dataFrameAnalyticsConfig, dataExtractorFactory, listener);

        verify(listener).onFailure(exceptionCaptor.capture());
        ElasticsearchException exception = (ElasticsearchException) exceptionCaptor.getValue();
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), containsString(TASK_ID));
        assertThat(exception.getMessage(), containsString("no results"));

        InOrder inOrder = inOrder(process);
        inOrder.verify(process).readAnalyticsResults();
        inOrder.verify(process).readError();
        inOrder.verify(process).close();
        verifyNoMoreInteractions(process, listener);
    }

    public void testRunJob_MultipleResults() throws Exception {
        when(process.readAnalyticsResults()).thenReturn(List.of(PROCESS_RESULT, PROCESS_RESULT).iterator());

        processManager.runJobAsync(TASK_ID, dataFrameAnalyticsConfig, dataExtractorFactory, listener);

        verify(listener).onFailure(exceptionCaptor.capture());
        ElasticsearchException exception = (ElasticsearchException) exceptionCaptor.getValue();
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), containsString(TASK_ID));
        assertThat(exception.getMessage(), containsString("more than one result"));

        InOrder inOrder = inOrder(process);
        inOrder.verify(process).readAnalyticsResults();
        inOrder.verify(process).readError();
        inOrder.verify(process).close();
        verifyNoMoreInteractions(process, listener);
    }

    public void testRunJob_OneResult_ParseException() throws Exception {
        when(process.readAnalyticsResults()).thenThrow(new ElasticsearchParseException("cannot parse result"));

        processManager.runJobAsync(TASK_ID, dataFrameAnalyticsConfig, dataExtractorFactory, listener);

        verify(listener).onFailure(exceptionCaptor.capture());
        ElasticsearchException exception = (ElasticsearchException) exceptionCaptor.getValue();
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), containsString(TASK_ID));
        assertThat(exception.getMessage(), containsString("cannot parse result"));

        InOrder inOrder = inOrder(process);
        inOrder.verify(process).readAnalyticsResults();
        inOrder.verify(process).readError();
        inOrder.verify(process).close();
        verifyNoMoreInteractions(process, listener);
    }

    public void testRunJob_FailsOnClose() throws Exception {
        doThrow(ExceptionsHelper.serverError("some LOG(ERROR) lines coming from cpp process")).when(process).close();

        processManager.runJobAsync(TASK_ID, dataFrameAnalyticsConfig, dataExtractorFactory, listener);

        verify(listener).onFailure(exceptionCaptor.capture());
        ElasticsearchException exception = (ElasticsearchException) exceptionCaptor.getValue();
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), containsString(TASK_ID));
        assertThat(exception.getMessage(), containsString("Error while closing process"));

        InOrder inOrder = inOrder(process);
        inOrder.verify(process).readAnalyticsResults();
        inOrder.verify(process).close();
        inOrder.verify(process).readError();
        verifyNoMoreInteractions(process, listener);
    }

    public void testRunJob_FailsOnClose_ProcessReportsError() throws Exception {
        doThrow(ExceptionsHelper.serverError("some LOG(ERROR) lines coming from cpp process")).when(process).close();
        when(process.readError()).thenReturn("Error from inside the process");

        processManager.runJobAsync(TASK_ID, dataFrameAnalyticsConfig, dataExtractorFactory, listener);

        verify(listener).onFailure(exceptionCaptor.capture());
        ElasticsearchException exception = (ElasticsearchException) exceptionCaptor.getValue();
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), containsString(TASK_ID));
        assertThat(exception.getMessage(), containsString("Error while closing process"));
        assertThat(exception.getMessage(), containsString("some LOG(ERROR) lines coming from cpp process"));
        assertThat(exception.getMessage(), containsString("Error from inside the process"));

        InOrder inOrder = inOrder(process);
        inOrder.verify(process).readAnalyticsResults();
        inOrder.verify(process).close();
        inOrder.verify(process).readError();
        verifyNoMoreInteractions(process, listener);
    }

    public void testRunJob_Ok() throws Exception {
        processManager.runJobAsync(TASK_ID, dataFrameAnalyticsConfig, dataExtractorFactory, listener);

        verify(listener).onResponse(resultCaptor.capture());
        MemoryUsageEstimationResult result = resultCaptor.getValue();
        assertThat(result, equalTo(PROCESS_RESULT));

        InOrder inOrder = inOrder(process);
        inOrder.verify(process).readAnalyticsResults();
        inOrder.verify(process).close();
        verifyNoMoreInteractions(process, listener);
    }
}
