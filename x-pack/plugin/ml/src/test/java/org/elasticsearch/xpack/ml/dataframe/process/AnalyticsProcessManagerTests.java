/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetectionTests;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsHolder;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.Before;
import org.mockito.InOrder;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Test for the basic functionality of {@link AnalyticsProcessManager} and {@link AnalyticsProcessManager.ProcessContext}.
 * This test does not spawn any threads. Instead:
 *  - job is run on a current thread (using {@code DirectExecutorService})
 *  - {@code processData} and {@code processResults} methods are not run at all (using mock executor)
 */
public class AnalyticsProcessManagerTests extends ESTestCase {

    private static final long TASK_ALLOCATION_ID = 123;
    private static final String CONFIG_ID = "config-id";
    private static final int NUM_ROWS = 100;
    private static final int NUM_COLS = 4;
    private static final AnalyticsResult PROCESS_RESULT = new AnalyticsResult(null, null, null);

    private Client client;
    private DataFrameAnalyticsAuditor auditor;
    private TrainedModelProvider trainedModelProvider;
    private ExecutorService executorServiceForJob;
    private ExecutorService executorServiceForProcess;
    private AnalyticsProcess<AnalyticsResult> process;
    private AnalyticsProcessFactory<AnalyticsResult> processFactory;
    private DataFrameAnalyticsTask task;
    private DataFrameAnalyticsConfig dataFrameAnalyticsConfig;
    private DataFrameDataExtractorFactory dataExtractorFactory;
    private DataFrameDataExtractor dataExtractor;
    private ResultsPersisterService resultsPersisterService;
    private AnalyticsProcessManager processManager;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpMocks() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.execute(any(), any())).thenReturn(mock(ActionFuture.class));
        executorServiceForJob = EsExecutors.newDirectExecutorService();
        executorServiceForProcess = mock(ExecutorService.class);
        process = mock(AnalyticsProcess.class);
        when(process.isProcessAlive()).thenReturn(true);
        when(process.readAnalyticsResults()).thenReturn(List.of(PROCESS_RESULT).iterator());
        processFactory = mock(AnalyticsProcessFactory.class);
        when(processFactory.createAnalyticsProcess(any(), any(), any(), any(), any())).thenReturn(process);
        auditor = mock(DataFrameAnalyticsAuditor.class);
        trainedModelProvider = mock(TrainedModelProvider.class);

        task = mock(DataFrameAnalyticsTask.class);
        when(task.getAllocationId()).thenReturn(TASK_ALLOCATION_ID);
        when(task.getStatsHolder()).thenReturn(new StatsHolder());
        dataFrameAnalyticsConfig = DataFrameAnalyticsConfigTests.createRandomBuilder(CONFIG_ID,
            false,
            OutlierDetectionTests.createRandom()).build();
        dataExtractor = mock(DataFrameDataExtractor.class);
        when(dataExtractor.collectDataSummary()).thenReturn(new DataFrameDataExtractor.DataSummary(NUM_ROWS, NUM_COLS));
        dataExtractorFactory = mock(DataFrameDataExtractorFactory.class);
        when(dataExtractorFactory.newExtractor(anyBoolean())).thenReturn(dataExtractor);
        when(dataExtractorFactory.getExtractedFields()).thenReturn(mock(ExtractedFields.class));

        resultsPersisterService = mock(ResultsPersisterService.class);

        processManager = new AnalyticsProcessManager(client, executorServiceForJob, executorServiceForProcess, processFactory, auditor,
            trainedModelProvider, resultsPersisterService);
    }

    public void testRunJob_TaskIsStopping() {
        when(task.isStopping()).thenReturn(true);

        processManager.runJob(task, dataFrameAnalyticsConfig, dataExtractorFactory);
        assertThat(processManager.getProcessContextCount(), equalTo(0));

        InOrder inOrder = inOrder(task);
        inOrder.verify(task).isStopping();
        inOrder.verify(task).markAsCompleted();
        verifyNoMoreInteractions(task);
    }

    public void testRunJob_ProcessContextAlreadyExists() {
        processManager.runJob(task, dataFrameAnalyticsConfig, dataExtractorFactory);
        assertThat(processManager.getProcessContextCount(), equalTo(1));
        processManager.runJob(task, dataFrameAnalyticsConfig, dataExtractorFactory);
        assertThat(processManager.getProcessContextCount(), equalTo(1));

        InOrder inOrder = inOrder(task);
        inOrder.verify(task).isStopping();
        inOrder.verify(task).getAllocationId();
        inOrder.verify(task).isStopping();
        inOrder.verify(task).getStatsHolder();
        inOrder.verify(task).isStopping();
        inOrder.verify(task).getAllocationId();
        inOrder.verify(task).updateState(DataFrameAnalyticsState.FAILED, "[config-id] Could not create process as one already exists");
        verifyNoMoreInteractions(task);
    }

    public void testRunJob_EmptyDataFrame() {
        when(dataExtractor.collectDataSummary()).thenReturn(new DataFrameDataExtractor.DataSummary(0, NUM_COLS));

        processManager.runJob(task, dataFrameAnalyticsConfig, dataExtractorFactory);
        assertThat(processManager.getProcessContextCount(), equalTo(0));  // Make sure the process context did not leak

        InOrder inOrder = inOrder(dataExtractor, executorServiceForProcess, process, task);
        inOrder.verify(task).isStopping();
        inOrder.verify(task).getAllocationId();
        inOrder.verify(task).isStopping();
        inOrder.verify(dataExtractor).collectDataSummary();
        inOrder.verify(dataExtractor).getCategoricalFields(dataFrameAnalyticsConfig.getAnalysis());
        inOrder.verify(task).getAllocationId();
        inOrder.verify(task).markAsCompleted();
        verifyNoMoreInteractions(dataExtractor, executorServiceForProcess, process, task);
    }

    public void testRunJob_Ok() {
        processManager.runJob(task, dataFrameAnalyticsConfig, dataExtractorFactory);
        assertThat(processManager.getProcessContextCount(), equalTo(1));

        InOrder inOrder = inOrder(dataExtractor, executorServiceForProcess, process, task);
        inOrder.verify(task).isStopping();
        inOrder.verify(task).getAllocationId();
        inOrder.verify(task).isStopping();
        inOrder.verify(dataExtractor).collectDataSummary();
        inOrder.verify(dataExtractor).getCategoricalFields(dataFrameAnalyticsConfig.getAnalysis());
        inOrder.verify(process).isProcessAlive();
        inOrder.verify(task).getStatsHolder();
        inOrder.verify(dataExtractor).getFieldNames();
        inOrder.verify(executorServiceForProcess, times(2)).execute(any());  // 'processData' and 'processResults' threads
        verifyNoMoreInteractions(dataExtractor, executorServiceForProcess, process, task);
    }

    public void testProcessContext_GetSetFailureReason() {
        AnalyticsProcessManager.ProcessContext processContext = processManager.new ProcessContext(dataFrameAnalyticsConfig);
        assertThat(processContext.getFailureReason(), is(nullValue()));

        processContext.setFailureReason("reason1");
        assertThat(processContext.getFailureReason(), equalTo("reason1"));

        processContext.setFailureReason(null);
        assertThat(processContext.getFailureReason(), equalTo("reason1"));

        processContext.setFailureReason("reason2");
        assertThat(processContext.getFailureReason(), equalTo("reason1"));

        verifyNoMoreInteractions(dataExtractor, process, task);
    }

    public void testProcessContext_StartProcess_TaskAlreadyStopped() {
        when(task.isStopping()).thenReturn(true);

        AnalyticsProcessManager.ProcessContext processContext = processManager.new ProcessContext(dataFrameAnalyticsConfig);
        processContext.stop();
        assertThat(processContext.startProcess(dataExtractorFactory, task, null), is(false));

        InOrder inOrder = inOrder(dataExtractor, process, task);
        inOrder.verify(task).isStopping();
        verifyNoMoreInteractions(dataExtractor, process, task);
    }

    public void testProcessContext_StartProcess_EmptyDataFrame() {
        when(dataExtractor.collectDataSummary()).thenReturn(new DataFrameDataExtractor.DataSummary(0, NUM_COLS));

        AnalyticsProcessManager.ProcessContext processContext = processManager.new ProcessContext(dataFrameAnalyticsConfig);
        assertThat(processContext.startProcess(dataExtractorFactory, task, null), is(false));

        InOrder inOrder = inOrder(dataExtractor, process, task);
        inOrder.verify(task).isStopping();
        inOrder.verify(dataExtractor).collectDataSummary();
        inOrder.verify(dataExtractor).getCategoricalFields(dataFrameAnalyticsConfig.getAnalysis());
        verifyNoMoreInteractions(dataExtractor, process, task);
    }

    public void testProcessContext_StartAndStop() throws Exception {
        AnalyticsProcessManager.ProcessContext processContext = processManager.new ProcessContext(dataFrameAnalyticsConfig);
        assertThat(processContext.startProcess(dataExtractorFactory, task, null), is(true));
        processContext.stop();

        InOrder inOrder = inOrder(dataExtractor, process, task);
        // startProcess
        inOrder.verify(task).isStopping();
        inOrder.verify(dataExtractor).collectDataSummary();
        inOrder.verify(dataExtractor).getCategoricalFields(dataFrameAnalyticsConfig.getAnalysis());
        inOrder.verify(process).isProcessAlive();
        inOrder.verify(task).getStatsHolder();
        inOrder.verify(dataExtractor).getFieldNames();
        // stop
        inOrder.verify(dataExtractor).cancel();
        inOrder.verify(process).kill();
        verifyNoMoreInteractions(dataExtractor, process, task);
    }

    public void testProcessContext_Stop() {
        AnalyticsProcessManager.ProcessContext processContext = processManager.new ProcessContext(dataFrameAnalyticsConfig);
        processContext.stop();

        verifyNoMoreInteractions(dataExtractor, process, task);
    }
}
