/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.manager;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.UpdateJobStatusAction;
import org.elasticsearch.xpack.prelert.job.AnalysisConfig;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.DataDescription;
import org.elasticsearch.xpack.prelert.job.Detector;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.data.DataProcessor;
import org.elasticsearch.xpack.prelert.job.metadata.Allocation;
import org.elasticsearch.xpack.prelert.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.prelert.job.process.autodetect.AutodetectCommunicator;
import org.elasticsearch.xpack.prelert.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.prelert.job.process.autodetect.AutodetectProcessFactory;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.AutodetectResultsParser;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.TimeRange;
import org.elasticsearch.xpack.prelert.job.results.AutodetectResult;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.mock.orig.Mockito.doAnswer;
import static org.elasticsearch.mock.orig.Mockito.doThrow;
import static org.elasticsearch.mock.orig.Mockito.times;
import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Calling the {@link DataProcessor#processData(String, InputStream, DataLoadParams, java.util.function.Supplier)}
 * method causes an AutodetectCommunicator to be created on demand. Most of these tests have to
 * do that before they can assert other things
 */
public class AutodetectProcessManagerTests extends ESTestCase {

    private JobManager jobManager;
    private JobProvider jobProvider;
    private JobResultsPersister jobResultsPersister;
    private JobDataCountsPersister jobDataCountsPersister;

    @Before
    public void initMocks() {
        jobManager = mock(JobManager.class);
        jobProvider = mock(JobProvider.class);
        jobResultsPersister = mock(JobResultsPersister.class);
        jobDataCountsPersister = mock(JobDataCountsPersister.class);
        givenAllocationWithStatus(JobStatus.OPENED);
    }

    public void testOpenJob() {
        Client client = mock(Client.class);
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        when(jobManager.getJobOrThrowIfUnknown("foo")).thenReturn(createJobDetails("foo"));
        AutodetectProcessManager manager = createManager(communicator, client);

        manager.openJob("foo", false);
        assertEquals(1, manager.numberOfOpenJobs());
        assertTrue(manager.jobHasActiveAutodetectProcess("foo"));
        UpdateJobStatusAction.Request expectedRequest = new UpdateJobStatusAction.Request("foo", JobStatus.OPENED);
        verify(client).execute(eq(UpdateJobStatusAction.INSTANCE), eq(expectedRequest), any());
    }

    public void testOpenJob_exceedMaxNumJobs() {
        when(jobManager.getJobOrThrowIfUnknown("foo")).thenReturn(createJobDetails("foo"));
        when(jobProvider.dataCounts("foo")).thenReturn(new DataCounts("foo"));
        when(jobManager.getJobOrThrowIfUnknown("bar")).thenReturn(createJobDetails("bar"));
        when(jobProvider.dataCounts("bar")).thenReturn(new DataCounts("bar"));
        when(jobManager.getJobOrThrowIfUnknown("baz")).thenReturn(createJobDetails("baz"));
        when(jobProvider.dataCounts("baz")).thenReturn(new DataCounts("baz"));
        when(jobManager.getJobOrThrowIfUnknown("foobar")).thenReturn(createJobDetails("foobar"));
        when(jobProvider.dataCounts("foobar")).thenReturn(new DataCounts("foobar"));

        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadPool.Cancellable  cancellable = mock(ThreadPool.Cancellable.class);
        when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(cancellable);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(PrelertPlugin.AUTODETECT_PROCESS_THREAD_POOL_NAME)).thenReturn(executorService);
        AutodetectResultsParser parser = mock(AutodetectResultsParser.class);
        @SuppressWarnings("unchecked")
        Stream<AutodetectResult> stream = mock(Stream.class);
        @SuppressWarnings("unchecked")
        Iterator<AutodetectResult> iterator = mock(Iterator.class);
        when(stream.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);
        when(parser.parseResults(any())).thenReturn(stream);
        AutodetectProcess autodetectProcess = mock(AutodetectProcess.class);
        when(autodetectProcess.isProcessAlive()).thenReturn(true);
        when(autodetectProcess.getPersistStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
        AutodetectProcessFactory autodetectProcessFactory = (j, i, e) -> autodetectProcess;
        Settings.Builder settings = Settings.builder();
        settings.put(AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.getKey(), 3);
        Set<Setting<?>> settingSet = new HashSet<>();
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingSet);
        AutodetectProcessManager manager = new AutodetectProcessManager(settings.build(), client, threadPool,
                jobManager, jobProvider, jobResultsPersister, jobDataCountsPersister, parser, autodetectProcessFactory, clusterSettings);

        manager.openJob("foo", false);
        manager.openJob("bar", false);
        manager.openJob("baz", false);
        assertEquals(3, manager.numberOfOpenJobs());

        Exception e = expectThrows(ElasticsearchStatusException.class, () -> manager.openJob("foobar", false));
        assertEquals("max running job capacity [3] reached", e.getMessage());

        manager.closeJob("baz");
        assertEquals(2, manager.numberOfOpenJobs());
        manager.openJob("foobar", false);
        assertEquals(3, manager.numberOfOpenJobs());
    }

    public void testProcessData()  {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);
        assertEquals(0, manager.numberOfOpenJobs());

        DataLoadParams params = new DataLoadParams(TimeRange.builder().build());
        manager.openJob("foo", false);
        manager.processData("foo", createInputStream(""), params, () -> false);
        assertEquals(1, manager.numberOfOpenJobs());
    }

    public void testProcessDataThrowsElasticsearchStatusException_onIoException() throws Exception {
        AutodetectCommunicator communicator = Mockito.mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);

        DataLoadParams params = mock(DataLoadParams.class);
        InputStream inputStream = createInputStream("");
        Supplier<Boolean> cancellable = () -> false;
        doThrow(new IOException("blah")).when(communicator).writeToJob(inputStream, params, cancellable);

        manager.openJob("foo", false);
        ESTestCase.expectThrows(ElasticsearchException.class,
                () -> manager.processData("foo", inputStream, params, cancellable));
    }

    public void testCloseJob() {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        when(jobManager.getJobOrThrowIfUnknown("foo")).thenReturn(createJobDetails("foo"));
        AutodetectProcessManager manager = createManager(communicator);
        assertEquals(0, manager.numberOfOpenJobs());

        manager.openJob("foo", false);
        manager.processData("foo", createInputStream(""), mock(DataLoadParams.class), () -> false);

        // job is created
        assertEquals(1, manager.numberOfOpenJobs());
        manager.closeJob("foo");
        assertEquals(0, manager.numberOfOpenJobs());
    }

    public void testBucketResetMessageIsSent() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);

        Supplier<Boolean> cancellable = () -> false;
        DataLoadParams params = new DataLoadParams(TimeRange.builder().startTime("1000").endTime("2000").build(), true);
        InputStream inputStream = createInputStream("");
        manager.openJob("foo", false);
        manager.processData("foo", inputStream, params, cancellable);
        verify(communicator).writeToJob(inputStream, params, cancellable);
    }

    public void testFlush() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);
        when(jobManager.getJobOrThrowIfUnknown("foo")).thenReturn(createJobDetails("foo"));

        InputStream inputStream = createInputStream("");
        manager.openJob("foo", false);
        manager.processData("foo", inputStream, mock(DataLoadParams.class), () -> false);

        InterimResultsParams params = InterimResultsParams.builder().build();
        manager.flushJob("foo", params);

        verify(communicator).flushJob(params);
    }

    public void testFlushThrows() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManagerAndCallProcessData(communicator, "foo");

        InterimResultsParams params = InterimResultsParams.builder().build();
        doThrow(new IOException("blah")).when(communicator).flushJob(params);

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, () -> manager.flushJob("foo", params));
        assertEquals("[foo] exception while flushing job", e.getMessage());
    }

    public void testWriteUpdateConfigMessage() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManagerAndCallProcessData(communicator, "foo");
        manager.writeUpdateConfigMessage("foo", "go faster");
        verify(communicator).writeUpdateConfigMessage("go faster");
    }

    public void testJobHasActiveAutodetectProcess() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);
        assertFalse(manager.jobHasActiveAutodetectProcess("foo"));

        manager.openJob("foo", false);
        manager.processData("foo", createInputStream(""), mock(DataLoadParams.class), () -> false);

        assertTrue(manager.jobHasActiveAutodetectProcess("foo"));
        assertFalse(manager.jobHasActiveAutodetectProcess("bar"));
    }

    public void testProcessData_GivenStatusNotStarted() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        when(communicator.writeToJob(any(), any(), any())).thenReturn(new DataCounts("foo"));
        AutodetectProcessManager manager = createManager(communicator);

        Job job = createJobDetails("foo");

        when(jobManager.getJobOrThrowIfUnknown("foo")).thenReturn(job);
        givenAllocationWithStatus(JobStatus.OPENED);

        InputStream inputStream = createInputStream("");
        manager.openJob("foo", false);
        DataCounts dataCounts = manager.processData("foo", inputStream, mock(DataLoadParams.class), () -> false);

        assertThat(dataCounts, equalTo(new DataCounts("foo")));
    }

    public void testCreate_notEnoughThreads() throws IOException {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doThrow(new EsRejectedExecutionException("")).when(executorService).execute(any());
        when(threadPool.executor(anyString())).thenReturn(executorService);
        when(jobManager.getJobOrThrowIfUnknown("_id")).thenReturn(createJobDetails("_id"));
        when(jobProvider.dataCounts("_id")).thenReturn(new DataCounts("_id"));

        Set<Setting<?>> settingSet = new HashSet<>();
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingSet);
        AutodetectResultsParser parser = mock(AutodetectResultsParser.class);
        AutodetectProcess autodetectProcess = mock(AutodetectProcess.class);
        AutodetectProcessFactory autodetectProcessFactory = (j, i, e) -> autodetectProcess;
        AutodetectProcessManager manager = new AutodetectProcessManager(Settings.EMPTY, client, threadPool,
                jobManager, jobProvider, jobResultsPersister, jobDataCountsPersister, parser, autodetectProcessFactory, clusterSettings);

        expectThrows(EsRejectedExecutionException.class, () -> manager.create("_id", false));
        verify(autodetectProcess, times(1)).close();
    }

    private void givenAllocationWithStatus(JobStatus status) {
        Allocation.Builder allocation = new Allocation.Builder();
        allocation.setStatus(status);
        when(jobManager.getJobAllocation("foo")).thenReturn(allocation.build());
    }

    private AutodetectProcessManager createManager(AutodetectCommunicator communicator) {
        Client client = mock(Client.class);
        return createManager(communicator, client);
    }

    private AutodetectProcessManager createManager(AutodetectCommunicator communicator, Client client) {
        ThreadPool threadPool = mock(ThreadPool.class);
        AutodetectResultsParser parser = mock(AutodetectResultsParser.class);
        AutodetectProcessFactory autodetectProcessFactory = mock(AutodetectProcessFactory.class);
        Set<Setting<?>> settingSet = new HashSet<>();
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingSet);
        AutodetectProcessManager manager = new AutodetectProcessManager(Settings.EMPTY, client, threadPool, jobManager,
                jobProvider, jobResultsPersister, jobDataCountsPersister, parser, autodetectProcessFactory, clusterSettings);
        manager = spy(manager);
        doReturn(communicator).when(manager).create(any(), anyBoolean());
        return manager;
    }

    private AutodetectProcessManager createManagerAndCallProcessData(AutodetectCommunicator communicator, String jobId) {
        AutodetectProcessManager manager = createManager(communicator);
        manager.openJob(jobId, false);
        manager.processData(jobId, createInputStream(""), mock(DataLoadParams.class), () -> false);
        return manager;
    }

    private Job createJobDetails(String jobId) {
        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setFormat(DataDescription.DataFormat.DELIMITED);
        dd.setFieldDelimiter(',');

        Detector d = new Detector.Builder("metric", "value").build();

        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(d));

        Job.Builder builder = new Job.Builder(jobId);
        builder.setDataDescription(dd);
        builder.setAnalysisConfig(ac);

        return builder.build();
    }

    private static InputStream createInputStream(String input) {
        return new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    }
}
