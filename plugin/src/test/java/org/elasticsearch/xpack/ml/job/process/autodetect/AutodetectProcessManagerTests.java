/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.action.OpenJobAction.JobTask;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerFactory;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.mock.orig.Mockito.doAnswer;
import static org.elasticsearch.mock.orig.Mockito.doReturn;
import static org.elasticsearch.mock.orig.Mockito.doThrow;
import static org.elasticsearch.mock.orig.Mockito.times;
import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Calling the
 * {@link AutodetectProcessManager#processData(String, InputStream, XContentType, DataLoadParams, BiConsumer)}
 * method causes an AutodetectCommunicator to be created on demand. Most of
 * these tests have to do that before they can assert other things
 */
public class AutodetectProcessManagerTests extends ESTestCase {

    private JobManager jobManager;
    private JobProvider jobProvider;
    private JobResultsPersister jobResultsPersister;
    private JobDataCountsPersister jobDataCountsPersister;
    private NormalizerFactory normalizerFactory;

    private DataCounts dataCounts = new DataCounts("foo");
    private ModelSizeStats modelSizeStats = new ModelSizeStats.Builder("foo").build();
    private ModelSnapshot modelSnapshot = new ModelSnapshot.Builder("foo").build();
    private Quantiles quantiles = new Quantiles("foo", new Date(), "state");
    private Set<MlFilter> filters = new HashSet<>();

    @Before
    public void initMocks() {
        jobManager = mock(JobManager.class);
        jobProvider = mock(JobProvider.class);
        jobResultsPersister = mock(JobResultsPersister.class);
        when(jobResultsPersister.bulkPersisterBuilder(any())).thenReturn(mock(JobResultsPersister.Builder.class));
        jobDataCountsPersister = mock(JobDataCountsPersister.class);
        normalizerFactory = mock(NormalizerFactory.class);

        when(jobManager.getJobOrThrowIfUnknown("foo")).thenReturn(createJobDetails("foo"));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Consumer<AutodetectParams> handler = (Consumer<AutodetectParams>) invocationOnMock.getArguments()[1];
            handler.accept(buildAutodetectParams());
            return null;
        }).when(jobProvider).getAutodetectParams(any(), any(), any());
    }

    public void testOpenJob() {
        Client client = mock(Client.class);
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        when(jobManager.getJobOrThrowIfUnknown("foo")).thenReturn(createJobDetails("foo"));
        AutodetectProcessManager manager = createManager(communicator, client);

        JobTask jobTask = mock(JobTask.class);
        manager.openJob("foo", jobTask, false, e -> {});
        assertEquals(1, manager.numberOfOpenJobs());
        assertTrue(manager.jobHasActiveAutodetectProcess("foo"));
        verify(jobTask).updatePersistentStatus(eq(JobState.OPENED), any());
    }

    public void testOpenJob_exceedMaxNumJobs() {
        when(jobManager.getJobOrThrowIfUnknown("foo")).thenReturn(createJobDetails("foo"));
        when(jobManager.getJobOrThrowIfUnknown("bar")).thenReturn(createJobDetails("bar"));
        when(jobManager.getJobOrThrowIfUnknown("baz")).thenReturn(createJobDetails("baz"));
        when(jobManager.getJobOrThrowIfUnknown("foobar")).thenReturn(createJobDetails("foobar"));

        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        ThreadPool.Cancellable cancellable = mock(ThreadPool.Cancellable.class);
        when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(cancellable);
        ExecutorService executorService = mock(ExecutorService.class);
        Future future = mock(Future.class);
        when(executorService.submit(any(Callable.class))).thenReturn(future);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.newDirectExecutorService());
        AutodetectProcess autodetectProcess = mock(AutodetectProcess.class);
        when(autodetectProcess.isProcessAlive()).thenReturn(true);
        when(autodetectProcess.readAutodetectResults()).thenReturn(Collections.emptyIterator());
        AutodetectProcessFactory autodetectProcessFactory =
                (j, modelSnapshot, quantiles, filters, i, e, onProcessCrash) -> autodetectProcess;
        Settings.Builder settings = Settings.builder();
        settings.put(AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.getKey(), 3);
        AutodetectProcessManager manager = spy(new AutodetectProcessManager(settings.build(), client, threadPool, jobManager, jobProvider,
                jobResultsPersister, jobDataCountsPersister, autodetectProcessFactory,
                normalizerFactory, new NamedXContentRegistry(Collections.emptyList())));
        doReturn(executorService).when(manager).createAutodetectExecutorService(any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            CheckedConsumer<Exception, IOException> consumer = (CheckedConsumer<Exception, IOException>) invocationOnMock.getArguments()[2];
            consumer.accept(null);
            return null;
        }).when(manager).setJobState(any(), eq(JobState.FAILED), any());

        JobTask jobTask = mock(JobTask.class);
        manager.openJob("foo", jobTask, false, e -> {});
        manager.openJob("bar", jobTask, false, e -> {});
        manager.openJob("baz", jobTask, false, e -> {});
        assertEquals(3, manager.numberOfOpenJobs());

        Exception[] holder = new Exception[1];
        manager.openJob("foobar", jobTask, false, e -> holder[0] = e);
        Exception e = holder[0];
        assertEquals("max running job capacity [3] reached", e.getMessage());

        manager.closeJob("baz", false, null);
        assertEquals(2, manager.numberOfOpenJobs());
        manager.openJob("foobar", jobTask, false, e1 -> {});
        assertEquals(3, manager.numberOfOpenJobs());
    }

    public void testProcessData()  {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);
        assertEquals(0, manager.numberOfOpenJobs());

        JobTask jobTask = mock(JobTask.class);
        DataLoadParams params = new DataLoadParams(TimeRange.builder().build(), Optional.empty());
        manager.openJob("foo", jobTask, false, e -> {});
        manager.processData("foo", createInputStream(""), randomFrom(XContentType.values()),
                params, (dataCounts1, e) -> {});
        assertEquals(1, manager.numberOfOpenJobs());
    }

    public void testProcessDataThrowsElasticsearchStatusException_onIoException() throws Exception {
        AutodetectCommunicator communicator = Mockito.mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);

        DataLoadParams params = mock(DataLoadParams.class);
        InputStream inputStream = createInputStream("");
        XContentType xContentType = randomFrom(XContentType.values());
        doAnswer(invocationOnMock -> {
            BiConsumer<DataCounts, Exception> handler = (BiConsumer<DataCounts, Exception>) invocationOnMock.getArguments()[3];
            handler.accept(null, new IOException("blah"));
            return null;
        }).when(communicator).writeToJob(eq(inputStream), same(xContentType), eq(params), any());


        JobTask jobTask = mock(JobTask.class);
        manager.openJob("foo", jobTask, false, e -> {});
        Exception[] holder = new Exception[1];
        manager.processData("foo", inputStream, xContentType, params, (dataCounts1, e) -> holder[0] = e);
        assertNotNull(holder[0]);
    }

    public void testCloseJob() {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);
        assertEquals(0, manager.numberOfOpenJobs());

        JobTask jobTask = mock(JobTask.class);
        manager.openJob("foo", jobTask, false, e -> {});
        manager.processData("foo", createInputStream(""), randomFrom(XContentType.values()),
                mock(DataLoadParams.class), (dataCounts1, e) -> {});

        // job is created
        assertEquals(1, manager.numberOfOpenJobs());
        manager.closeJob("foo", false, null);
        assertEquals(0, manager.numberOfOpenJobs());
    }

    public void testBucketResetMessageIsSent() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);
        XContentType xContentType = randomFrom(XContentType.values());

        DataLoadParams params = new DataLoadParams(TimeRange.builder().startTime("1000").endTime("2000").build(), Optional.empty());
        InputStream inputStream = createInputStream("");
        JobTask jobTask = mock(JobTask.class);
        manager.openJob("foo", jobTask, false, e -> {});
        manager.processData("foo", inputStream, xContentType, params, (dataCounts1, e) -> {});
        verify(communicator).writeToJob(same(inputStream), same(xContentType), same(params), any());
    }

    public void testFlush() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);

        JobTask jobTask = mock(JobTask.class);
        InputStream inputStream = createInputStream("");
        manager.openJob("foo", jobTask, false, e -> {});
        manager.processData("foo", inputStream, randomFrom(XContentType.values()),
                mock(DataLoadParams.class), (dataCounts1, e) -> {});

        InterimResultsParams params = InterimResultsParams.builder().build();
        manager.flushJob("foo", params, e -> {});

        verify(communicator).flushJob(same(params), any());
    }

    public void testFlushThrows() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManagerAndCallProcessData(communicator, "foo");

        InterimResultsParams params = InterimResultsParams.builder().build();
        doAnswer(invocationOnMock -> {
            BiConsumer<Void, Exception> handler = (BiConsumer<Void, Exception>) invocationOnMock.getArguments()[1];
            handler.accept(null, new IOException("blah"));
            return null;
        }).when(communicator).flushJob(same(params), any());

        Exception[] holder = new Exception[1];
        manager.flushJob("foo", params, e -> holder[0] = e);
        assertEquals("[foo] exception while flushing job", holder[0].getMessage());
    }

    public void testwriteUpdateProcessMessage() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManagerAndCallProcessData(communicator, "foo");
        ModelPlotConfig modelConfig = mock(ModelPlotConfig.class);
        List<DetectionRule> rules = Collections.singletonList(mock(DetectionRule.class));
        List<JobUpdate.DetectorUpdate> detectorUpdates = Collections.singletonList(new JobUpdate.DetectorUpdate(2, null, rules));
        manager.writeUpdateProcessMessage("foo", detectorUpdates, modelConfig, e -> {});
        verify(communicator).writeUpdateProcessMessage(same(modelConfig), same(detectorUpdates), any());
    }

    public void testJobHasActiveAutodetectProcess() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);
        assertFalse(manager.jobHasActiveAutodetectProcess("foo"));

        JobTask jobTask = mock(JobTask.class);
        manager.openJob("foo", jobTask, false, e -> {});
        manager.processData("foo", createInputStream(""), randomFrom(XContentType.values()),
                mock(DataLoadParams.class), (dataCounts1, e) -> {});

        assertTrue(manager.jobHasActiveAutodetectProcess("foo"));
        assertFalse(manager.jobHasActiveAutodetectProcess("bar"));
    }

    public void testProcessData_GivenStateNotOpened() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            BiConsumer<DataCounts, Exception> handler = (BiConsumer<DataCounts, Exception>) invocationOnMock.getArguments()[3];
            handler.accept(new DataCounts("foo"), null);
            return null;
        }).when(communicator).writeToJob(any(), any(), any(), any());
        AutodetectProcessManager manager = createManager(communicator);

        JobTask jobTask = mock(JobTask.class);
        manager.openJob("foo", jobTask, false, e -> {});
        InputStream inputStream = createInputStream("");
        DataCounts[] dataCounts = new DataCounts[1];
        manager.processData("foo", inputStream,
                randomFrom(XContentType.values()), mock(DataLoadParams.class), (dataCounts1, e) -> {
            dataCounts[0] = dataCounts1;
        });

        assertThat(dataCounts[0], equalTo(new DataCounts("foo")));
    }

    public void testCreate_notEnoughThreads() throws IOException {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doThrow(new EsRejectedExecutionException("")).when(executorService).submit(any(Runnable.class));
        when(threadPool.executor(anyString())).thenReturn(executorService);
        when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(mock(ThreadPool.Cancellable.class));
        when(jobManager.getJobOrThrowIfUnknown("my_id")).thenReturn(createJobDetails("my_id"));
        AutodetectProcess autodetectProcess = mock(AutodetectProcess.class);
        AutodetectProcessFactory autodetectProcessFactory =
                (j, modelSnapshot, quantiles, filters, i, e, onProcessCrash) -> autodetectProcess;
        AutodetectProcessManager manager = new AutodetectProcessManager(Settings.EMPTY, client, threadPool, jobManager, jobProvider,
                jobResultsPersister, jobDataCountsPersister, autodetectProcessFactory,
                normalizerFactory, new NamedXContentRegistry(Collections.emptyList()));

        JobTask jobTask = mock(JobTask.class);
        expectThrows(EsRejectedExecutionException.class,
                () -> manager.create("my_id", jobTask, buildAutodetectParams(), false, e -> {}));
        verify(autodetectProcess, times(1)).close();
    }

    private AutodetectParams buildAutodetectParams() {
        return new AutodetectParams.Builder("foo")
                .setDataCounts(dataCounts)
                .setModelSizeStats(modelSizeStats)
                .setModelSnapshot(modelSnapshot)
                .setQuantiles(quantiles)
                .setFilters(filters)
                .build();
    }

    private AutodetectProcessManager createManager(AutodetectCommunicator communicator) {
        Client client = mock(Client.class);
        return createManager(communicator, client);
    }

    private AutodetectProcessManager createManager(AutodetectCommunicator communicator, Client client) {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.newDirectExecutorService());
        AutodetectProcessFactory autodetectProcessFactory = mock(AutodetectProcessFactory.class);
        AutodetectProcessManager manager = new AutodetectProcessManager(Settings.EMPTY, client,
                threadPool, jobManager, jobProvider, jobResultsPersister, jobDataCountsPersister,
                autodetectProcessFactory, normalizerFactory, new NamedXContentRegistry(Collections.emptyList()));
        manager = spy(manager);
        doReturn(communicator).when(manager).create(any(), any(), eq(buildAutodetectParams()), anyBoolean(), any());
        return manager;
    }

    private AutodetectProcessManager createManagerAndCallProcessData(AutodetectCommunicator communicator, String jobId) {
        AutodetectProcessManager manager = createManager(communicator);
        JobTask jobTask = mock(JobTask.class);
        manager.openJob(jobId, jobTask, false, e -> {});
        manager.processData(jobId, createInputStream(""), randomFrom(XContentType.values()),
                mock(DataLoadParams.class), (dataCounts, e) -> {});
        return manager;
    }

    private Job createJobDetails(String jobId) {
        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setFormat(DataDescription.DataFormat.DELIMITED);
        dd.setTimeFormat("epoch");
        dd.setFieldDelimiter(',');

        Detector d = new Detector.Builder("metric", "value").build();

        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(d));

        Job.Builder builder = new Job.Builder(jobId);
        builder.setDataDescription(dd);
        builder.setAnalysisConfig(ac);
        builder.setCreateTime(new Date());

        return builder.build();
    }

    private static InputStream createInputStream(String input) {
        return new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    }
}
