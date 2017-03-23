/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
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
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerFactory;
import org.elasticsearch.xpack.persistent.PersistentTasksService;
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
import java.util.concurrent.ExecutorService;
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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Calling the {@link AutodetectProcessManager#processData(String, InputStream, DataLoadParams)}
 * method causes an AutodetectCommunicator to be created on demand. Most of these tests have to
 * do that before they can assert other things
 */
public class AutodetectProcessManagerTests extends ESTestCase {

    private JobManager jobManager;
    private JobProvider jobProvider;
    private JobResultsPersister jobResultsPersister;
    private JobDataCountsPersister jobDataCountsPersister;
    private NormalizerFactory normalizerFactory;

    private DataCounts dataCounts = new DataCounts("foo");
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
            Consumer<DataCounts> handler = (Consumer<DataCounts>) invocationOnMock.getArguments()[1];
            handler.accept(dataCounts);
            return null;
        }).when(jobProvider).dataCounts(any(), any(), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Consumer<ModelSnapshot> handler = (Consumer<ModelSnapshot>) invocationOnMock.getArguments()[2];
            handler.accept(modelSnapshot);
            return null;
        }).when(jobProvider).getModelSnapshot(anyString(), anyString(), any(), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Consumer<Quantiles> handler = (Consumer<Quantiles>) invocationOnMock.getArguments()[1];
            handler.accept(quantiles);
            return null;
        }).when(jobProvider).getQuantiles(any(), any(), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Consumer<Set<MlFilter>> handler = (Consumer<Set<MlFilter>>) invocationOnMock.getArguments()[0];
            handler.accept(filters);
            return null;
        }).when(jobProvider).getFilters(any(), any(), any());
    }

    public void testOpenJob() {
        Client client = mock(Client.class);
        PersistentTasksService persistentTasksService = mock(PersistentTasksService.class);
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        when(jobManager.getJobOrThrowIfUnknown("foo")).thenReturn(createJobDetails("foo"));
        AutodetectProcessManager manager = createManager(communicator, client, persistentTasksService);

        manager.openJob("foo", 1L, false, e -> {});
        assertEquals(1, manager.numberOfOpenJobs());
        assertTrue(manager.jobHasActiveAutodetectProcess("foo"));
        verify(persistentTasksService).updateStatus(eq(1L), eq(JobState.OPENED), any());
    }

    public void testOpenJob_exceedMaxNumJobs() {
        when(jobManager.getJobOrThrowIfUnknown("foo")).thenReturn(createJobDetails("foo"));
        doAnswer(invocationOnMock -> {
            String jobId = (String) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            Consumer<DataCounts> handler = (Consumer<DataCounts>) invocationOnMock.getArguments()[1];
            handler.accept(new DataCounts(jobId));
            return null;
        }).when(jobProvider).dataCounts(any(), any(), any());

        when(jobManager.getJobOrThrowIfUnknown("bar")).thenReturn(createJobDetails("bar"));
        when(jobManager.getJobOrThrowIfUnknown("baz")).thenReturn(createJobDetails("baz"));
        when(jobManager.getJobOrThrowIfUnknown("foobar")).thenReturn(createJobDetails("foobar"));

        Client client = mock(Client.class);
        PersistentTasksService persistentTasksService = mock(PersistentTasksService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadPool.Cancellable cancellable = mock(ThreadPool.Cancellable.class);
        when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(cancellable);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.newDirectExecutorService());
        AutodetectProcess autodetectProcess = mock(AutodetectProcess.class);
        when(autodetectProcess.isProcessAlive()).thenReturn(true);
        when(autodetectProcess.readAutodetectResults()).thenReturn(Collections.emptyIterator());
        AutodetectProcessFactory autodetectProcessFactory = (j, modelSnapshot, quantiles, filters, i, e) -> autodetectProcess;
        Settings.Builder settings = Settings.builder();
        settings.put(AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.getKey(), 3);
        AutodetectProcessManager manager = spy(new AutodetectProcessManager(settings.build(), client, threadPool, jobManager, jobProvider,
                jobResultsPersister, jobDataCountsPersister, autodetectProcessFactory,
                normalizerFactory, persistentTasksService,
                new NamedXContentRegistry(Collections.emptyList())));

        DataCounts dataCounts = new DataCounts("foo");
        ModelSnapshot modelSnapshot = new ModelSnapshot.Builder("foo").build();
        Quantiles quantiles = new Quantiles("foo", new Date(), "state");
        Set<MlFilter> filters = new HashSet<>();
        doAnswer(invocationOnMock -> {
            AutodetectProcessManager.MultiConsumer consumer = (AutodetectProcessManager.MultiConsumer) invocationOnMock.getArguments()[1];
            consumer.accept(dataCounts, modelSnapshot, quantiles, filters);
            return null;
        }).when(manager).gatherRequiredInformation(any(), any(), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            CheckedConsumer<Exception, IOException> consumer = (CheckedConsumer<Exception, IOException>) invocationOnMock.getArguments()[2];
            consumer.accept(null);
            return null;
        }).when(manager).setJobState(anyLong(), eq(JobState.FAILED), any());

        manager.openJob("foo", 1L, false, e -> {});
        manager.openJob("bar", 2L, false, e -> {});
        manager.openJob("baz", 3L, false, e -> {});
        assertEquals(3, manager.numberOfOpenJobs());

        Exception[] holder = new Exception[1];
        manager.openJob("foobar", 4L, false, e -> holder[0] = e);
        Exception e = holder[0];
        assertEquals("max running job capacity [3] reached", e.getMessage());

        manager.closeJob("baz", false, null);
        assertEquals(2, manager.numberOfOpenJobs());
        manager.openJob("foobar", 4L, false, e1 -> {});
        assertEquals(3, manager.numberOfOpenJobs());
    }

    public void testProcessData()  {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);
        assertEquals(0, manager.numberOfOpenJobs());

        DataLoadParams params = new DataLoadParams(TimeRange.builder().build(), Optional.empty());
        manager.openJob("foo", 1L, false, e -> {});
        manager.processData("foo", createInputStream(""), params);
        assertEquals(1, manager.numberOfOpenJobs());
    }

    public void testProcessDataThrowsElasticsearchStatusException_onIoException() throws Exception {
        AutodetectCommunicator communicator = Mockito.mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);

        DataLoadParams params = mock(DataLoadParams.class);
        InputStream inputStream = createInputStream("");
        doThrow(new IOException("blah")).when(communicator).writeToJob(inputStream, params);

        manager.openJob("foo", 1L, false, e -> {});
        ESTestCase.expectThrows(ElasticsearchException.class,
                () -> manager.processData("foo", inputStream, params));
    }

    public void testCloseJob() {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);
        assertEquals(0, manager.numberOfOpenJobs());

        manager.openJob("foo", 1L, false, e -> {});
        manager.processData("foo", createInputStream(""), mock(DataLoadParams.class));

        // job is created
        assertEquals(1, manager.numberOfOpenJobs());
        manager.closeJob("foo", false, null);
        assertEquals(0, manager.numberOfOpenJobs());
    }

    public void testBucketResetMessageIsSent() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);

        DataLoadParams params = new DataLoadParams(TimeRange.builder().startTime("1000").endTime("2000").build(), Optional.empty());
        InputStream inputStream = createInputStream("");
        manager.openJob("foo", 1L, false, e -> {});
        manager.processData("foo", inputStream, params);
        verify(communicator).writeToJob(inputStream, params);
    }

    public void testFlush() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);

        InputStream inputStream = createInputStream("");
        manager.openJob("foo", 1L, false, e -> {});
        manager.processData("foo", inputStream, mock(DataLoadParams.class));

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

    public void testwriteUpdateProcessMessage() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManagerAndCallProcessData(communicator, "foo");
        ModelPlotConfig modelConfig = mock(ModelPlotConfig.class);
        List<DetectionRule> rules = Collections.singletonList(mock(DetectionRule.class));
        List<JobUpdate.DetectorUpdate> detectorUpdates = Collections.singletonList(new JobUpdate.DetectorUpdate(2, null, rules));
        manager.writeUpdateProcessMessage("foo", detectorUpdates, modelConfig);
        verify(communicator).writeUpdateModelPlotMessage(modelConfig);
        verify(communicator).writeUpdateDetectorRulesMessage(2, rules);
    }

    public void testJobHasActiveAutodetectProcess() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        AutodetectProcessManager manager = createManager(communicator);
        assertFalse(manager.jobHasActiveAutodetectProcess("foo"));

        manager.openJob("foo", 1L, false, e -> {});
        manager.processData("foo", createInputStream(""), mock(DataLoadParams.class));

        assertTrue(manager.jobHasActiveAutodetectProcess("foo"));
        assertFalse(manager.jobHasActiveAutodetectProcess("bar"));
    }

    public void testProcessData_GivenStateNotOpened() throws IOException {
        AutodetectCommunicator communicator = mock(AutodetectCommunicator.class);
        when(communicator.writeToJob(any(), any())).thenReturn(new DataCounts("foo"));
        AutodetectProcessManager manager = createManager(communicator);

        InputStream inputStream = createInputStream("");
        manager.openJob("foo", 1L, false, e -> {});
        DataCounts dataCounts = manager.processData("foo", inputStream, mock(DataLoadParams.class));

        assertThat(dataCounts, equalTo(new DataCounts("foo")));
    }

    public void testCreate_notEnoughThreads() throws IOException {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doThrow(new EsRejectedExecutionException("")).when(executorService).submit(any(Runnable.class));
        when(threadPool.executor(anyString())).thenReturn(executorService);
        when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(mock(ThreadPool.Cancellable.class));
        when(jobManager.getJobOrThrowIfUnknown("my_id")).thenReturn(createJobDetails("my_id"));
        doAnswer(invocationOnMock -> {
            String jobId = (String) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            Consumer<DataCounts> handler = (Consumer<DataCounts>) invocationOnMock.getArguments()[1];
            handler.accept(new DataCounts(jobId));
            return null;
        }).when(jobProvider).dataCounts(eq("my_id"), any(), any());
        PersistentTasksService persistentTasksService = mock(PersistentTasksService.class);
        AutodetectProcess autodetectProcess = mock(AutodetectProcess.class);
        AutodetectProcessFactory autodetectProcessFactory = (j, modelSnapshot, quantiles, filters, i, e) -> autodetectProcess;
        AutodetectProcessManager manager = new AutodetectProcessManager(Settings.EMPTY, client, threadPool, jobManager, jobProvider,
                jobResultsPersister, jobDataCountsPersister, autodetectProcessFactory,
                normalizerFactory, persistentTasksService,
                new NamedXContentRegistry(Collections.emptyList()));

        expectThrows(EsRejectedExecutionException.class,
                () -> manager.create("my_id", 1L, dataCounts, modelSnapshot, quantiles, filters, false, e -> {}));
        verify(autodetectProcess, times(1)).close();
    }

    private AutodetectProcessManager createManager(AutodetectCommunicator communicator) {
        Client client = mock(Client.class);
        PersistentTasksService persistentTasksService = mock(PersistentTasksService.class);
        return createManager(communicator, client, persistentTasksService);
    }

    private AutodetectProcessManager createManager(AutodetectCommunicator communicator, Client client,
                                                   PersistentTasksService persistentTasksService) {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.newDirectExecutorService());
        AutodetectProcessFactory autodetectProcessFactory = mock(AutodetectProcessFactory.class);
        AutodetectProcessManager manager = new AutodetectProcessManager(Settings.EMPTY, client,
                threadPool, jobManager, jobProvider, jobResultsPersister, jobDataCountsPersister,
                autodetectProcessFactory, normalizerFactory, persistentTasksService,
                new NamedXContentRegistry(Collections.emptyList()));
        manager = spy(manager);
        doReturn(communicator).when(manager)
                .create(any(), anyLong(), eq(dataCounts), eq(modelSnapshot), eq(quantiles), eq(filters), anyBoolean(), any());
        return manager;
    }

    private AutodetectProcessManager createManagerAndCallProcessData(AutodetectCommunicator communicator, String jobId) {
        AutodetectProcessManager manager = createManager(communicator);
        manager.openJob(jobId, 1L, false, e -> {});
        manager.processData(jobId, createInputStream(""), mock(DataLoadParams.class));
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
