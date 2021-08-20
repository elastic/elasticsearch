/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzerTests;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerFactory;
import org.elasticsearch.xpack.ml.job.task.JobTask;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.process.NativeStorageProvider;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.action.support.master.MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.mock.orig.Mockito.doAnswer;
import static org.elasticsearch.mock.orig.Mockito.doReturn;
import static org.elasticsearch.mock.orig.Mockito.doThrow;
import static org.elasticsearch.mock.orig.Mockito.times;
import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.elasticsearch.mock.orig.Mockito.verifyNoMoreInteractions;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Calling the
 * {@link AutodetectProcessManager#processData(JobTask, AnalysisRegistry, InputStream, XContentType, DataLoadParams, BiConsumer)}
 * method causes an AutodetectCommunicator to be created on demand. Most of
 * these tests have to do that before they can assert other things
 */
public class AutodetectProcessManagerTests extends ESTestCase {

    private Client client;
    private ThreadPool threadPool;
    private AnalysisRegistry analysisRegistry;
    private JobManager jobManager;
    private JobResultsProvider jobResultsProvider;
    private JobResultsPersister jobResultsPersister;
    private JobDataCountsPersister jobDataCountsPersister;
    private AnnotationPersister annotationPersister;
    private AutodetectCommunicator autodetectCommunicator;
    private AutodetectProcessFactory autodetectFactory;
    private NormalizerFactory normalizerFactory;
    private AnomalyDetectionAuditor auditor;
    private ClusterState clusterState;
    private ClusterService clusterService;
    private NativeStorageProvider nativeStorageProvider;

    private DataCounts dataCounts = new DataCounts("foo");
    private ModelSizeStats modelSizeStats = new ModelSizeStats.Builder("foo").build();
    private ModelSnapshot modelSnapshot = new ModelSnapshot.Builder("foo").build();
    private Quantiles quantiles = new Quantiles("foo", new Date(), "state");

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        client = mock(Client.class);

        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        analysisRegistry = CategorizationAnalyzerTests.buildTestAnalysisRegistry(TestEnvironment.newEnvironment(settings));
        jobManager = mock(JobManager.class);
        jobResultsProvider = mock(JobResultsProvider.class);
        jobResultsPersister = mock(JobResultsPersister.class);
        JobResultsPersister.Builder bulkPersisterBuilder = mock(JobResultsPersister.Builder.class);
        when(bulkPersisterBuilder.shouldRetry(any())).thenReturn(bulkPersisterBuilder);
        when(jobResultsPersister.bulkPersisterBuilder(any())).thenReturn(bulkPersisterBuilder);
        jobDataCountsPersister = mock(JobDataCountsPersister.class);
        annotationPersister = mock(AnnotationPersister.class);
        AnnotationPersister.Builder bulkAnnotationsBuilder = mock(AnnotationPersister.Builder.class);
        when(bulkAnnotationsBuilder.shouldRetry(any())).thenReturn(bulkAnnotationsBuilder);
        when(annotationPersister.bulkPersisterBuilder(any())).thenReturn(bulkAnnotationsBuilder);
        autodetectCommunicator = mock(AutodetectCommunicator.class);
        autodetectFactory = mock(AutodetectProcessFactory.class);
        normalizerFactory = mock(NormalizerFactory.class);
        auditor = mock(AnomalyDetectionAuditor.class);
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings =
            new ClusterSettings(Settings.EMPTY,
                new HashSet<>(Arrays.asList(MachineLearning.MAX_OPEN_JOBS_PER_NODE,
                    ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES)));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        Metadata metadata = Metadata.builder()
            .indices(ImmutableOpenMap.<String, IndexMetadata>builder()
                .fPut(
                    AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001",
                    IndexMetadata.builder(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001")
                        .settings(
                            Settings.builder()
                                .put(SETTING_NUMBER_OF_SHARDS, 1)
                                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                                .put(SETTING_VERSION_CREATED, Version.CURRENT)
                                .build())
                        .putAlias(AliasMetadata.builder(AnomalyDetectorsIndex.jobStateIndexWriteAlias()).build())
                        .build())
                .fPut(
                    AnnotationIndex.INDEX_NAME,
                    IndexMetadata.builder(AnnotationIndex.INDEX_NAME)
                        .settings(
                            Settings.builder()
                                .put(SETTING_NUMBER_OF_SHARDS, 1)
                                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                                .put(SETTING_VERSION_CREATED, Version.CURRENT)
                                .build())
                        .putAlias(AliasMetadata.builder(AnnotationIndex.READ_ALIAS_NAME).build())
                        .putAlias(AliasMetadata.builder(AnnotationIndex.WRITE_ALIAS_NAME).build())
                        .build())
                .build())
            .build();
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getMinNodeVersion()).thenReturn(JobResultsProvider.HIDDEN_INTRODUCED_VERSION);
        clusterState = mock(ClusterState.class);
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.nodes()).thenReturn(nodes);
        nativeStorageProvider = mock(NativeStorageProvider.class);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Job> listener = (ActionListener<Job>) invocationOnMock.getArguments()[1];
            listener.onResponse(createJobDetails("foo"));
            return null;
        }).when(jobManager).getJob(eq("foo"), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Consumer<AutodetectParams> handler = (Consumer<AutodetectParams>) invocationOnMock.getArguments()[1];
            handler.accept(buildAutodetectParams());
            return null;
        }).when(jobResultsProvider).getAutodetectParams(any(), any(), any());
    }

    public void testOpenJob() {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Job> listener = (ActionListener<Job>) invocationOnMock.getArguments()[1];
            listener.onResponse(createJobDetails("foo"));
            return null;
        }).when(jobManager).getJob(eq("foo"), any());
        AutodetectProcessManager manager = createSpyManager();

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        when(jobTask.getAllocationId()).thenReturn(1L);
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        assertEquals(1, manager.numberOfOpenJobs());
        assertTrue(manager.jobHasActiveAutodetectProcess(jobTask));
        verify(jobTask).updatePersistentTaskState(eq(new JobTaskState(JobState.OPENED, 1L, null)), any());
    }

    public void testOpenJob_withoutVersion() {
        Job.Builder jobBuilder = new Job.Builder(createJobDetails("no_version"));
        jobBuilder.setJobVersion(null);
        Job job = jobBuilder.build();
        assertThat(job.getJobVersion(), is(nullValue()));

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Job> listener = (ActionListener<Job>) invocationOnMock.getArguments()[1];
            listener.onResponse(job);
            return null;
        }).when(jobManager).getJob(eq(job.getId()), any());

        AutodetectProcessManager manager = createSpyManager();
        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn(job.getId());
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> errorHolder.set(e));
        Exception error = errorHolder.get();
        assertThat(error, is(notNullValue()));
        assertThat(error.getMessage(), equalTo("Cannot open job [no_version] because jobs created prior to version 5.5 are not supported"));
    }

    @SuppressWarnings("unchecked")
    public void testOpenJob_exceedMaxNumJobs() {
        for (String jobId : new String [] {"foo", "bar", "baz", "foobar"}) {
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                ActionListener<Job> listener = (ActionListener<Job>) invocationOnMock.getArguments()[1];
                listener.onResponse(createJobDetails(jobId));
                return null;
            }).when(jobManager).getJob(eq(jobId), any());
        }

        ThreadPool.Cancellable cancellable = mock(ThreadPool.Cancellable.class);
        when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(cancellable);

        AutodetectProcess autodetectProcess = mock(AutodetectProcess.class);
        when(autodetectProcess.isProcessAlive()).thenReturn(true);
        when(autodetectProcess.readAutodetectResults()).thenReturn(Collections.emptyIterator());

        autodetectFactory = (pid, j, autodetectParams, e, onProcessCrash) -> autodetectProcess;
        Settings.Builder settings = Settings.builder();
        settings.put(MachineLearning.MAX_OPEN_JOBS_PER_NODE.getKey(), 3);
        AutodetectProcessManager manager = createSpyManager(settings.build());
        doCallRealMethod().when(manager).create(any(), any(), any(), any());

        ExecutorService executorService = mock(ExecutorService.class);
        Future<?> future = mock(Future.class);
        when(executorService.submit(any(Callable.class))).thenReturn(future);
        doReturn(executorService).when(manager).createAutodetectExecutorService(any());

        doAnswer(invocationOnMock -> {
            CheckedConsumer<Exception, IOException> consumer = (CheckedConsumer<Exception, IOException>) invocationOnMock.getArguments()[3];
            consumer.accept(null);
            return null;
        }).when(manager).setJobState(any(), eq(JobState.FAILED), any(), any());

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("bar");
        when(jobTask.getAllocationId()).thenReturn(1L);
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("baz");
        when(jobTask.getAllocationId()).thenReturn(2L);
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        assertEquals(3, manager.numberOfOpenJobs());

        Exception[] holder = new Exception[1];
        jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foobar");
        when(jobTask.getAllocationId()).thenReturn(3L);
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> holder[0] = e);
        Exception e = holder[0];
        assertEquals("max running job capacity [3] reached", e.getMessage());

        jobTask = mock(JobTask.class);
        when(jobTask.getAllocationId()).thenReturn(2L);
        when(jobTask.getJobId()).thenReturn("baz");
        manager.closeJob(jobTask, null);
        assertEquals(2, manager.numberOfOpenJobs());
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e1, b) -> {});
        assertEquals(3, manager.numberOfOpenJobs());
    }

    public void testProcessData()  {
        AutodetectProcessManager manager = createSpyManager();
        assertEquals(0, manager.numberOfOpenJobs());

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        DataLoadParams params = new DataLoadParams(TimeRange.builder().build(), Optional.empty());
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        manager.processData(jobTask, analysisRegistry, createInputStream(""), randomFrom(XContentType.values()),
                params, (dataCounts1, e) -> {});
        assertEquals(1, manager.numberOfOpenJobs());
    }

    public void testProcessDataThrowsElasticsearchStatusException_onIoException() {
        AutodetectProcessManager manager = createSpyManager();

        DataLoadParams params = mock(DataLoadParams.class);
        InputStream inputStream = createInputStream("");
        XContentType xContentType = randomFrom(XContentType.values());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            BiConsumer<DataCounts, Exception> handler = (BiConsumer<DataCounts, Exception>) invocationOnMock.getArguments()[4];
            handler.accept(null, new IOException("blah"));
            return null;
        }).when(autodetectCommunicator).writeToJob(eq(inputStream), same(analysisRegistry), same(xContentType), eq(params), any());


        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        Exception[] holder = new Exception[1];
        manager.processData(jobTask, analysisRegistry, inputStream, xContentType, params, (dataCounts1, e) -> holder[0] = e);
        assertNotNull(holder[0]);
    }

    public void testCloseJob() {
        AutodetectProcessManager manager = createSpyManager();
        assertEquals(0, manager.numberOfOpenJobs());

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        manager.processData(jobTask, analysisRegistry, createInputStream(""), randomFrom(XContentType.values()),
                mock(DataLoadParams.class), (dataCounts1, e) -> {});

        // job is created
        assertEquals(1, manager.numberOfOpenJobs());
        manager.closeJob(jobTask, null);
        assertEquals(0, manager.numberOfOpenJobs());
    }

    public void testCanCloseClosingJob() throws Exception {
        AtomicInteger numberOfCommunicatorCloses = new AtomicInteger(0);
        doAnswer(invocationOnMock -> {
            numberOfCommunicatorCloses.incrementAndGet();
            // This increases the chance of the two threads both getting into
            // the middle of the AutodetectProcessManager.close() method
            Thread.yield();
            return null;
        }).when(autodetectCommunicator).close();
        AutodetectProcessManager manager = createSpyManager();
        assertEquals(0, manager.numberOfOpenJobs());

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        manager.processData(jobTask, analysisRegistry, createInputStream(""), randomFrom(XContentType.values()),
                mock(DataLoadParams.class), (dataCounts1, e) -> {});

        assertEquals(1, manager.numberOfOpenJobs());

        // Close the job in a separate thread
        Thread closeThread = new Thread(() -> manager.closeJob(jobTask, "in separate thread"));
        closeThread.start();
        Thread.yield();

        // Also close the job in the current thread, so that we have two simultaneous close requests
        manager.closeJob(jobTask, "in main test thread");

        // The 10 second timeout here is usually far in excess of what is required.  In the vast
        // majority of cases the other thread will exit within a few milliseconds.  However, it
        // has been observed that on some VMs the test can fail because the VM stalls at the
        // wrong moment.  A 10 second timeout is on a par with the length of time assertBusy()
        // would wait under these circumstances.
        closeThread.join(10000);
        assertFalse(closeThread.isAlive());

        // Only one of the threads should have called AutodetectCommunicator.close()
        assertEquals(1, numberOfCommunicatorCloses.get());
        assertEquals(0, manager.numberOfOpenJobs());
    }

    public void testCanKillClosingJob() throws Exception {
        CountDownLatch closeStartedLatch = new CountDownLatch(1);
        CountDownLatch killLatch = new CountDownLatch(1);
        CountDownLatch closeInterruptedLatch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            closeStartedLatch.countDown();
            if (killLatch.await(3, TimeUnit.SECONDS)) {
                closeInterruptedLatch.countDown();
            }
            return null;
        }).when(autodetectCommunicator).close();
        doAnswer(invocationOnMock -> {
            killLatch.countDown();
            return null;
        }).when(autodetectCommunicator).killProcess(anyBoolean(), anyBoolean(), anyBoolean());
        AutodetectProcessManager manager = createSpyManager();
        assertEquals(0, manager.numberOfOpenJobs());

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        manager.processData(jobTask, analysisRegistry, createInputStream(""), randomFrom(XContentType.values()),
                mock(DataLoadParams.class), (dataCounts1, e) -> {});

        // Close the job in a separate thread so that it can simulate taking a long time to close
        Thread closeThread = new Thread(() -> manager.closeJob(jobTask, null));
        closeThread.start();
        assertTrue(closeStartedLatch.await(3, TimeUnit.SECONDS));

        // Kill the job in the current thread, which will be while the job is "closing"
        manager.killProcess(jobTask, false, null);
        assertEquals(0, killLatch.getCount());

        // Assert close method was awoken by the kill
        assertTrue(closeInterruptedLatch.await(3, TimeUnit.SECONDS));

        closeThread.join(500);
        assertFalse(closeThread.isAlive());
    }

    public void testBucketResetMessageIsSent() {
        AutodetectProcessManager manager = createSpyManager();
        XContentType xContentType = randomFrom(XContentType.values());

        DataLoadParams params = new DataLoadParams(TimeRange.builder().startTime("1000").endTime("2000").build(), Optional.empty());
        InputStream inputStream = createInputStream("");
        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        manager.processData(jobTask, analysisRegistry, inputStream, xContentType, params, (dataCounts1, e) -> {});
        verify(autodetectCommunicator).writeToJob(same(inputStream), same(analysisRegistry), same(xContentType), same(params), any());
    }

    public void testFlush() {
        AutodetectProcessManager manager = createSpyManager();

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        InputStream inputStream = createInputStream("");
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        manager.processData(jobTask, analysisRegistry, inputStream, randomFrom(XContentType.values()),
                mock(DataLoadParams.class), (dataCounts1, e) -> {});

        FlushJobParams params = FlushJobParams.builder().build();
        manager.flushJob(jobTask, params, ActionListener.wrap(flushAcknowledgement -> {}, e -> fail(e.getMessage())));

        verify(autodetectCommunicator).flushJob(same(params), any());
    }

    public void testFlushThrows() {
        AutodetectProcessManager manager = createSpyManagerAndCallProcessData("foo");

        FlushJobParams params = FlushJobParams.builder().build();
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            BiConsumer<Void, Exception> handler = (BiConsumer<Void, Exception>) invocationOnMock.getArguments()[1];
            handler.accept(null, new IOException("blah"));
            return null;
        }).when(autodetectCommunicator).flushJob(same(params), any());

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        Exception[] holder = new Exception[1];
        manager.flushJob(jobTask, params, ActionListener.wrap(flushAcknowledgement -> {}, e -> holder[0] = e));
        assertEquals("[foo] exception while flushing job", holder[0].getMessage());
    }

    public void testCloseThrows() {
        AutodetectProcessManager manager = createSpyManager();

        // let the communicator throw, simulating a problem with the underlying
        // autodetect, e.g. a crash
        doThrow(Exception.class).when(autodetectCommunicator).close();

        // create a jobtask
        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        manager.processData(jobTask, analysisRegistry, createInputStream(""), randomFrom(XContentType.values()), mock(DataLoadParams.class),
                (dataCounts1, e) -> {
                });
        verify(manager).setJobState(any(), eq(JobState.OPENED), any(), any());
        // job is created
        assertEquals(1, manager.numberOfOpenJobs());
        expectThrows(ElasticsearchException.class, () -> manager.closeJob(jobTask, null));
        assertEquals(0, manager.numberOfOpenJobs());

        verify(manager).setJobState(any(), eq(JobState.FAILED), any());
    }

    public void testWriteUpdateProcessMessage() {
        AutodetectProcessManager manager = createSpyManagerAndCallProcessData("foo");
        ModelPlotConfig modelConfig = mock(ModelPlotConfig.class);
        List<DetectionRule> rules = Collections.singletonList(mock(DetectionRule.class));
        List<JobUpdate.DetectorUpdate> detectorUpdates = Collections.singletonList(new JobUpdate.DetectorUpdate(2, null, rules));
        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        UpdateParams updateParams = UpdateParams.builder("foo").modelPlotConfig(modelConfig).detectorUpdates(detectorUpdates).build();

        manager.writeUpdateProcessMessage(jobTask, updateParams, e -> {});

        ArgumentCaptor<UpdateProcessMessage> captor = ArgumentCaptor.forClass(UpdateProcessMessage.class);
        verify(autodetectCommunicator).writeUpdateProcessMessage(captor.capture(), any());

        UpdateProcessMessage updateProcessMessage = captor.getValue();
        assertThat(updateProcessMessage.getModelPlotConfig(), equalTo(modelConfig));
        assertThat(updateProcessMessage.getDetectorUpdates(), equalTo(detectorUpdates));
    }

    public void testJobHasActiveAutodetectProcess() {
        AutodetectProcessManager manager = createSpyManager();
        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        assertFalse(manager.jobHasActiveAutodetectProcess(jobTask));

        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        manager.processData(jobTask, analysisRegistry, createInputStream(""), randomFrom(XContentType.values()),
                mock(DataLoadParams.class), (dataCounts1, e) -> {});

        assertTrue(manager.jobHasActiveAutodetectProcess(jobTask));
        jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("bar");
        when(jobTask.getAllocationId()).thenReturn(1L);
        assertFalse(manager.jobHasActiveAutodetectProcess(jobTask));
    }

    public void testKillKillsAutodetectProcess() throws IOException {
        AutodetectProcessManager manager = createSpyManager();
        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        assertFalse(manager.jobHasActiveAutodetectProcess(jobTask));

        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        manager.processData(jobTask, analysisRegistry, createInputStream(""), randomFrom(XContentType.values()),
                mock(DataLoadParams.class), (dataCounts1, e) -> {});

        assertTrue(manager.jobHasActiveAutodetectProcess(jobTask));

        manager.killAllProcessesOnThisNode();

        verify(autodetectCommunicator).killProcess(false, false, true);
    }

    public void testKillingAMissingJobFinishesTheTask() {
        AutodetectProcessManager manager = createSpyManager();
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        AtomicBoolean markCalled = new AtomicBoolean();
        JobTask jobTask = new JobTask("foo", 0, "type", "action", TaskId.EMPTY_TASK_ID, org.elasticsearch.core.Map.of(), licenseState) {
            @Override
            protected void doMarkAsCompleted() {
                markCalled.set(true);
            }
        };
        jobTask.init(mock(PersistentTasksService.class), mock(TaskManager.class), "taskid", 0);

        manager.killProcess(jobTask, false, null);

        assertThat(markCalled.get(), is(true));
    }

    public void testProcessData_GivenStateNotOpened() {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            BiConsumer<DataCounts, Exception> handler = (BiConsumer<DataCounts, Exception>) invocationOnMock.getArguments()[4];
            handler.accept(new DataCounts("foo"), null);
            return null;
        }).when(autodetectCommunicator).writeToJob(any(), any(), any(), any(), any());
        AutodetectProcessManager manager = createSpyManager();

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        InputStream inputStream = createInputStream("");
        DataCounts[] dataCounts = new DataCounts[1];
        manager.processData(jobTask, analysisRegistry, inputStream,
                randomFrom(XContentType.values()), mock(DataLoadParams.class), (dataCounts1, e) -> dataCounts[0] = dataCounts1);

        assertThat(dataCounts[0], equalTo(new DataCounts("foo")));
    }

    public void testCreate_notEnoughThreads() throws IOException {
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        ExecutorService executorService = mock(ExecutorService.class);
        doThrow(new EsRejectedExecutionException("")).when(executorService).submit(any(Runnable.class));
        when(threadPool.executor(anyString())).thenReturn(executorService);
        when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(mock(ThreadPool.Cancellable.class));
        Job job = createJobDetails("my_id");
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Job> listener = (ActionListener<Job>) invocationOnMock.getArguments()[1];
            listener.onResponse(job);
            return null;
        }).when(jobManager).getJob(eq("my_id"), any());

        AutodetectProcess autodetectProcess = mock(AutodetectProcess.class);
        autodetectFactory = (pid, j, autodetectParams, e, onProcessCrash) -> autodetectProcess;
        AutodetectProcessManager manager = createSpyManager();
        doCallRealMethod().when(manager).create(any(), any(), any(), any());

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("my_id");
        expectThrows(EsRejectedExecutionException.class,
                () -> manager.create(jobTask, job, buildAutodetectParams(), (e, b) -> {}));
        verify(autodetectProcess, times(1)).close();
    }

    public void testCreate_givenFirstTime() {
        modelSnapshot = null;
        AutodetectProcessManager manager = createNonSpyManager("foo");

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        manager.create(jobTask, createJobDetails("foo"), buildAutodetectParams(), (e, b) -> {});

        String expectedNotification = "Loading model snapshot [N/A], job latest_record_timestamp [N/A]";
        verify(auditor).info("foo", expectedNotification);
        verifyNoMoreInteractions(auditor);
    }

    public void testCreate_givenExistingModelSnapshot() {
        modelSnapshot = new ModelSnapshot.Builder("foo").setSnapshotId("snapshot-1")
                .setLatestRecordTimeStamp(new Date(0L)).build();
        dataCounts = new DataCounts("foo");
        dataCounts.setLatestRecordTimeStamp(new Date(1L));
        AutodetectProcessManager manager = createNonSpyManager("foo");

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        manager.create(jobTask, createJobDetails("foo"), buildAutodetectParams(), (e, b) -> {});

        String expectedNotification = "Loading model snapshot [snapshot-1] with " +
                "latest_record_timestamp [1970-01-01T00:00:00.000Z], " +
                "job latest_record_timestamp [1970-01-01T00:00:00.001Z]";
        verify(auditor).info("foo", expectedNotification);
        verifyNoMoreInteractions(auditor);
    }

    public void testCreate_givenNonZeroCountsAndNoModelSnapshotNorQuantiles() {
        modelSnapshot = null;
        quantiles = null;
        dataCounts = new DataCounts("foo");
        dataCounts.setLatestRecordTimeStamp(new Date(0L));
        dataCounts.incrementProcessedRecordCount(42L);
        AutodetectProcessManager manager = createNonSpyManager("foo");

        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn("foo");
        manager.create(jobTask, createJobDetails("foo"), buildAutodetectParams(), (e, b) -> {});

        String expectedNotification = "Loading model snapshot [N/A], " +
                "job latest_record_timestamp [1970-01-01T00:00:00.000Z]";
        verify(auditor).info("foo", expectedNotification);
        verify(auditor).warning("foo", "No model snapshot could be found for a job with processed records");
        verify(auditor).warning("foo", "No quantiles could be found for a job with processed records");
        verifyNoMoreInteractions(auditor);
    }

    private AutodetectProcessManager createNonSpyManager(String jobId) {
        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.executor(anyString())).thenReturn(executorService);
        when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(mock(ThreadPool.Cancellable.class));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Job> listener = (ActionListener<Job>) invocationOnMock.getArguments()[1];
            listener.onResponse(createJobDetails(jobId));
            return null;
        }).when(jobManager).getJob(eq(jobId), any());

        AutodetectProcess autodetectProcess = mock(AutodetectProcess.class);
        autodetectFactory = (pid, j, autodetectParams, e, onProcessCrash) -> autodetectProcess;
        return createManager(Settings.EMPTY);
    }

    private AutodetectParams buildAutodetectParams() {
        return new AutodetectParams.Builder("foo")
                .setDataCounts(dataCounts)
                .setModelSizeStats(modelSizeStats)
                .setModelSnapshot(modelSnapshot)
                .setQuantiles(quantiles)
                .build();
    }

    private AutodetectProcessManager createSpyManager() {
        return createSpyManager(Settings.EMPTY);
    }

    private AutodetectProcessManager createSpyManager(Settings settings) {
        AutodetectProcessManager manager = createManager(settings);
        manager = spy(manager);
        doReturn(autodetectCommunicator).when(manager).create(any(), any(), eq(buildAutodetectParams()), any());
        return manager;
    }

    private AutodetectProcessManager createManager(Settings settings) {
        return new AutodetectProcessManager(settings,
            client, threadPool, new NamedXContentRegistry(Collections.emptyList()), auditor, clusterService, jobManager, jobResultsProvider,
            jobResultsPersister, jobDataCountsPersister, annotationPersister, autodetectFactory, normalizerFactory, nativeStorageProvider,
            TestIndexNameExpressionResolver.newInstance());
    }
    private AutodetectProcessManager createSpyManagerAndCallProcessData(String jobId) {
        AutodetectProcessManager manager = createSpyManager();
        JobTask jobTask = mock(JobTask.class);
        when(jobTask.getJobId()).thenReturn(jobId);
        manager.openJob(jobTask, clusterState, DEFAULT_MASTER_NODE_TIMEOUT, (e, b) -> {});
        manager.processData(jobTask, analysisRegistry, createInputStream(""), randomFrom(XContentType.values()),
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

        return builder.build(new Date());
    }

    private static InputStream createInputStream(String input) {
        return new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    }
}
