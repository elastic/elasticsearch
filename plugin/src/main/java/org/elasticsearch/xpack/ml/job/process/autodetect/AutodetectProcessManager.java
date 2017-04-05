/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.OpenJobAction.JobTask;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobRenormalizedResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutoDetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerFactory;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.ml.job.process.normalizer.ScoresUpdater;
import org.elasticsearch.xpack.ml.job.process.normalizer.ShortCircuitingRenormalizer;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class AutodetectProcessManager extends AbstractComponent {

    // TODO: Ideally this setting shouldn't need to exist
    // We should be able from the job config to estimate the memory/cpu a job needs to have,
    // and if we know that then we can prior to assigning a job to a node fail based on the
    // available resources on that node: https://github.com/elastic/x-pack-elasticsearch/issues/546
    // Note: on small instances on cloud, this setting will be set to: 1
    public static final Setting<Integer> MAX_RUNNING_JOBS_PER_NODE =
            Setting.intSetting("max_running_jobs", 10, 1, 512, Setting.Property.NodeScope);

    private final Client client;
    private final ThreadPool threadPool;
    private final JobManager jobManager;
    private final JobProvider jobProvider;
    private final AutodetectProcessFactory autodetectProcessFactory;
    private final NormalizerFactory normalizerFactory;

    private final JobResultsPersister jobResultsPersister;
    private final JobDataCountsPersister jobDataCountsPersister;

    private final ConcurrentMap<String, AutodetectCommunicator> autoDetectCommunicatorByJob;

    private final int maxAllowedRunningJobs;

    private final NamedXContentRegistry xContentRegistry;

    private final Auditor auditor;

    public AutodetectProcessManager(Settings settings, Client client, ThreadPool threadPool,
            JobManager jobManager, JobProvider jobProvider, JobResultsPersister jobResultsPersister,
            JobDataCountsPersister jobDataCountsPersister,
            AutodetectProcessFactory autodetectProcessFactory, NormalizerFactory normalizerFactory,
            NamedXContentRegistry xContentRegistry, Auditor auditor) {
        super(settings);
        this.client = client;
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
        this.maxAllowedRunningJobs = MAX_RUNNING_JOBS_PER_NODE.get(settings);
        this.autodetectProcessFactory = autodetectProcessFactory;
        this.normalizerFactory = normalizerFactory;
        this.jobManager = jobManager;
        this.jobProvider = jobProvider;
        this.jobResultsPersister = jobResultsPersister;
        this.jobDataCountsPersister = jobDataCountsPersister;
        this.autoDetectCommunicatorByJob = new ConcurrentHashMap<>();
        this.auditor = auditor;
    }

    public synchronized void closeAllJobs(String reason) throws IOException {
        int numJobs = autoDetectCommunicatorByJob.size();
        if (numJobs != 0) {
            logger.info("Closing [{}] jobs, because [{}]", numJobs, reason);
        }

        for (Map.Entry<String, AutodetectCommunicator> entry : autoDetectCommunicatorByJob.entrySet()) {
            closeJob(entry.getKey(), false, reason);
        }
    }

    /**
     * Passes data to the native process.
     * This is a blocking call that won't return until all the data has been
     * written to the process.
     * <p>
     * An ElasticsearchStatusException will be thrown is any of these error conditions occur:
     * <ol>
     * <li>If a configured field is missing from the CSV header</li>
     * <li>If JSON data is malformed and we cannot recover parsing</li>
     * <li>If a high proportion of the records the timestamp field that cannot be parsed</li>
     * <li>If a high proportion of the records chronologically out of order</li>
     * </ol>
     *
     * @param jobId  the jobId
     * @param input  Data input stream
     * @param xContentType  the {@link XContentType} of the input
     * @param params Data processing parameters
     * @param handler   Delegate error or datacount results (Count of records, fields, bytes, etc written)
     */
    public void processData(String jobId, InputStream input, XContentType xContentType,
                            DataLoadParams params, BiConsumer<DataCounts, Exception> handler) {
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            throw new IllegalStateException("[" + jobId + "] Cannot process data: no active autodetect process for job");
        }
        communicator.writeToJob(input, xContentType, params, handler);
    }

    /**
     * Flush the running job, ensuring that the native process has had the
     * opportunity to process all data previously sent to it with none left
     * sitting in buffers.
     *
     * @param jobId  The job to flush
     * @param params Parameters about whether interim results calculation
     *               should occur and for which period of time
     */
    public void flushJob(String jobId, InterimResultsParams params, Consumer<Exception> handler) {
        logger.debug("Flushing job {}", jobId);
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            String message = String.format(Locale.ROOT, "[%s] Cannot flush: no active autodetect process for job", jobId);
            logger.debug(message);
            throw new IllegalArgumentException(message);
        }

        communicator.flushJob(params, (aVoid, e) -> {
            if (e == null) {
                handler.accept(null);
            } else {
                String msg = String.format(Locale.ROOT, "[%s] exception while flushing job", jobId);
                logger.error(msg);
                handler.accept(ExceptionsHelper.serverError(msg, e));
            }
        });
    }

    public void writeUpdateProcessMessage(String jobId, List<JobUpdate.DetectorUpdate> updates, ModelPlotConfig config,
                                          Consumer<Exception> handler) {
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            logger.debug("Cannot update model debug config: no active autodetect process for job {}", jobId);
            handler.accept(null);
            return;
        }
        communicator.writeUpdateProcessMessage(config, updates, (aVoid, e) -> {
            if (e == null) {
                handler.accept(null);
            } else {
                handler.accept(e);
            }
        });
    }

    public void openJob(String jobId, JobTask jobTask, boolean ignoreDowntime, Consumer<Exception> handler) {
        Job job = jobManager.getJobOrThrowIfUnknown(jobId);
        jobProvider.getAutodetectParams(job, params -> {
            // We need to fork, otherwise we restore model state from a network thread (several GET api calls):
            threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    handler.accept(e);
                }

                @Override
                protected void doRun() throws Exception {
                    try {
                        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.computeIfAbsent(jobId, id ->
                                create(id, jobTask, params, ignoreDowntime, handler));
                        communicator.writeJobInputHeader();
                        setJobState(jobTask, JobState.OPENED);
                    } catch (Exception e1) {
                        if (e1 instanceof ElasticsearchStatusException) {
                            logger.info(e1.getMessage());
                        } else {
                            String msg = String.format(Locale.ROOT, "[%s] exception while opening job", jobId);
                            logger.error(msg, e1);
                        }
                        setJobState(jobTask, JobState.FAILED, e2 -> handler.accept(e1));
                    }
                }
            });
        }, e1 -> {
            logger.warn("Failed to gather information required to open job [" + jobId + "]", e1);
            setJobState(jobTask, JobState.FAILED, e2 -> handler.accept(e1));
        });
    }

    AutodetectCommunicator create(String jobId, JobTask jobTask, AutodetectParams autodetectParams,
                                  boolean ignoreDowntime, Consumer<Exception> handler) {
        if (autoDetectCommunicatorByJob.size() == maxAllowedRunningJobs) {
            throw new ElasticsearchStatusException("max running job capacity [" + maxAllowedRunningJobs + "] reached",
                    RestStatus.TOO_MANY_REQUESTS);
        }

        notifyLoadingSnapshot(jobId, autodetectParams);

        if (autodetectParams.dataCounts().getProcessedRecordCount() > 0) {
            if (autodetectParams.modelSnapshot() == null) {
                String msg = "No model snapshot could be found for a job with processed records";
                logger.warn("[{}] {}", jobId, msg);
                auditor.warning(jobId, "No model snapshot could be found for a job with processed records");
            }
            if (autodetectParams.quantiles() == null) {
                String msg = "No quantiles could be found for a job with processed records";
                logger.warn("[{}] {}", jobId, msg);
                auditor.warning(jobId, msg);
            }
        }

        Job job = jobManager.getJobOrThrowIfUnknown(jobId);
        // A TP with no queue, so that we fail immediately if there are no threads available
        ExecutorService autoDetectExecutorService = threadPool.executor(MachineLearning.AUTODETECT_THREAD_POOL_NAME);
        DataCountsReporter dataCountsReporter = new DataCountsReporter(settings, job, autodetectParams.dataCounts(),
                jobDataCountsPersister);
        ScoresUpdater scoresUpdater = new ScoresUpdater(job, jobProvider, new JobRenormalizedResultsPersister(settings, client),
                normalizerFactory);
        ExecutorService renormalizerExecutorService = threadPool.executor(MachineLearning.NORMALIZER_THREAD_POOL_NAME);
        Renormalizer renormalizer = new ShortCircuitingRenormalizer(jobId, scoresUpdater,
                renormalizerExecutorService, job.getAnalysisConfig().getUsePerPartitionNormalization());

        AutodetectProcess process = autodetectProcessFactory.createAutodetectProcess(job, autodetectParams.modelSnapshot(),
                autodetectParams.quantiles(), autodetectParams.filters(), ignoreDowntime,
                autoDetectExecutorService, () -> setJobState(jobTask, JobState.FAILED));
        boolean usePerPartitionNormalization = job.getAnalysisConfig().getUsePerPartitionNormalization();
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(
                client, jobId, renormalizer, jobResultsPersister, autodetectParams.modelSizeStats());
        ExecutorService autodetectWorkerExecutor;
        try {
            autodetectWorkerExecutor = createAutodetectExecutorService(autoDetectExecutorService);
            autoDetectExecutorService.submit(() -> processor.process(process, usePerPartitionNormalization));
        } catch (EsRejectedExecutionException e) {
            // If submitting the operation to read the results from the process fails we need to close
            // the process too, so that other submitted operations to threadpool are stopped.
            try {
                IOUtils.close(process);
            } catch (IOException ioe) {
                logger.error("Can't close autodetect", ioe);
            }
            throw e;
        }
        return new AutodetectCommunicator(job, process, dataCountsReporter, processor,
                handler, xContentRegistry, autodetectWorkerExecutor);

    }

    private void notifyLoadingSnapshot(String jobId, AutodetectParams autodetectParams) {
        ModelSnapshot modelSnapshot = autodetectParams.modelSnapshot();
        StringBuilder msgBuilder = new StringBuilder("Loading model snapshot [");
        if (modelSnapshot == null) {
            msgBuilder.append("N/A");
        } else {
            msgBuilder.append(modelSnapshot.getSnapshotId());
            msgBuilder.append("] with latest_record_timestamp [");
            Date snapshotLatestRecordTimestamp = modelSnapshot.getLatestRecordTimeStamp();
            msgBuilder.append(snapshotLatestRecordTimestamp == null ? "N/A" :
                    XContentBuilder.DEFAULT_DATE_PRINTER.print(
                            snapshotLatestRecordTimestamp.getTime()));
        }
        msgBuilder.append("], job latest_record_timestamp [");
        Date jobLatestRecordTimestamp = autodetectParams.dataCounts().getLatestRecordTimeStamp();
        msgBuilder.append(jobLatestRecordTimestamp == null ? "N/A"
                : XContentBuilder.DEFAULT_DATE_PRINTER.print(jobLatestRecordTimestamp.getTime()));
        msgBuilder.append("]");
        String msg = msgBuilder.toString();
        logger.info("[{}] {}", jobId, msg);
        auditor.info(jobId, msg);
    }

    /**
     * Stop the running job and mark it as finished.<br>
     *
     * @param jobId   The job to stop
     * @param restart Whether the job should be restarted by persistent tasks
     * @param reason  The reason for closing the job
     */
    public void closeJob(String jobId, boolean restart, String reason) {
        logger.debug("Attempting to close job [{}], because [{}]", jobId, reason);
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.remove(jobId);
        if (communicator == null) {
            logger.debug("Cannot close: no active autodetect process for job {}", jobId);
            return;
        }

        if (reason == null) {
            logger.info("Closing job [{}]", jobId);
        } else {
            logger.info("Closing job [{}], because [{}]", jobId, reason);
        }

        try {
            communicator.close(restart, reason);
        } catch (Exception e) {
            logger.warn("Exception closing stopped process input stream", e);
            throw ExceptionsHelper.serverError("Exception closing stopped process input stream", e);
        }
    }

    int numberOfOpenJobs() {
        return autoDetectCommunicatorByJob.size();
    }

    boolean jobHasActiveAutodetectProcess(String jobId) {
        return autoDetectCommunicatorByJob.get(jobId) != null;
    }

    public Optional<Duration> jobOpenTime(String jobId) {
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            return Optional.empty();
        }
        return Optional.of(Duration.between(communicator.getProcessStartTime(), ZonedDateTime.now()));
    }

    private void setJobState(JobTask jobTask, JobState state) {
        jobTask.updatePersistentStatus(state, new ActionListener<PersistentTask<?>>() {
            @Override
            public void onResponse(PersistentTask<?> persistentTask) {
                logger.info("Successfully set job state to [{}] for job [{}]", state, jobTask.getJobId());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Could not set job state to [" + state + "] for job [" + jobTask.getJobId() + "]", e);
            }
        });
    }

    public void setJobState(JobTask jobTask, JobState state, CheckedConsumer<Exception, IOException> handler) {
        jobTask.updatePersistentStatus(state, new ActionListener<PersistentTask<?>>() {
                    @Override
                    public void onResponse(PersistentTask<?> persistentTask) {
                        try {
                            handler.accept(null);
                        } catch (IOException e1) {
                            logger.warn("Error while delegating response", e1);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            handler.accept(e);
                        } catch (IOException e1) {
                            logger.warn("Error while delegating exception [" + e.getMessage() + "]", e1);
                        }
                    }
                });
    }

    public Optional<Tuple<DataCounts, ModelSizeStats>> getStatistics(String jobId) {
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            return Optional.empty();
        }
        return Optional.of(new Tuple<>(communicator.getDataCounts(), communicator.getModelSizeStats()));
    }

    ExecutorService createAutodetectExecutorService(ExecutorService executorService) {
        AutodetectWorkerExecutorService autoDetectWorkerExecutor = new AutodetectWorkerExecutorService(threadPool.getThreadContext());
        executorService.submit(autoDetectWorkerExecutor::start);
        return autoDetectWorkerExecutor;
    }

    /*
     * The autodetect native process can only handle a single operation at a time. In order to guarantee that, all
     * operations are initially added to a queue and a worker thread from ml autodetect threadpool will process each
     * operation at a time.
     */
    class AutodetectWorkerExecutorService extends AbstractExecutorService {

        private final ThreadContext contextHolder;
        private final CountDownLatch awaitTermination = new CountDownLatch(1);
        private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(100);

        private volatile boolean running = true;

        AutodetectWorkerExecutorService(ThreadContext contextHolder) {
            this.contextHolder = contextHolder;
        }

        @Override
        public void shutdown() {
            running = false;
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public boolean isShutdown() {
            return running == false;
        }

        @Override
        public boolean isTerminated() {
            return awaitTermination.getCount() == 0;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return awaitTermination.await(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            boolean added = queue.offer(contextHolder.preserveContext(command));
            if (added == false) {
                throw new ElasticsearchStatusException("Unable to submit operation", RestStatus.TOO_MANY_REQUESTS);
            }
        }

        void start() {
            try {
                while (running) {
                    Runnable runnable = queue.poll(500, TimeUnit.MILLISECONDS);
                    if (runnable != null) {
                        try {
                            runnable.run();
                        } catch (Exception e) {
                            logger.error("error handeling job operation", e);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                awaitTermination.countDown();
            }
        }

    }
}
