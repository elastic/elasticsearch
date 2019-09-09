/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.TransportOpenJobAction.JobTask;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobRenormalizedResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.persistence.ScheduledEventsQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutodetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerFactory;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.ml.job.process.normalizer.ScoresUpdater;
import org.elasticsearch.xpack.ml.job.process.normalizer.ShortCircuitingRenormalizer;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.process.NativeStorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class AutodetectProcessManager implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AutodetectProcessManager.class);

    private final Client client;
    private final Environment environment;
    private final ThreadPool threadPool;
    private final JobManager jobManager;
    private final JobResultsProvider jobResultsProvider;
    private final AutodetectProcessFactory autodetectProcessFactory;
    private final NormalizerFactory normalizerFactory;

    private final JobResultsPersister jobResultsPersister;
    private final JobDataCountsPersister jobDataCountsPersister;

    private NativeStorageProvider nativeStorageProvider;
    private final ConcurrentMap<Long, ProcessContext> processByAllocation = new ConcurrentHashMap<>();

    private volatile int maxAllowedRunningJobs;

    private final NamedXContentRegistry xContentRegistry;

    private final AnomalyDetectionAuditor auditor;

    private volatile boolean upgradeInProgress;

    public AutodetectProcessManager(Environment environment, Settings settings, Client client, ThreadPool threadPool,
                                    NamedXContentRegistry xContentRegistry, AnomalyDetectionAuditor auditor, ClusterService clusterService,
                                    JobManager jobManager, JobResultsProvider jobResultsProvider, JobResultsPersister jobResultsPersister,
                                    JobDataCountsPersister jobDataCountsPersister, AutodetectProcessFactory autodetectProcessFactory,
                                    NormalizerFactory normalizerFactory, NativeStorageProvider nativeStorageProvider) {
        this.environment = environment;
        this.client = client;
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
        this.maxAllowedRunningJobs = MachineLearning.MAX_OPEN_JOBS_PER_NODE.get(settings);
        this.autodetectProcessFactory = autodetectProcessFactory;
        this.normalizerFactory = normalizerFactory;
        this.jobManager = jobManager;
        this.jobResultsProvider = jobResultsProvider;
        this.jobResultsPersister = jobResultsPersister;
        this.jobDataCountsPersister = jobDataCountsPersister;
        this.auditor = auditor;
        this.nativeStorageProvider = Objects.requireNonNull(nativeStorageProvider);
        clusterService.addListener(this);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(MachineLearning.MAX_OPEN_JOBS_PER_NODE, this::setMaxAllowedRunningJobs);
    }

    void setMaxAllowedRunningJobs(int maxAllowedRunningJobs) {
        this.maxAllowedRunningJobs = maxAllowedRunningJobs;
    }

    public synchronized void closeAllJobsOnThisNode(String reason) {
        int numJobs = processByAllocation.size();
        if (numJobs != 0) {
            logger.info("Closing [{}] jobs, because [{}]", numJobs, reason);

            for (ProcessContext process : processByAllocation.values()) {
                closeJob(process.getJobTask(), false, reason);
            }
        }
    }

    public void killProcess(JobTask jobTask, boolean awaitCompletion, String reason) {
        logger.trace("[{}] Killing process: awaitCompletion = [{}]; reason = [{}]", jobTask.getJobId(), awaitCompletion, reason);
        ProcessContext processContext = processByAllocation.remove(jobTask.getAllocationId());
        if (processContext != null) {
            processContext.newKillBuilder()
                    .setAwaitCompletion(awaitCompletion)
                    .setFinish(true)
                    .setReason(reason)
                    .setShouldFinalizeJob(upgradeInProgress == false)
                    .kill();
        } else {
            // If the process is missing but the task exists this is most likely
            // due to 2 reasons. The first is because the job went into the failed
            // state then the node restarted causing the task to be recreated
            // but the failed process wasn't. The second is that the job went into
            // the failed state and the user tries to remove it force-deleting it.
            // Force-delete issues a kill but the process will not be present
            // as it is cleaned up already. In both cases, we still need to remove
            // the task from the TaskManager (which is what the kill would do)
            logger.trace("[{}] Marking job task as completed", jobTask.getJobId());
            jobTask.markAsCompleted();
        }
    }

    public void killAllProcessesOnThisNode() {
        Iterator<ProcessContext> iterator = processByAllocation.values().iterator();
        while (iterator.hasNext()) {
            ProcessContext processContext = iterator.next();
            processContext.newKillBuilder()
                    .setAwaitCompletion(false)
                    .setFinish(false)
                    .setSilent(true)
                    .kill();
            iterator.remove();
        }
    }

    /**
     * Initiate background persistence of the job
     * @param jobTask The job task
     * @param handler Listener
     */
    public void persistJob(JobTask jobTask, Consumer<Exception> handler) {
        AutodetectCommunicator communicator = getOpenAutodetectCommunicator(jobTask);
        if (communicator == null) {
            String message = String.format(Locale.ROOT, "Cannot persist because job [%s] does not have a corresponding autodetect process",
                jobTask.getJobId());
            logger.debug(message);
            handler.accept(ExceptionsHelper.conflictStatusException(message));
            return;
        }
        communicator.persistJob((aVoid, e) -> handler.accept(e));
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
     * @param jobTask          The job task
     * @param analysisRegistry Registry of analyzer components - this is used to build a categorization analyzer if necessary
     * @param input            Data input stream
     * @param xContentType     the {@link XContentType} of the input
     * @param params           Data processing parameters
     * @param handler          Delegate error or datacount results (Count of records, fields, bytes, etc written as a result of this call)
     */
    public void processData(JobTask jobTask, AnalysisRegistry analysisRegistry, InputStream input,
                            XContentType xContentType, DataLoadParams params, BiConsumer<DataCounts, Exception> handler) {
        AutodetectCommunicator communicator = getOpenAutodetectCommunicator(jobTask);
        if (communicator == null) {
            throw ExceptionsHelper.conflictStatusException("Cannot process data because job [" + jobTask.getJobId() +
                "] does not have a corresponding autodetect process");
        }
        communicator.writeToJob(input, analysisRegistry, xContentType, params, handler);
    }

    /**
     * Flush the running job, ensuring that the native process has had the
     * opportunity to process all data previously sent to it with none left
     * sitting in buffers.
     *
     * @param jobTask   The job task
     * @param params Parameters describing the controls that will accompany the flushing
     *               (e.g. calculating interim results, time control, etc.)
     */
    public void flushJob(JobTask jobTask, FlushJobParams params, ActionListener<FlushAcknowledgement> handler) {
        logger.debug("Flushing job {}", jobTask.getJobId());
        AutodetectCommunicator communicator = getOpenAutodetectCommunicator(jobTask);
        if (communicator == null) {
            String message = String.format(Locale.ROOT, "Cannot flush because job [%s] does not have a corresponding autodetect process",
                jobTask.getJobId());
            logger.debug(message);
            handler.onFailure(ExceptionsHelper.conflictStatusException(message));
            return;
        }

        communicator.flushJob(params, (flushAcknowledgement, e) -> {
            if (e != null) {
                String msg = String.format(Locale.ROOT, "[%s] exception while flushing job", jobTask.getJobId());
                logger.error(msg);
                handler.onFailure(ExceptionsHelper.serverError(msg, e));
            } else {
                handler.onResponse(flushAcknowledgement);
            }
        });
    }

    /**
     * Do a forecast for the running job.
     *
     * @param jobTask   The job task
     * @param params    Forecast parameters
     */
    public void forecastJob(JobTask jobTask, ForecastParams params, Consumer<Exception> handler) {
        String jobId = jobTask.getJobId();
        logger.debug("Forecasting job {}", jobId);
        AutodetectCommunicator communicator = getOpenAutodetectCommunicator(jobTask);
        if (communicator == null) {
            String message = String.format(Locale.ROOT,
                "Cannot forecast because job [%s] does not have a corresponding autodetect process", jobId);
            logger.debug(message);
            handler.accept(ExceptionsHelper.conflictStatusException(message));
            return;
        }

        communicator.forecastJob(params, (aVoid, e) -> {
            if (e == null) {
                handler.accept(null);
            } else {
                String msg = String.format(Locale.ROOT, "[%s] exception while forecasting job", jobId);
                logger.error(msg, e);
                handler.accept(ExceptionsHelper.serverError(msg, e));
            }
        });
    }

    public void writeUpdateProcessMessage(JobTask jobTask, UpdateParams updateParams, Consumer<Exception> handler) {
        AutodetectCommunicator communicator = getOpenAutodetectCommunicator(jobTask);
        if (communicator == null) {
            String message = "Cannot update the job config because job [" + jobTask.getJobId() +
                "] does not have a corresponding autodetect process";
            logger.debug(message);
            handler.accept(ExceptionsHelper.conflictStatusException(message));
            return;
        }

        UpdateProcessMessage.Builder updateProcessMessage = new UpdateProcessMessage.Builder();
        updateProcessMessage.setModelPlotConfig(updateParams.getModelPlotConfig());
        updateProcessMessage.setDetectorUpdates(updateParams.getDetectorUpdates());

        // Step 3. Set scheduled events on message and write update process message
        ActionListener<QueryPage<ScheduledEvent>> eventsListener = ActionListener.wrap(
                events -> {
                    updateProcessMessage.setScheduledEvents(events == null ? null : events.results());
                    communicator.writeUpdateProcessMessage(updateProcessMessage.build(), (aVoid, e) -> {
                        if (e == null) {
                            handler.accept(null);
                        } else {
                            handler.accept(e);
                        }
                    });
                }, handler
        );

        // Step 2. Set the filter on the message and get scheduled events
        ActionListener<MlFilter> filterListener = ActionListener.wrap(
                filter -> {
                    updateProcessMessage.setFilter(filter);

                    if (updateParams.isUpdateScheduledEvents()) {
                        jobManager.getJob(jobTask.getJobId(), new ActionListener<Job>() {
                            @Override
                            public void onResponse(Job job) {
                                DataCounts dataCounts = getStatistics(jobTask).get().v1();
                                ScheduledEventsQueryBuilder query = new ScheduledEventsQueryBuilder()
                                        .start(job.earliestValidTimestamp(dataCounts));
                                jobResultsProvider.scheduledEventsForJob(jobTask.getJobId(), job.getGroups(), query, eventsListener);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                handler.accept(e);
                            }
                        });
                    } else {
                        eventsListener.onResponse(null);
                    }
                }, handler
        );

        // Step 1. Get the filter
        if (updateParams.getFilter() == null) {
            filterListener.onResponse(null);
        } else {
            GetFiltersAction.Request getFilterRequest = new GetFiltersAction.Request();
            getFilterRequest.setFilterId(updateParams.getFilter().getId());
            executeAsyncWithOrigin(client, ML_ORIGIN, GetFiltersAction.INSTANCE, getFilterRequest,
                    new ActionListener<GetFiltersAction.Response>() {

                @Override
                public void onResponse(GetFiltersAction.Response response) {
                    filterListener.onResponse(response.getFilters().results().get(0));
                }

                @Override
                public void onFailure(Exception e) {
                    handler.accept(e);
                }
            });
        }
    }

    public void openJob(JobTask jobTask, ClusterState clusterState, BiConsumer<Exception, Boolean> closeHandler) {
        String jobId = jobTask.getJobId();
        logger.info("Opening job [{}]", jobId);

        // Start the process
        ActionListener<Boolean> stateAliasHandler = ActionListener.wrap(
            r -> {
                jobManager.getJob(jobId, ActionListener.wrap(
                    job -> {
                        if (job.getJobVersion() == null) {
                            closeHandler.accept(ExceptionsHelper.badRequestException("Cannot open job [" + jobId
                                + "] because jobs created prior to version 5.5 are not supported"), true);
                            return;
                        }

                        processByAllocation.putIfAbsent(jobTask.getAllocationId(), new ProcessContext(jobTask));
                        jobResultsProvider.getAutodetectParams(job, params -> {
                            // We need to fork, otherwise we restore model state from a network thread (several GET api calls):
                            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                                @Override
                                public void onFailure(Exception e) {
                                    closeHandler.accept(e, true);
                                }

                                @Override
                                protected void doRun() {
                                    ProcessContext processContext = processByAllocation.get(jobTask.getAllocationId());
                                    if (processContext == null) {
                                        logger.debug("Aborted opening job [{}] as it has been closed", jobId);
                                        return;
                                    }

                                    try {
                                        if (createProcessAndSetRunning(processContext, job, params, closeHandler)) {
                                            processContext.getAutodetectCommunicator().restoreState(params.modelSnapshot());
                                            setJobState(jobTask, JobState.OPENED);
                                        }
                                    } catch (Exception e1) {
                                        // No need to log here as the persistent task framework will log it
                                        try {
                                            // Don't leave a partially initialised process hanging around
                                            processContext.newKillBuilder()
                                                .setAwaitCompletion(false)
                                                .setFinish(false)
                                                .kill();
                                            processByAllocation.remove(jobTask.getAllocationId());
                                        } finally {
                                            setJobState(jobTask, JobState.FAILED, e1.getMessage(), e2 -> closeHandler.accept(e1, true));
                                        }
                                    }
                                }
                            });
                        }, e1 -> {
                            logger.warn("Failed to gather information required to open job [" + jobId + "]", e1);
                            setJobState(jobTask, JobState.FAILED, e1.getMessage(), e2 -> closeHandler.accept(e1, true));
                        });
                    },
                    e -> closeHandler.accept(e, true)
                ));
            },
            e -> closeHandler.accept(e, true));

        // Make sure the state index and alias exist
        ActionListener<Boolean> resultsMappingUpdateHandler = ActionListener.wrap(
            ack -> AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(client, clusterState, stateAliasHandler),
            e -> closeHandler.accept(e, true)
        );

        // Try adding the results doc mapping - this updates to the latest version if an old mapping is present
        ElasticsearchMappings.addDocMappingIfMissing(AnomalyDetectorsIndex.jobResultsAliasedName(jobId),
            ElasticsearchMappings::resultsMapping, client, clusterState, resultsMappingUpdateHandler);
    }

    private boolean createProcessAndSetRunning(ProcessContext processContext,
                                               Job job,
                                               AutodetectParams params,
                                               BiConsumer<Exception, Boolean> handler) throws IOException {
        // At this point we lock the process context until the process has been started.
        // The reason behind this is to ensure closing the job does not happen before
        // the process is started as that can result to the job getting seemingly closed
        // but the actual process is hanging alive.
        processContext.tryLock();
        try {
            if (processContext.getState() != ProcessContext.ProcessStateName.NOT_RUNNING) {
                logger.debug("Cannot open job [{}] when its state is [{}]",
                    job.getId(), processContext.getState().getClass().getName());
                return false;
            }
            AutodetectCommunicator communicator = create(processContext.getJobTask(), job, params, handler);
            communicator.writeHeader();
            processContext.setRunning(communicator);
            return true;
        } finally {
            // Now that the process is running and we have updated its state we can unlock.
            // It is important to unlock before we initialize the communicator (ie. load the model state)
            // as that may be a long-running method.
            processContext.unlock();
        }
    }

    AutodetectCommunicator create(JobTask jobTask, Job job, AutodetectParams autodetectParams, BiConsumer<Exception, Boolean> handler) {
        // Copy for consistency within a single method call
        int localMaxAllowedRunningJobs = maxAllowedRunningJobs;
        // Closing jobs can still be using some or all threads in MachineLearning.JOB_COMMS_THREAD_POOL_NAME
        // that an open job uses, so include them too when considering if enough threads are available.
        int currentRunningJobs = processByAllocation.size();
        // TODO: in future this will also need to consider jobs that are not anomaly detector jobs
        if (currentRunningJobs > localMaxAllowedRunningJobs) {
            throw new ElasticsearchStatusException("max running job capacity [" + localMaxAllowedRunningJobs + "] reached",
                    RestStatus.TOO_MANY_REQUESTS);
        }

        String jobId = jobTask.getJobId();
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

        // A TP with no queue, so that we fail immediately if there are no threads available
        ExecutorService autodetectExecutorService = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
        DataCountsReporter dataCountsReporter = new DataCountsReporter(job, autodetectParams.dataCounts(), jobDataCountsPersister);
        ScoresUpdater scoresUpdater = new ScoresUpdater(job, jobResultsProvider,
                new JobRenormalizedResultsPersister(job.getId(), client), normalizerFactory);
        ExecutorService renormalizerExecutorService = threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME);
        Renormalizer renormalizer = new ShortCircuitingRenormalizer(jobId, scoresUpdater,
                renormalizerExecutorService);

        AutodetectProcess process = autodetectProcessFactory.createAutodetectProcess(job, autodetectParams, autodetectExecutorService,
                onProcessCrash(jobTask));
        AutodetectResultProcessor processor =
            new AutodetectResultProcessor(
                client,
                auditor,
                jobId,
                renormalizer,
                jobResultsPersister,
                process,
                autodetectParams.modelSizeStats(),
                autodetectParams.timingStats());
        ExecutorService autodetectWorkerExecutor;
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            autodetectWorkerExecutor = createAutodetectExecutorService(autodetectExecutorService);
            autodetectExecutorService.submit(processor::process);
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
        return new AutodetectCommunicator(job, environment, process, new StateStreamer(client), dataCountsReporter, processor, handler,
                xContentRegistry, autodetectWorkerExecutor);

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
                    XContentElasticsearchExtension.DEFAULT_DATE_PRINTER.print(
                            snapshotLatestRecordTimestamp.getTime()));
        }
        msgBuilder.append("], job latest_record_timestamp [");
        Date jobLatestRecordTimestamp = autodetectParams.dataCounts().getLatestRecordTimeStamp();
        msgBuilder.append(jobLatestRecordTimestamp == null ? "N/A"
                : XContentElasticsearchExtension.DEFAULT_DATE_PRINTER.print(jobLatestRecordTimestamp.getTime()));
        msgBuilder.append("]");
        String msg = msgBuilder.toString();
        logger.info("[{}] {}", jobId, msg);
        auditor.info(jobId, msg);
    }

    private Consumer<String> onProcessCrash(JobTask jobTask) {
        return (reason) -> {
            ProcessContext processContext = processByAllocation.remove(jobTask.getAllocationId());
            if (processContext != null) {
                AutodetectCommunicator communicator = processContext.getAutodetectCommunicator();
                if (communicator != null) {
                    communicator.destroyCategorizationAnalyzer();
                }
            }
            setJobState(jobTask, JobState.FAILED, reason);
            try {
                nativeStorageProvider.cleanupLocalTmpStorage(jobTask.getDescription());
            } catch (IOException e) {
                logger.error(new ParameterizedMessage("[{}] Failed to delete temporary files", jobTask.getJobId()), e);
            }
        };
    }

    /**
     * Stop the running job and mark it as finished.
     *
     * @param jobTask The job to stop
     * @param restart Whether the job should be restarted by persistent tasks
     * @param reason  The reason for closing the job
     */
    public void closeJob(JobTask jobTask, boolean restart, String reason) {
        String jobId = jobTask.getJobId();
        long allocationId = jobTask.getAllocationId();
        logger.debug("Attempting to close job [{}], because [{}]", jobId, reason);
        // don't remove the process context immediately, because we need to ensure
        // it is reachable to enable killing a job while it is closing
        ProcessContext processContext = processByAllocation.get(allocationId);
        if (processContext == null) {
            logger.debug("Cannot close job [{}] as it has already been closed", jobId);
            return;
        }

        processContext.tryLock();
        try {
            if (processContext.setDying() == false) {
                logger.debug("Cannot close job [{}] as it has been marked as dying", jobId);
                // The only way we can get here is if 2 close requests are made very close together.
                // The other close has done the work so it's safe to return here without doing anything.
                return;
            }

            if (reason == null) {
                logger.info("Closing job [{}]", jobId);
            } else {
                logger.info("Closing job [{}], because [{}]", jobId, reason);
            }

            AutodetectCommunicator communicator = processContext.getAutodetectCommunicator();
            if (communicator == null) {
                logger.debug("Job [{}] is being closed before its process is started", jobId);
                jobTask.markAsCompleted();
            } else {
                communicator.close(restart, reason);
            }

            processByAllocation.remove(allocationId);
        } catch (Exception e) {
            // If the close failed because the process has explicitly been killed by us then just pass on that exception
            if (e instanceof ElasticsearchStatusException && ((ElasticsearchStatusException) e).status() == RestStatus.CONFLICT) {
                throw e;
            }
            logger.warn("[" + jobId + "] Exception closing autodetect process", e);
            setJobState(jobTask, JobState.FAILED, e.getMessage());
            throw ExceptionsHelper.serverError("Exception closing autodetect process", e);
        } finally {
            // to ensure the contract that multiple simultaneous close calls for the same job wait until
            // the job is closed is honoured, hold the lock throughout the close procedure so that another
            // thread that gets into this method blocks until the first thread has finished closing the job
            processContext.unlock();
        }
        // delete any tmp storage
        try {
            nativeStorageProvider.cleanupLocalTmpStorage(jobTask.getDescription());
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("[{}] Failed to delete temporary files", jobId), e);
        }
    }

    int numberOfOpenJobs() {
        return (int) processByAllocation.values().stream()
                .filter(p -> p.getState() != ProcessContext.ProcessStateName.DYING)
                .count();
    }

    boolean jobHasActiveAutodetectProcess(JobTask jobTask) {
        return getAutodetectCommunicator(jobTask) != null;
    }

    private AutodetectCommunicator getAutodetectCommunicator(JobTask jobTask) {
        return processByAllocation.getOrDefault(jobTask.getAllocationId(), new ProcessContext(jobTask)).getAutodetectCommunicator();
    }

    private AutodetectCommunicator getOpenAutodetectCommunicator(JobTask jobTask) {
        ProcessContext processContext = processByAllocation.get(jobTask.getAllocationId());
        if (processContext != null && processContext.getState() == ProcessContext.ProcessStateName.RUNNING) {
            return processContext.getAutodetectCommunicator();
        }
        return null;
    }

    public boolean hasOpenAutodetectCommunicator(long jobAllocationId) {
        ProcessContext processContext = processByAllocation.get(jobAllocationId);
        if (processContext != null && processContext.getState() == ProcessContext.ProcessStateName.RUNNING) {
            return processContext.getAutodetectCommunicator() != null;
        }
        return false;
    }

    public Optional<Duration> jobOpenTime(JobTask jobTask) {
        AutodetectCommunicator communicator = getAutodetectCommunicator(jobTask);
        if (communicator == null) {
            return Optional.empty();
        }
        return Optional.of(Duration.between(communicator.getProcessStartTime(), ZonedDateTime.now()));
    }

    void setJobState(JobTask jobTask, JobState state, String reason) {
        JobTaskState jobTaskState = new JobTaskState(state, jobTask.getAllocationId(), reason);
        jobTask.updatePersistentTaskState(jobTaskState, new ActionListener<PersistentTask<?>>() {
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

    void setJobState(JobTask jobTask, JobState state) {
        setJobState(jobTask, state, null);
    }

    void setJobState(JobTask jobTask, JobState state, String reason, CheckedConsumer<Exception, IOException> handler) {
        JobTaskState jobTaskState = new JobTaskState(state, jobTask.getAllocationId(), reason);
        jobTask.updatePersistentTaskState(jobTaskState, new ActionListener<PersistentTask<?>>() {
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

    public Optional<Tuple<DataCounts, Tuple<ModelSizeStats, TimingStats>>> getStatistics(JobTask jobTask) {
        AutodetectCommunicator communicator = getAutodetectCommunicator(jobTask);
        if (communicator == null) {
            return Optional.empty();
        }
        return Optional.of(
            new Tuple<>(communicator.getDataCounts(), new Tuple<>(communicator.getModelSizeStats(), communicator.getTimingStats())));
    }

    ExecutorService createAutodetectExecutorService(ExecutorService executorService) {
        AutodetectWorkerExecutorService autodetectWorkerExecutor = new AutodetectWorkerExecutorService(threadPool.getThreadContext());
        executorService.submit(autodetectWorkerExecutor::start);
        return autodetectWorkerExecutor;
    }

    public ByteSizeValue getMinLocalStorageAvailable() {
        return nativeStorageProvider.getMinLocalStorageAvailable();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        upgradeInProgress = MlMetadata.getMlMetadata(event.state()).isUpgradeMode();
    }

}
