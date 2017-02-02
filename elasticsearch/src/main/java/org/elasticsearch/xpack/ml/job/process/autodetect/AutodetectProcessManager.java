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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.UpdateJobStateAction;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.job.metadata.Allocation;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobRenormalizedResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutoDetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerFactory;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.ml.job.process.normalizer.ScoresUpdater;
import org.elasticsearch.xpack.ml.job.process.normalizer.ShortCircuitingRenormalizer;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class AutodetectProcessManager extends AbstractComponent {

    // TODO (norelease) default needs to be reconsidered
    // Cannot be dynamic because the thread pool that is sized to match cannot be resized
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

    public AutodetectProcessManager(Settings settings, Client client, ThreadPool threadPool, JobManager jobManager,
                                    JobProvider jobProvider, JobResultsPersister jobResultsPersister,
                                    JobDataCountsPersister jobDataCountsPersister,
                                    AutodetectProcessFactory autodetectProcessFactory, NormalizerFactory normalizerFactory) {
        super(settings);
        this.client = client;
        this.threadPool = threadPool;
        this.maxAllowedRunningJobs = MAX_RUNNING_JOBS_PER_NODE.get(settings);
        this.autodetectProcessFactory = autodetectProcessFactory;
        this.normalizerFactory = normalizerFactory;
        this.jobManager = jobManager;
        this.jobProvider = jobProvider;

        this.jobResultsPersister = jobResultsPersister;
        this.jobDataCountsPersister = jobDataCountsPersister;

        this.autoDetectCommunicatorByJob = new ConcurrentHashMap<>();
    }

    /**
     * Passes data to the native process.
     * This is a blocking call that won't return until all the data has been
     * written to the process.
     *
     * An ElasticsearchStatusException will be thrown is any of these error conditions occur:
     * <ol>
     *     <li>If a configured field is missing from the CSV header</li>
     *     <li>If JSON data is malformed and we cannot recover parsing</li>
     *     <li>If a high proportion of the records the timestamp field that cannot be parsed</li>
     *     <li>If a high proportion of the records chronologically out of order</li>
     * </ol>
     *
     * @param jobId     the jobId
     * @param input     Data input stream
     * @param params    Data processing parameters
     * @return Count of records, fields, bytes, etc written
     */
    public DataCounts processData(String jobId, InputStream input, DataLoadParams params) {
        Allocation allocation = jobManager.getJobAllocation(jobId);
        if (allocation.getState() != JobState.OPENED) {
            throw new IllegalArgumentException("job [" + jobId + "] state is [" + allocation.getState() + "], but must be ["
                    + JobState.OPENED + "] for processing data");
        }

        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            throw new IllegalStateException("job [" +  jobId + "] with state [" + allocation.getState() + "] hasn't been started");
        }
        try {
            return communicator.writeToJob(input, params);
            // TODO check for errors from autodetect
        } catch (IOException e) {
            String msg = String.format(Locale.ROOT, "Exception writing to process for job %s", jobId);
            if (e.getCause() instanceof TimeoutException) {
                logger.warn("Connection to process was dropped due to a timeout - if you are feeding this job from a connector it " +
                        "may be that your connector stalled for too long", e.getCause());
            } else {
                logger.error("Unexpected exception", e);
            }
            throw ExceptionsHelper.serverError(msg, e);
        }
    }

    /**
     * Flush the running job, ensuring that the native process has had the
     * opportunity to process all data previously sent to it with none left
     * sitting in buffers.
     *
     * @param jobId The job to flush
     * @param params Parameters about whether interim results calculation
     * should occur and for which period of time
     */
    public void flushJob(String jobId, InterimResultsParams params) {
        logger.debug("Flushing job {}", jobId);
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            String message = String.format(Locale.ROOT, "[%s] Cannot flush: no active autodetect process for job", jobId);
            logger.debug(message);
            throw new IllegalArgumentException(message);
        }
        try {
            communicator.flushJob(params);
            // TODO check for errors from autodetect
        } catch (IOException ioe) {
            String msg = String.format(Locale.ROOT, "[%s] exception while flushing job", jobId);
            logger.error(msg);
            throw ExceptionsHelper.serverError(msg, ioe);
        }
    }

    public void writeUpdateConfigMessage(String jobId, String config) throws IOException {
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            logger.debug("Cannot update config: no active autodetect process for job {}", jobId);
            return;
        }
        communicator.writeUpdateConfigMessage(config);
        // TODO check for errors from autodetect
    }

    public void openJob(String jobId, boolean ignoreDowntime, Consumer<Exception> handler) {
        gatherRequiredInformation(jobId, (dataCounts, modelSnapshot, quantiles, filters) -> {
            autoDetectCommunicatorByJob.computeIfAbsent(jobId, id -> {
                AutodetectCommunicator communicator =
                        create(id, dataCounts, modelSnapshot, quantiles, filters, ignoreDowntime, handler);
                try {
                    communicator.writeJobInputHeader();
                } catch (IOException ioe) {
                    String msg = String.format(Locale.ROOT, "[%s] exception while opening job", jobId);
                    logger.error(msg);
                    throw ExceptionsHelper.serverError(msg, ioe);
                }
                setJobState(jobId, JobState.OPENED);
                return communicator;
            });
        }, handler);
    }

    // TODO: add a method on JobProvider that fetches all required info via 1 msearch call, so that we have a single lambda
    // instead of 4 nested lambdas.
    void gatherRequiredInformation(String jobId, MultiConsumer handler, Consumer<Exception> errorHandler) {
        Job job = jobManager.getJobOrThrowIfUnknown(jobId);
        jobProvider.dataCounts(jobId, dataCounts -> {
            jobProvider.modelSnapshots(jobId, 0, 1, page -> {
                ModelSnapshot modelSnapshot = page.results().isEmpty() ? null : page.results().get(0);
                jobProvider.getQuantiles(jobId, quantiles -> {
                    Set<String> ids = job.getAnalysisConfig().extractReferencedFilters();
                    jobProvider.getFilters(filterDocument -> handler.accept(dataCounts, modelSnapshot, quantiles, filterDocument),
                            errorHandler, ids);
                }, errorHandler);
            }, errorHandler);
        }, errorHandler);
    }

    interface MultiConsumer {

        void accept(DataCounts dataCounts, ModelSnapshot modelSnapshot, Quantiles quantiles, Set<MlFilter> filters);

    }

    AutodetectCommunicator create(String jobId, DataCounts dataCounts, ModelSnapshot modelSnapshot, Quantiles quantiles,
                                  Set<MlFilter> filters, boolean ignoreDowntime, Consumer<Exception> handler) {
        if (autoDetectCommunicatorByJob.size() == maxAllowedRunningJobs) {
            throw new ElasticsearchStatusException("max running job capacity [" + maxAllowedRunningJobs + "] reached",
                    RestStatus.CONFLICT);
        }

        Job job = jobManager.getJobOrThrowIfUnknown(jobId);
        // A TP with no queue, so that we fail immediately if there are no threads available
        ExecutorService executorService = threadPool.executor(MlPlugin.AUTODETECT_PROCESS_THREAD_POOL_NAME);
        try (DataCountsReporter dataCountsReporter = new DataCountsReporter(threadPool, settings, job.getId(), dataCounts,
                jobDataCountsPersister)) {
            ScoresUpdater scoresUpdater = new ScoresUpdater(job, jobProvider, new JobRenormalizedResultsPersister(settings, client),
                    normalizerFactory);
            Renormalizer renormalizer = new ShortCircuitingRenormalizer(jobId, scoresUpdater,
                    threadPool.executor(MlPlugin.THREAD_POOL_NAME), job.getAnalysisConfig().getUsePerPartitionNormalization());

            AutodetectProcess process = autodetectProcessFactory.createAutodetectProcess(job, modelSnapshot, quantiles, filters,
                    ignoreDowntime, executorService);
            boolean usePerPartitionNormalization = job.getAnalysisConfig().getUsePerPartitionNormalization();
            AutoDetectResultProcessor processor = new AutoDetectResultProcessor(jobId, renormalizer, jobResultsPersister);
            try {
                executorService.submit(() -> processor.process(process, usePerPartitionNormalization));
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
            return new AutodetectCommunicator(job, process, dataCountsReporter, processor, handler);
        }
    }

    /**
     * Stop the running job and mark it as finished.<br>
     *  @param jobId The job to stop
     *
     */
    public void closeJob(String jobId) {
        logger.debug("Closing job {}", jobId);
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.remove(jobId);
        if (communicator == null) {
            logger.debug("Cannot close: no active autodetect process for job {}", jobId);
            return;
        }

        try {
            communicator.close();
            setJobState(jobId, JobState.CLOSED);
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

    public Duration jobUpTime(String jobId) {
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            return Duration.ZERO;
        }
        return Duration.between(communicator.getProcessStartTime(), ZonedDateTime.now());
    }

    private void setJobState(String jobId, JobState state) {
        UpdateJobStateAction.Request request = new UpdateJobStateAction.Request(jobId, state);
        client.execute(UpdateJobStateAction.INSTANCE, request, new ActionListener<UpdateJobStateAction.Response>() {
            @Override
            public void onResponse(UpdateJobStateAction.Response response) {
                if (response.isAcknowledged()) {
                    logger.info("Successfully set job state to [{}] for job [{}]", state, jobId);
                } else {
                    logger.info("Changing job state to [{}] for job [{}] wasn't acked", state, jobId);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Could not set job state to [" + state + "] for job [" + jobId +"]", e);
            }
        });
    }

    public void setJobState(String jobId, JobState state, Consumer<Void> handler, Consumer<Exception> errorHandler) {
        UpdateJobStateAction.Request request = new UpdateJobStateAction.Request(jobId, state);
        client.execute(UpdateJobStateAction.INSTANCE, request, ActionListener.wrap(r -> handler.accept(null), errorHandler));
    }

    public Optional<Tuple<DataCounts, ModelSizeStats>> getStatistics(String jobId) {
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            return Optional.empty();
        }
        return Optional.of(new Tuple<>(communicator.getDataCounts(), communicator.getModelSizeStats()));
    }
}
