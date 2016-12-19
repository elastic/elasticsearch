/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.UpdateJobStatusAction;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.data.DataProcessor;
import org.elasticsearch.xpack.prelert.job.metadata.Allocation;
import org.elasticsearch.xpack.prelert.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.prelert.job.persistence.JobRenormalizedResultsPersister;
import org.elasticsearch.xpack.prelert.job.persistence.UsagePersister;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.prelert.job.process.autodetect.AutodetectCommunicator;
import org.elasticsearch.xpack.prelert.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.prelert.job.process.autodetect.AutodetectProcessFactory;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.AutoDetectResultProcessor;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.AutodetectResultsParser;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.StateProcessor;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.prelert.job.process.normalizer.NormalizerFactory;
import org.elasticsearch.xpack.prelert.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.prelert.job.process.normalizer.ScoresUpdater;
import org.elasticsearch.xpack.prelert.job.process.normalizer.ShortCircuitingRenormalizer;
import org.elasticsearch.xpack.prelert.job.status.StatusReporter;
import org.elasticsearch.xpack.prelert.job.usage.UsageReporter;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class AutodetectProcessManager extends AbstractComponent implements DataProcessor {

    // TODO (norelease) default needs to be reconsidered
    public static final Setting<Integer> MAX_RUNNING_JOBS_PER_NODE =
            Setting.intSetting("max_running_jobs", 10, 1, 128, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private final Client client;
    private final ThreadPool threadPool;
    private final JobManager jobManager;
    private final JobProvider jobProvider;
    private final AutodetectResultsParser parser;
    private final AutodetectProcessFactory autodetectProcessFactory;
    private final NormalizerFactory normalizerFactory;

    private final UsagePersister usagePersister;
    private final StateProcessor stateProcessor;
    private final JobResultsPersister jobResultsPersister;
    private final JobRenormalizedResultsPersister jobRenormalizedResultsPersister;
    private final JobDataCountsPersister jobDataCountsPersister;

    private final ConcurrentMap<String, AutodetectCommunicator> autoDetectCommunicatorByJob;

    private volatile int maxAllowedRunningJobs;

    public AutodetectProcessManager(Settings settings, Client client, ThreadPool threadPool, JobManager jobManager,
                                    JobProvider jobProvider, JobResultsPersister jobResultsPersister,
                                    JobRenormalizedResultsPersister jobRenormalizedResultsPersister,
                                    JobDataCountsPersister jobDataCountsPersister, AutodetectResultsParser parser,
                                    AutodetectProcessFactory autodetectProcessFactory, NormalizerFactory normalizerFactory,
                                    ClusterSettings clusterSettings) {
        super(settings);
        this.client = client;
        this.threadPool = threadPool;
        this.maxAllowedRunningJobs = MAX_RUNNING_JOBS_PER_NODE.get(settings);
        this.parser = parser;
        this.autodetectProcessFactory = autodetectProcessFactory;
        this.normalizerFactory = normalizerFactory;
        this.jobManager = jobManager;
        this.jobProvider = jobProvider;

        this.jobResultsPersister = jobResultsPersister;
        this.jobRenormalizedResultsPersister = jobRenormalizedResultsPersister;
        this.stateProcessor = new StateProcessor(settings, jobResultsPersister);
        this.usagePersister = new UsagePersister(settings, client);
        this.jobDataCountsPersister = jobDataCountsPersister;

        this.autoDetectCommunicatorByJob = new ConcurrentHashMap<>();
        clusterSettings.addSettingsUpdateConsumer(MAX_RUNNING_JOBS_PER_NODE, val -> maxAllowedRunningJobs = val);
    }

    @Override
    public DataCounts processData(String jobId, InputStream input, DataLoadParams params, Supplier<Boolean> cancelled) {
        Allocation allocation = jobManager.getJobAllocation(jobId);
        if (allocation.getStatus() != JobStatus.OPENED) {
            throw new IllegalArgumentException("job [" + jobId + "] status is [" + allocation.getStatus() + "], but must be ["
                    + JobStatus.OPENED + "] for processing data");
        }

        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            throw new IllegalStateException("job [" +  jobId + "] with status [" + allocation.getStatus() + "] hasn't been started");
        }
        try {
            return communicator.writeToJob(input, params, cancelled);
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

    @Override
    public void flushJob(String jobId, InterimResultsParams params) {
        logger.debug("Flushing job {}", jobId);
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            logger.debug("Cannot flush: no active autodetect process for job {}", jobId);
            return;
        }
        try {
            communicator.flushJob(params);
            // TODO check for errors from autodetect
        } catch (IOException ioe) {
            String msg = String.format(Locale.ROOT, "[%s] exception while flushing job", jobId);
            logger.warn(msg);
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

    @Override
    public void openJob(String jobId, boolean ignoreDowntime) {
        autoDetectCommunicatorByJob.computeIfAbsent(jobId, id -> {
            AutodetectCommunicator communicator = create(id, ignoreDowntime);
            setJobStatus(jobId, JobStatus.OPENED);
            return communicator;
        });
    }

    AutodetectCommunicator create(String jobId, boolean ignoreDowntime) {
        if (autoDetectCommunicatorByJob.size() == maxAllowedRunningJobs) {
            throw new ElasticsearchStatusException("max running job capacity [" + maxAllowedRunningJobs + "] reached",
                    RestStatus.CONFLICT);
        }

        // TODO norelease, once we remove black hole process
        // then we can  remove this method and move not enough threads logic to the auto detect process factory
        Job job = jobManager.getJobOrThrowIfUnknown(jobId);
        // A TP with no queue, so that we fail immediately if there are no threads available
        ExecutorService executorService = threadPool.executor(PrelertPlugin.AUTODETECT_PROCESS_THREAD_POOL_NAME);

        UsageReporter usageReporter = new UsageReporter(settings, job.getId(), usagePersister);
        try (StatusReporter statusReporter = new StatusReporter(threadPool, settings, job.getId(), jobProvider.dataCounts(jobId),
                usageReporter, jobDataCountsPersister)) {
            ScoresUpdater scoresUpdator = new ScoresUpdater(job, jobProvider, jobRenormalizedResultsPersister, normalizerFactory);
            Renormalizer renormalizer = new ShortCircuitingRenormalizer(jobId, scoresUpdator,
                    threadPool.executor(PrelertPlugin.THREAD_POOL_NAME), job.getAnalysisConfig().getUsePerPartitionNormalization());
            AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormalizer, jobResultsPersister, parser);

            AutodetectProcess process = null;
            try {
                process = autodetectProcessFactory.createAutodetectProcess(job, ignoreDowntime, executorService);
                return new AutodetectCommunicator(executorService, job, process, statusReporter, processor, stateProcessor);
            } catch (Exception e) {
                try {
                    IOUtils.close(process);
                } catch (IOException ioe) {
                    logger.error("Can't close autodetect", ioe);
                }
                throw e;
            }
        }
    }

    @Override
    public void closeJob(String jobId) {
        logger.debug("Closing job {}", jobId);
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.remove(jobId);
        if (communicator == null) {
            logger.debug("Cannot close: no active autodetect process for job {}", jobId);
            return;
        }

        try {
            communicator.close();
            setJobStatus(jobId, JobStatus.CLOSED);
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

    private void setJobStatus(String jobId, JobStatus status) {
        UpdateJobStatusAction.Request request = new UpdateJobStatusAction.Request(jobId, status);
        client.execute(UpdateJobStatusAction.INSTANCE, request, new ActionListener<UpdateJobStatusAction.Response>() {
            @Override
            public void onResponse(UpdateJobStatusAction.Response response) {
                logger.info("Successfully set job status to [{}] for job [{}]", status, jobId);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Could not set job status to [" + status + "] for job [" + jobId +"]", e);
            }
        });
    }

    public Optional<ModelSizeStats> getModelSizeStats(String jobId) {
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            return Optional.empty();
        }

        return communicator.getModelSizeStats();
    }

    public Optional<DataCounts> getDataCounts(String jobId) {
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            return Optional.empty();
        }

        return communicator.getDataCounts();
    }
}
