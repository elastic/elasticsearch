/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.manager;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.data.DataProcessor;
import org.elasticsearch.xpack.prelert.job.metadata.Allocation;
import org.elasticsearch.xpack.prelert.job.persistence.ElasticsearchJobDataCountsPersister;
import org.elasticsearch.xpack.prelert.job.persistence.ElasticsearchPersister;
import org.elasticsearch.xpack.prelert.job.persistence.ElasticsearchUsagePersister;
import org.elasticsearch.xpack.prelert.job.persistence.JobDataCountsPersister;
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
import org.elasticsearch.xpack.prelert.job.process.normalizer.noop.NoOpRenormaliser;
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

public class AutodetectProcessManager extends AbstractComponent implements DataProcessor {

    // TODO (norelease) to be reconsidered
    public static final Setting<Integer> MAX_RUNNING_JOBS_PER_NODE =
            Setting.intSetting("max_running_jobs", 10, Setting.Property.NodeScope);

    private final Client client;
    private final Environment env;
    private final int maxRunningJobs;
    private final ThreadPool threadPool;
    private final JobManager jobManager;
    private final JobProvider jobProvider;
    private final AutodetectResultsParser parser;
    private final AutodetectProcessFactory autodetectProcessFactory;

    private final ConcurrentMap<String, AutodetectCommunicator> autoDetectCommunicatorByJob;

    public AutodetectProcessManager(Settings settings, Client client, Environment env, ThreadPool threadPool, JobManager jobManager,
                                    JobProvider jobProvider, AutodetectResultsParser parser,
                                    AutodetectProcessFactory autodetectProcessFactory) {
        super(settings);
        this.client = client;
        this.env = env;
        this.threadPool = threadPool;
        this.maxRunningJobs = MAX_RUNNING_JOBS_PER_NODE.get(settings);
        this.parser = parser;
        this.autodetectProcessFactory = autodetectProcessFactory;
        this.jobManager = jobManager;
        this.jobProvider = jobProvider;
        this.autoDetectCommunicatorByJob = new ConcurrentHashMap<>();
    }

    @Override
    public DataCounts processData(String jobId, InputStream input, DataLoadParams params) {
        Allocation allocation = jobManager.getJobAllocation(jobId);
        if (allocation.getStatus().isAnyOf(JobStatus.PAUSING, JobStatus.PAUSED)) {
            return new DataCounts(jobId);
        }

        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.computeIfAbsent(jobId, id -> {
            return create(id, params.isIgnoreDowntime());
        });
        try {
            return communicator.writeToJob(input, params);
            // TODO check for errors from autodetect
        } catch (IOException e) {
            String msg = String.format(Locale.ROOT, "Exception writing to process for job %s", jobId);
            if (e.getCause() instanceof TimeoutException) {
                logger.warn("Connection to process was dropped due to a timeout - if you are feeding this job from a connector it " +
                        "may be that your connector stalled for too long", e.getCause());
            }
            throw ExceptionsHelper.serverError(msg);
        }
    }

    AutodetectCommunicator create(String jobId, boolean ignoreDowntime) {
        if (autoDetectCommunicatorByJob.size() == maxRunningJobs) {
            throw new ElasticsearchStatusException("max running job capacity [" + maxRunningJobs + "] reached",
                    RestStatus.TOO_MANY_REQUESTS);
        }

        // TODO norelease, once we remove black hole process and all persisters are singletons then we can
        // remove this method and move not enough threads logic to the auto detect process factory
        Job job = jobManager.getJobOrThrowIfUnknown(jobId);
        Logger jobLogger = Loggers.getLogger(job.getJobId());
        // A TP with no queue, so that we fail immediately if there are no threads available
        ExecutorService executorService = threadPool.executor(PrelertPlugin.AUTODETECT_PROCESS_THREAD_POOL_NAME);

        ElasticsearchUsagePersister usagePersister = new ElasticsearchUsagePersister(client, jobLogger);
        UsageReporter usageReporter = new UsageReporter(settings, job.getJobId(), usagePersister, jobLogger);
        JobDataCountsPersister jobDataCountsPersister = new ElasticsearchJobDataCountsPersister(client);
        JobResultsPersister persister = new ElasticsearchPersister(jobId, client);
        StatusReporter statusReporter = new StatusReporter(env, settings, job.getJobId(), jobProvider.dataCounts(jobId),
                usageReporter, jobDataCountsPersister, jobLogger, job.getAnalysisConfig().getBucketSpanOrDefault());
        AutoDetectResultProcessor processor =  new AutoDetectResultProcessor(new NoOpRenormaliser(), persister, parser);
        StateProcessor stateProcessor = new StateProcessor(settings, persister);

        AutodetectProcess process = null;
        try {
            process = autodetectProcessFactory.createAutodetectProcess(job, ignoreDowntime, executorService);
            // TODO Port the normalizer from the old project
            return new AutodetectCommunicator(executorService, job, process, jobLogger, statusReporter, processor, stateProcessor);
        } catch (Exception e) {
            try {
                IOUtils.close(process);
            } catch (IOException ioe) {
                logger.error("Can't close autodetect", ioe);
            }
            throw e;
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
            String msg = String.format(Locale.ROOT, "Exception flushing process for job %s", jobId);
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
    public void closeJob(String jobId) {
        logger.debug("Closing job {}", jobId);
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.remove(jobId);
        if (communicator == null) {
            logger.debug("Cannot close: no active autodetect process for job {}", jobId);
            return;
        }

        try {
            communicator.close();
            setJobFinishedTimeAndStatus(jobId, JobStatus.CLOSED);
            // TODO check for errors from autodetect
            // TODO delete associated files (model config etc)
        } catch (Exception e) {
            logger.warn("Exception closing stopped process input stream", e);
            throw ExceptionsHelper.serverError("Exception closing stopped process input stream", e);
        }
    }

    public int numberOfRunningJobs() {
        return autoDetectCommunicatorByJob.size();
    }

    public boolean jobHasActiveAutodetectProcess(String jobId) {
        return autoDetectCommunicatorByJob.get(jobId) != null;
    }

    public Duration jobUpTime(String jobId) {
        AutodetectCommunicator communicator = autoDetectCommunicatorByJob.get(jobId);
        if (communicator == null) {
            return Duration.ZERO;
        }
        return Duration.between(communicator.getProcessStartTime(), ZonedDateTime.now());
    }

    private void setJobFinishedTimeAndStatus(String jobId, JobStatus status) {
        // NORELEASE Implement this.
        // Perhaps move the JobStatus and finish time to a separate document stored outside the cluster state
        logger.error("Cannot set finished job status and time- Not Implemented");
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
