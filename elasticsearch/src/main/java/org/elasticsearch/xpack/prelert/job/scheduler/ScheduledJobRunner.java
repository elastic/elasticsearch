/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.StartJobSchedulerAction;
import org.elasticsearch.xpack.prelert.action.UpdateJobSchedulerStatusAction;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.SchedulerStatus;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.audit.Auditor;
import org.elasticsearch.xpack.prelert.job.config.DefaultFrequency;
import org.elasticsearch.xpack.prelert.job.extraction.DataExtractor;
import org.elasticsearch.xpack.prelert.job.extraction.DataExtractorFactory;
import org.elasticsearch.xpack.prelert.job.metadata.Allocation;
import org.elasticsearch.xpack.prelert.job.metadata.PrelertMetadata;
import org.elasticsearch.xpack.prelert.job.persistence.BucketsQueryBuilder;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ScheduledJobRunner extends AbstractComponent {

    private final Client client;
    private final ClusterService clusterService;
    private final JobProvider jobProvider;
    private final DataExtractorFactory dataExtractorFactory;
    private final ThreadPool threadPool;
    private final Supplier<Long> currentTimeSupplier;

    public ScheduledJobRunner(ThreadPool threadPool, Client client, ClusterService clusterService, JobProvider jobProvider,
                              DataExtractorFactory dataExtractorFactory, Supplier<Long> currentTimeSupplier) {
        super(Settings.EMPTY);
        this.threadPool = threadPool;
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = Objects.requireNonNull(client);
        this.jobProvider = Objects.requireNonNull(jobProvider);
        this.dataExtractorFactory = Objects.requireNonNull(dataExtractorFactory);
        this.currentTimeSupplier = Objects.requireNonNull(currentTimeSupplier);
    }

    public void run(String jobId, long startTime, Long endTime, StartJobSchedulerAction.SchedulerTask task, Consumer<Exception> handler) {
        PrelertMetadata prelertMetadata = clusterService.state().metaData().custom(PrelertMetadata.TYPE);
        validate(jobId, prelertMetadata);

        setJobSchedulerStatus(jobId, SchedulerStatus.STARTED, error -> {
            logger.info("[{}] Starting scheduler", jobId);
            Job job = prelertMetadata.getJobs().get(jobId);
            Holder holder = createJobScheduler(job, task, handler);
            task.setHolder(holder);
            holder.future = threadPool.executor(PrelertPlugin.SCHEDULER_THREAD_POOL_NAME).submit(() -> {
                try {
                    Long next = holder.scheduledJob.runLookBack(startTime, endTime);
                    if (next != null) {
                        doScheduleRealtime(next, job.getId(), holder);
                    } else {
                        holder.stop();
                    }
                } catch (ScheduledJob.ExtractionProblemException e) {
                    holder.problemTracker.reportExtractionProblem(e.getCause().getMessage());
                } catch (ScheduledJob.AnalysisProblemException e) {
                    holder.problemTracker.reportAnalysisProblem(e.getCause().getMessage());
                } catch (ScheduledJob.EmptyDataCountException e) {
                    if (holder.problemTracker.updateEmptyDataCount(true)) {
                        holder.stop();
                    }
                } catch (Exception e) {
                    logger.error("Failed lookback import for job[" + job.getId() + "]", e);
                    holder.stop();
                }
                holder.problemTracker.finishReport();
            });
        });
    }

    private void doScheduleRealtime(long delayInMsSinceEpoch, String jobId, Holder holder) {
        if (holder.isRunning()) {
            TimeValue delay = computeNextDelay(delayInMsSinceEpoch);
            logger.debug("Waiting [{}] before executing next realtime import for job [{}]", delay, jobId);
            holder.future = threadPool.schedule(delay, PrelertPlugin.SCHEDULER_THREAD_POOL_NAME, () -> {
                long nextDelayInMsSinceEpoch;
                try {
                    nextDelayInMsSinceEpoch = holder.scheduledJob.runRealtime();
                } catch (ScheduledJob.ExtractionProblemException e) {
                    nextDelayInMsSinceEpoch = e.nextDelayInMsSinceEpoch;
                    holder.problemTracker.reportExtractionProblem(e.getCause().getMessage());
                } catch (ScheduledJob.AnalysisProblemException e) {
                    nextDelayInMsSinceEpoch = e.nextDelayInMsSinceEpoch;
                    holder.problemTracker.reportAnalysisProblem(e.getCause().getMessage());
                } catch (ScheduledJob.EmptyDataCountException e) {
                    nextDelayInMsSinceEpoch = e.nextDelayInMsSinceEpoch;
                    if (holder.problemTracker.updateEmptyDataCount(true)) {
                        holder.problemTracker.finishReport();
                        holder.stop();
                        return;
                    }
                } catch (Exception e) {
                    logger.error("Unexpected scheduler failure for job [" + jobId + "] stopping...", e);
                    holder.stop();
                    return;
                }
                holder.problemTracker.finishReport();
                doScheduleRealtime(nextDelayInMsSinceEpoch, jobId, holder);
            });
        } else {
            holder.stop();
        }
    }

    public static void validate(String jobId, PrelertMetadata prelertMetadata) {
        Job job = prelertMetadata.getJobs().get(jobId);
        if (job == null) {
            throw ExceptionsHelper.missingJobException(jobId);
        }

        if (job.getSchedulerConfig() == null) {
            throw new IllegalArgumentException("job [" + job.getId() + "] is not a scheduled job");
        }

        Allocation allocation = prelertMetadata.getAllocations().get(jobId);
        if (allocation.getStatus() != JobStatus.OPENED) {
            throw new ElasticsearchStatusException("cannot start scheduler, expected job status [{}], but got [{}]",
                    RestStatus.CONFLICT, JobStatus.OPENED, allocation.getStatus());
        }

        SchedulerStatus status = prelertMetadata.getSchedulerStatuses().get(jobId);
        if (status != SchedulerStatus.STOPPED) {
            throw new ElasticsearchStatusException("scheduler already started, expected scheduler status [{}], but got [{}]",
                    RestStatus.CONFLICT, SchedulerStatus.STOPPED, status);
        }

    }

    private Holder createJobScheduler(Job job, StartJobSchedulerAction.SchedulerTask task, Consumer<Exception> handler) {
        Auditor auditor = jobProvider.audit(job.getId());
        Duration frequency = getFrequencyOrDefault(job);
        Duration queryDelay = Duration.ofSeconds(job.getSchedulerConfig().getQueryDelay());
        DataExtractor dataExtractor = dataExtractorFactory.newExtractor(job);
        ScheduledJob scheduledJob =  new ScheduledJob(job.getId(), frequency.toMillis(), queryDelay.toMillis(),
                dataExtractor, client, auditor, currentTimeSupplier, getLatestFinalBucketEndTimeMs(job),
                getLatestRecordTimestamp(job.getId()));
        return new Holder(job, scheduledJob, new ProblemTracker(() -> auditor), handler);
    }

    private long getLatestFinalBucketEndTimeMs(Job job) {
        Duration bucketSpan = Duration.ofSeconds(job.getAnalysisConfig().getBucketSpan());
        long latestFinalBucketEndMs = -1L;
        BucketsQueryBuilder.BucketsQuery latestBucketQuery = new BucketsQueryBuilder()
                .sortField(Bucket.TIMESTAMP.getPreferredName())
                .sortDescending(true).size(1)
                .includeInterim(false)
                .build();
        QueryPage<Bucket> buckets;
        try {
            buckets = jobProvider.buckets(job.getId(), latestBucketQuery);
            if (buckets.results().size() == 1) {
                latestFinalBucketEndMs = buckets.results().get(0).getTimestamp().getTime() + bucketSpan.toMillis() - 1;
            }
        } catch (ResourceNotFoundException e) {
            logger.error("Could not retrieve latest bucket timestamp", e);
        }
        return latestFinalBucketEndMs;
    }

    private long getLatestRecordTimestamp(String jobId) {
        long latestRecordTimeMs = -1L;
        DataCounts dataCounts = jobProvider.dataCounts(jobId);
        if (dataCounts.getLatestRecordTimeStamp() != null) {
            latestRecordTimeMs = dataCounts.getLatestRecordTimeStamp().getTime();
        }
        return latestRecordTimeMs;
    }

    private static Duration getFrequencyOrDefault(Job job) {
        Long frequency = job.getSchedulerConfig().getFrequency();
        Long bucketSpan = job.getAnalysisConfig().getBucketSpan();
        return frequency == null ? DefaultFrequency.ofBucketSpan(bucketSpan) : Duration.ofSeconds(frequency);
    }

    private TimeValue computeNextDelay(long next) {
        return new TimeValue(Math.max(1, next - currentTimeSupplier.get()));
    }

    private void setJobSchedulerStatus(String jobId, SchedulerStatus status, Consumer<Exception> supplier) {
        UpdateJobSchedulerStatusAction.Request request = new UpdateJobSchedulerStatusAction.Request(jobId, status);
        client.execute(UpdateJobSchedulerStatusAction.INSTANCE, request, new ActionListener<UpdateJobSchedulerStatusAction.Response>() {
            @Override
            public void onResponse(UpdateJobSchedulerStatusAction.Response response) {
                if (response.isAcknowledged()) {
                    logger.debug("successfully set job scheduler status to [{}] for job [{}]", status, jobId);
                } else {
                    logger.info("set job scheduler status to [{}] for job [{}], but was not acknowledged", status, jobId);
                }
                supplier.accept(null);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("could not set job scheduler status to [" + status + "] for job [" + jobId +"]", e);
                supplier.accept(e);
            }
        });
    }

    public class Holder {

        private final String jobId;
        private final ScheduledJob scheduledJob;
        private final ProblemTracker problemTracker;
        private final Consumer<Exception> handler;
        volatile Future<?> future;

        private Holder(Job job, ScheduledJob scheduledJob, ProblemTracker problemTracker, Consumer<Exception> handler) {
            this.jobId = job.getId();
            this.scheduledJob = scheduledJob;
            this.problemTracker = problemTracker;
            this.handler = handler;
        }

        boolean isRunning() {
            return scheduledJob.isRunning();
        }

        public void stop() {
            logger.info("Stopping scheduler for job [{}]", jobId);
            scheduledJob.stop();
            FutureUtils.cancel(future);
            setJobSchedulerStatus(jobId, SchedulerStatus.STOPPED, error -> handler.accept(null));
        }

    }
}
