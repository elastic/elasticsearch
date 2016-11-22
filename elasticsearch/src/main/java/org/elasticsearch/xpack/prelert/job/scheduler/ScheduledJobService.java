/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.UpdateJobSchedulerStatusAction;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobSchedulerStatus;
import org.elasticsearch.xpack.prelert.job.SchedulerState;
import org.elasticsearch.xpack.prelert.job.audit.Auditor;
import org.elasticsearch.xpack.prelert.job.config.DefaultFrequency;
import org.elasticsearch.xpack.prelert.job.data.DataProcessor;
import org.elasticsearch.xpack.prelert.job.extraction.DataExtractor;
import org.elasticsearch.xpack.prelert.job.extraction.DataExtractorFactory;
import org.elasticsearch.xpack.prelert.job.metadata.Allocation;
import org.elasticsearch.xpack.prelert.job.persistence.BucketsQueryBuilder;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.elasticsearch.xpack.prelert.job.results.Bucket;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class ScheduledJobService extends AbstractComponent {

    private final Client client;
    private final JobProvider jobProvider;
    private final DataProcessor dataProcessor;
    private final DataExtractorFactory dataExtractorFactory;
    private final ThreadPool threadPool;
    private final Supplier<Long> currentTimeSupplier;
    final ConcurrentMap<String, Holder> registry;

    public ScheduledJobService(ThreadPool threadPool, Client client, JobProvider jobProvider, DataProcessor dataProcessor,
                               DataExtractorFactory dataExtractorFactory, Supplier<Long> currentTimeSupplier) {
        super(Settings.EMPTY);
        this.threadPool = threadPool;
        this.client = Objects.requireNonNull(client);
        this.jobProvider = Objects.requireNonNull(jobProvider);
        this.dataProcessor = Objects.requireNonNull(dataProcessor);
        this.dataExtractorFactory = Objects.requireNonNull(dataExtractorFactory);
        this.currentTimeSupplier = Objects.requireNonNull(currentTimeSupplier);
        this.registry = ConcurrentCollections.newConcurrentMap();
    }

    public void start(Job job, Allocation allocation) {
        SchedulerState schedulerState = allocation.getSchedulerState();
        if (schedulerState == null) {
            throw new IllegalStateException("Job [" + job.getId() + "] is not a scheduled job");
        }

        if (schedulerState.getStatus() != JobSchedulerStatus.STARTING) {
            throw new IllegalStateException("expected job scheduler status [" + JobSchedulerStatus.STARTING + "], but got [" +
                    schedulerState.getStatus()  + "] instead");
        }

        if (registry.containsKey(allocation.getJobId())) {
            throw new IllegalStateException("job [" + allocation.getJobId() + "] has already been started");
        }

        logger.info("Starting scheduler [{}]", allocation);
        Holder holder = createJobScheduler(job);
        registry.put(job.getId(), holder);

        threadPool.executor(PrelertPlugin.SCHEDULER_THREAD_POOL_NAME).execute(() -> {
            try {
                Long next = holder.scheduledJob.runLookBack(allocation.getSchedulerState());
                if (next != null) {
                    doScheduleRealtime(next, job.getId(), holder);
                } else {
                    holder.scheduledJob.stop();
                    requestStopping(job.getId());
                }
            } catch (ScheduledJob.ExtractionProblemException e) {
                holder.problemTracker.reportExtractionProblem(e.getCause().getMessage());
            } catch (ScheduledJob.AnalysisProblemException e) {
                holder.problemTracker.reportAnalysisProblem(e.getCause().getMessage());
            } catch (ScheduledJob.EmptyDataCountException e) {
                if (holder.problemTracker.updateEmptyDataCount(true)) {
                    requestStopping(job.getJobId());
                }
            } catch (Exception e) {
                logger.error("Failed lookback import for job[" + job.getId() + "]", e);
                requestStopping(job.getId());
            }
            holder.problemTracker.finishReport();
        });
        setJobSchedulerStatus(job.getId(), JobSchedulerStatus.STARTED);
    }

    public void stop(Allocation allocation) {
        SchedulerState schedulerState = allocation.getSchedulerState();
        if (schedulerState == null) {
            throw new IllegalStateException("Job [" + allocation.getJobId() + "] is not a scheduled job");
        }
        if (schedulerState.getStatus() != JobSchedulerStatus.STOPPING) {
            throw new IllegalStateException("expected job scheduler status [" + JobSchedulerStatus.STOPPING + "], but got [" +
                    schedulerState.getStatus()  + "] instead");
        }

        if (registry.containsKey(allocation.getJobId()) == false) {
            throw new IllegalStateException("job [" + allocation.getJobId() + "] has not been started");
        }

        logger.info("Stopping scheduler for job [{}]", allocation.getJobId());
        Holder holder = registry.remove(allocation.getJobId());
        holder.scheduledJob.stop();
        dataProcessor.closeJob(allocation.getJobId());
        setJobSchedulerStatus(allocation.getJobId(), JobSchedulerStatus.STOPPED);
    }

    public void stopAllJobs() {
        for (Map.Entry<String, Holder> entry : registry.entrySet()) {
            entry.getValue().scheduledJob.stop();
            dataProcessor.closeJob(entry.getKey());
        }
        registry.clear();
    }

    private void doScheduleRealtime(long delayInMsSinceEpoch, String jobId, Holder holder) {
        if (holder.scheduledJob.isRunning()) {
            TimeValue delay = computeNextDelay(delayInMsSinceEpoch);
            logger.debug("Waiting [{}] before executing next realtime import for job [{}]", delay, jobId);
            threadPool.schedule(delay, PrelertPlugin.SCHEDULER_THREAD_POOL_NAME, () -> {
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
                        requestStopping(jobId);
                        return;
                    }
                } catch (Exception e) {
                    logger.error("Unexpected scheduler failure for job [" + jobId + "] stopping...", e);
                    requestStopping(jobId);
                    return;
                }
                holder.problemTracker.finishReport();
                doScheduleRealtime(nextDelayInMsSinceEpoch, jobId, holder);
            });
        } else {
            requestStopping(jobId);
        }
    }

    private void requestStopping(String jobId) {
        setJobSchedulerStatus(jobId, JobSchedulerStatus.STOPPING);
    }

    Holder createJobScheduler(Job job) {
        Auditor auditor = jobProvider.audit(job.getJobId());
        Duration frequency = getFrequencyOrDefault(job);
        Duration queryDelay = Duration.ofSeconds(job.getSchedulerConfig().getQueryDelay());
        DataExtractor dataExtractor = dataExtractorFactory.newExtractor(job);
        ScheduledJob scheduledJob =  new ScheduledJob(job.getJobId(), frequency.toMillis(), queryDelay.toMillis(),
                dataExtractor, dataProcessor, auditor, currentTimeSupplier, getLatestFinalBucketEndTimeMs(job),
                getLatestRecordTimestamp(job.getJobId()));
        return new Holder(scheduledJob, new ProblemTracker(() -> auditor));
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

    private void setJobSchedulerStatus(String jobId, JobSchedulerStatus status) {
        UpdateJobSchedulerStatusAction.Request request = new UpdateJobSchedulerStatusAction.Request(jobId, status);
        client.execute(UpdateJobSchedulerStatusAction.INSTANCE, request, new ActionListener<UpdateJobSchedulerStatusAction.Response>() {
            @Override
            public void onResponse(UpdateJobSchedulerStatusAction.Response response) {
                if (response.isAcknowledged()) {
                    logger.debug("successfully set job scheduler status to [{}] for job [{}]", status, jobId);
                } else {
                    logger.info("set job scheduler status to [{}] for job [{}], but was not acknowledged", status, jobId);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("could not set job scheduler status to [" + status + "] for job [" + jobId +"]", e);
            }
        });
    }

    private static class Holder {

        private final ScheduledJob scheduledJob;
        private final ProblemTracker problemTracker;

        private Holder(ScheduledJob scheduledJob, ProblemTracker problemTracker) {
            this.scheduledJob = scheduledJob;
            this.problemTracker = problemTracker;
        }
    }
}
