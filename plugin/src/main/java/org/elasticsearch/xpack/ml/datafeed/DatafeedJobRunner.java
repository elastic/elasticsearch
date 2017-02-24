/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.CloseJobAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.chunked.ChunkedDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.scroll.ScrollDataExtractorFactory;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.DefaultFrequency;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.persistence.BucketsQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.Result;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.persistent.UpdatePersistentTaskStatusAction;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class DatafeedJobRunner extends AbstractComponent {

    private static final String INF_SYMBOL = "\u221E";

    private final Client client;
    private final ClusterService clusterService;
    private final JobProvider jobProvider;
    private final ThreadPool threadPool;
    private final Supplier<Long> currentTimeSupplier;
    private final Auditor auditor;

    public DatafeedJobRunner(ThreadPool threadPool, Client client, ClusterService clusterService, JobProvider jobProvider,
                              Supplier<Long> currentTimeSupplier, Auditor auditor) {
        super(Settings.EMPTY);
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.jobProvider = Objects.requireNonNull(jobProvider);
        this.threadPool = threadPool;
        this.currentTimeSupplier = Objects.requireNonNull(currentTimeSupplier);
        this.auditor = auditor;
    }

    public void run(StartDatafeedAction.DatafeedTask task, Consumer<Exception> handler) {
        String datafeedId = task.getDatafeedId();
        ClusterState state = clusterService.state();
        // CS on master node can be ahead on the node where job and datafeed tasks run,
        // so check again and fail if in case of unexpected cs. Persist tasks will retry later then.
        if (StartDatafeedAction.selectNode(logger, datafeedId, state) == null) {
            handler.accept(new ElasticsearchStatusException("Local cs [{}] isn't ready to start datafeed [{}] yet",
                    RestStatus.CONFLICT, state.getVersion(), datafeedId));
            return;
        }
        logger.info("Attempt to start datafeed based on cluster state version [{}]", state.getVersion());
        MlMetadata mlMetadata = state.metaData().custom(MlMetadata.TYPE);
        DatafeedConfig datafeed = mlMetadata.getDatafeed(datafeedId);
        Job job = mlMetadata.getJobs().get(datafeed.getJobId());
        gatherInformation(job.getId(), (buckets, dataCounts) -> {
            long latestFinalBucketEndMs = -1L;
            Duration bucketSpan = Duration.ofSeconds(job.getAnalysisConfig().getBucketSpan());
            if (buckets.results().size() == 1) {
                latestFinalBucketEndMs = buckets.results().get(0).getTimestamp().getTime() + bucketSpan.toMillis() - 1;
            }
            long latestRecordTimeMs = -1L;
            if (dataCounts.getLatestRecordTimeStamp() != null) {
                latestRecordTimeMs = dataCounts.getLatestRecordTimeStamp().getTime();
            }
            Holder holder = createJobDatafeed(datafeed, job, latestFinalBucketEndMs, latestRecordTimeMs, handler, task);
            updateDatafeedState(task.getPersistentTaskId(), DatafeedState.STARTED, e -> {
                if (e != null) {
                    handler.accept(e);
                } else {
                    innerRun(holder, task.getStartTime(), task.getEndTime());
                }
            });
        }, handler);
    }

    // Important: Holder must be created and assigned to DatafeedTask before setting state to started,
    // otherwise if a stop datafeed call is made immediately after the start datafeed call we could cancel
    // the DatafeedTask without stopping datafeed, which causes the datafeed to keep on running.
    private void innerRun(Holder holder, long startTime, Long endTime) {
        logger.info("Starting datafeed [{}] for job [{}] in [{}, {})", holder.datafeed.getId(), holder.datafeed.getJobId(),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.printer().print(startTime),
                endTime == null ? INF_SYMBOL : DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.printer().print(endTime));
        holder.future = threadPool.executor(MachineLearning.DATAFEED_RUNNER_THREAD_POOL_NAME).submit(() -> {
            Long next = null;
            try {
                next = holder.datafeedJob.runLookBack(startTime, endTime);
            } catch (DatafeedJob.ExtractionProblemException e) {
                if (endTime == null) {
                    next = e.nextDelayInMsSinceEpoch;
                }
                holder.problemTracker.reportExtractionProblem(e.getCause().getMessage());
            } catch (DatafeedJob.AnalysisProblemException e) {
                if (endTime == null) {
                    next = e.nextDelayInMsSinceEpoch;
                }
                holder.problemTracker.reportAnalysisProblem(e.getCause().getMessage());
            } catch (DatafeedJob.EmptyDataCountException e) {
                if (endTime == null) {
                    holder.problemTracker.reportEmptyDataCount();
                    next = e.nextDelayInMsSinceEpoch;
                }
            } catch (Exception e) {
                logger.error("Failed lookback import for job [" + holder.datafeed.getJobId() + "]", e);
                holder.stop("general_lookback_failure", e);
                return;
            }
            if (next != null) {
                doDatafeedRealtime(next, holder.datafeed.getJobId(), holder);
            } else {
                holder.stop("no_realtime", null);
                holder.problemTracker.finishReport();
            }
        });
    }

    void doDatafeedRealtime(long delayInMsSinceEpoch, String jobId, Holder holder) {
        if (holder.isRunning()) {
            TimeValue delay = computeNextDelay(delayInMsSinceEpoch);
            logger.debug("Waiting [{}] before executing next realtime import for job [{}]", delay, jobId);
            holder.future = threadPool.schedule(delay, MachineLearning.DATAFEED_RUNNER_THREAD_POOL_NAME, () -> {
                long nextDelayInMsSinceEpoch;
                try {
                    nextDelayInMsSinceEpoch = holder.datafeedJob.runRealtime();
                    holder.problemTracker.reportNoneEmptyCount();
                } catch (DatafeedJob.ExtractionProblemException e) {
                    nextDelayInMsSinceEpoch = e.nextDelayInMsSinceEpoch;
                    holder.problemTracker.reportExtractionProblem(e.getCause().getMessage());
                } catch (DatafeedJob.AnalysisProblemException e) {
                    nextDelayInMsSinceEpoch = e.nextDelayInMsSinceEpoch;
                    holder.problemTracker.reportAnalysisProblem(e.getCause().getMessage());
                } catch (DatafeedJob.EmptyDataCountException e) {
                    nextDelayInMsSinceEpoch = e.nextDelayInMsSinceEpoch;
                    holder.problemTracker.reportEmptyDataCount();
                } catch (Exception e) {
                    logger.error("Unexpected datafeed failure for job [" + jobId + "] stopping...", e);
                    holder.stop("general_realtime_error", e);
                    return;
                }
                holder.problemTracker.finishReport();
                doDatafeedRealtime(nextDelayInMsSinceEpoch, jobId, holder);
            });
        }
    }

    Holder createJobDatafeed(DatafeedConfig datafeed, Job job, long finalBucketEndMs, long latestRecordTimeMs,
                                      Consumer<Exception> handler, StartDatafeedAction.DatafeedTask task) {
        Duration frequency = getFrequencyOrDefault(datafeed, job);
        Duration queryDelay = Duration.ofSeconds(datafeed.getQueryDelay());
        DataExtractorFactory dataExtractorFactory = createDataExtractorFactory(datafeed, job);
        DatafeedJob datafeedJob =  new DatafeedJob(job.getId(), buildDataDescription(job), frequency.toMillis(), queryDelay.toMillis(),
                dataExtractorFactory, client, auditor, currentTimeSupplier, finalBucketEndMs, latestRecordTimeMs);
        Holder holder = new Holder(datafeed, datafeedJob, task.isLookbackOnly(), new ProblemTracker(auditor, job.getId()), handler);
        task.setHolder(holder);
        return holder;
    }

    DataExtractorFactory createDataExtractorFactory(DatafeedConfig datafeedConfig, Job job) {
        boolean isScrollSearch = datafeedConfig.hasAggregations() == false;
        DataExtractorFactory dataExtractorFactory = isScrollSearch ? new ScrollDataExtractorFactory(client, datafeedConfig, job)
                : new AggregationDataExtractorFactory(client, datafeedConfig, job);
        ChunkingConfig chunkingConfig = datafeedConfig.getChunkingConfig();
        if (chunkingConfig == null) {
            chunkingConfig = isScrollSearch ? ChunkingConfig.newAuto() : ChunkingConfig.newOff();
        }

        return chunkingConfig.isEnabled() ? new ChunkedDataExtractorFactory(client, datafeedConfig, job, dataExtractorFactory)
                : dataExtractorFactory;
    }

    private static DataDescription buildDataDescription(Job job) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.JSON);
        if (job.getDataDescription() != null) {
            dataDescription.setTimeField(job.getDataDescription().getTimeField());
        }
        dataDescription.setTimeFormat(DataDescription.EPOCH_MS);
        return dataDescription.build();
    }

    private void gatherInformation(String jobId, BiConsumer<QueryPage<Bucket>, DataCounts> handler, Consumer<Exception> errorHandler) {
        BucketsQueryBuilder.BucketsQuery latestBucketQuery = new BucketsQueryBuilder()
                .sortField(Result.TIMESTAMP.getPreferredName())
                .sortDescending(true).size(1)
                .includeInterim(false)
                .build();
        jobProvider.buckets(jobId, latestBucketQuery, buckets -> {
            jobProvider.dataCounts(jobId, dataCounts -> handler.accept(buckets, dataCounts), errorHandler);
        }, e -> {
            if (e instanceof ResourceNotFoundException) {
                QueryPage<Bucket> empty = new QueryPage<>(Collections.emptyList(), 0, Bucket.RESULT_TYPE_FIELD);
                jobProvider.dataCounts(jobId, dataCounts -> handler.accept(empty, dataCounts), errorHandler);
            } else {
                errorHandler.accept(e);
            }
        });
    }

    private void updateDatafeedState(long persistentTaskId, DatafeedState datafeedState, Consumer<Exception> handler) {
        UpdatePersistentTaskStatusAction.Request request = new UpdatePersistentTaskStatusAction.Request(persistentTaskId, datafeedState);
        client.execute(UpdatePersistentTaskStatusAction.INSTANCE, request, ActionListener.wrap(r -> {
            handler.accept(null);
        }, handler));
    }

    private static Duration getFrequencyOrDefault(DatafeedConfig datafeed, Job job) {
        Long frequency = datafeed.getFrequency();
        Long bucketSpan = job.getAnalysisConfig().getBucketSpan();
        return frequency == null ? DefaultFrequency.ofBucketSpan(bucketSpan) : Duration.ofSeconds(frequency);
    }

    private TimeValue computeNextDelay(long next) {
        return new TimeValue(Math.max(1, next - currentTimeSupplier.get()));
    }

    public class Holder {

        private final DatafeedConfig datafeed;
        private final DatafeedJob datafeedJob;
        private final boolean autoCloseJob;
        private final ProblemTracker problemTracker;
        private final Consumer<Exception> handler;
        volatile Future<?> future;

        Holder(DatafeedConfig datafeed, DatafeedJob datafeedJob, boolean autoCloseJob, ProblemTracker problemTracker,
                       Consumer<Exception> handler) {
            this.datafeed = datafeed;
            this.datafeedJob = datafeedJob;
            this.autoCloseJob = autoCloseJob;
            this.problemTracker = problemTracker;
            this.handler = handler;
        }

        boolean isRunning() {
            return datafeedJob.isRunning();
        }

        public void stop(String source, Exception e) {
            logger.info("[{}] attempt to stop datafeed [{}] for job [{}]", source, datafeed.getId(), datafeed.getJobId());
            // We need to fork, because:
            // 1) We are being called from cluster state update thread and we should return as soon as possible
            // 2) We also index into the notifaction index and that is forbidden from the cluster state update thread:
            //    (Caused by: java.lang.AssertionError: should not be called by a cluster state applier. reason [the applied
            //     cluster state is not yet available])
            threadPool.executor(ThreadPool.Names.GENERIC).submit(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to stop [{}] datafeed [{}] for job [{}]", source, datafeed.getId(), datafeed.getJobId());
                    handler.accept(e);
                }

                @Override
                protected void doRun() throws Exception {
                    if (datafeedJob.stop()) {
                        FutureUtils.cancel(future);
                        handler.accept(e);
                        auditor.info(datafeed.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_STOPPED));
                        logger.info("[{}] datafeed [{}] for job [{}] has been stopped", source, datafeed.getId(), datafeed.getJobId());
                        if (autoCloseJob) {
                            closeJob();
                        }
                    } else {
                        logger.info("[{}] datafeed [{}] for job [{}] was already stopped", source, datafeed.getId(), datafeed.getJobId());
                    }
                }
            });
        }

        private void closeJob() {
            CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(datafeed.getJobId());
            client.execute(CloseJobAction.INSTANCE, closeJobRequest, new ActionListener<CloseJobAction.Response>() {

                @Override
                public void onResponse(CloseJobAction.Response response) {
                    if (!response.isClosed()) {
                        logger.error("[{}] job close action was not acknowledged", datafeed.getJobId());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("[" + datafeed.getJobId() + "] failed to  auto-close job", e);
                }
            });
        }
    }
}
