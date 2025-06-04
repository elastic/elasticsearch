/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;
import org.elasticsearch.xpack.ml.job.task.JobTask;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.process.NativeStorageProvider;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ml.action.ForecastJobAction.Request.DURATION;
import static org.elasticsearch.xpack.core.ml.action.ForecastJobAction.Request.FORECAST_LOCAL_STORAGE_LIMIT;

public class TransportForecastJobAction extends TransportJobTaskAction<ForecastJobAction.Request, ForecastJobAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportForecastJobAction.class);

    private final JobResultsProvider jobResultsProvider;
    private final JobManager jobManager;
    private final NativeStorageProvider nativeStorageProvider;
    private final AnomalyDetectionAuditor auditor;
    private final long cppMinAvailableDiskSpaceBytes;

    @Inject
    public TransportForecastJobAction(
        TransportService transportService,
        Settings settings,
        ClusterService clusterService,
        ActionFilters actionFilters,
        JobResultsProvider jobResultsProvider,
        AutodetectProcessManager processManager,
        JobManager jobManager,
        NativeStorageProvider nativeStorageProvider,
        AnomalyDetectionAuditor auditor
    ) {
        super(
            ForecastJobAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            ForecastJobAction.Request::new,
            ForecastJobAction.Response::new,
            // ThreadPool.Names.SAME, because operations is executed by autodetect worker thread
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            processManager
        );
        this.jobResultsProvider = jobResultsProvider;
        this.jobManager = jobManager;
        this.nativeStorageProvider = nativeStorageProvider;
        this.auditor = auditor;
        // The C++ enforces 80% of the free disk space that the Java enforces
        this.cppMinAvailableDiskSpaceBytes = MachineLearning.MIN_DISK_SPACE_OFF_HEAP.get(settings).getBytes() / 5 * 4;
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        ForecastJobAction.Request request,
        JobTask task,
        ActionListener<ForecastJobAction.Response> listener
    ) {
        jobManager.getJob(task.getJobId(), ActionListener.wrap(job -> {
            validate(job, request);

            ForecastParams.Builder paramsBuilder = ForecastParams.builder();

            if (request.getDuration() != null) {
                paramsBuilder.duration(request.getDuration());
            }

            if (request.getExpiresIn() != null) {
                paramsBuilder.expiresIn(request.getExpiresIn());
            }

            Long adjustedLimit = getAdjustedMemoryLimit(job, request.getMaxModelMemory(), auditor);
            if (adjustedLimit != null) {
                paramsBuilder.maxModelMemory(adjustedLimit);
            }

            // tmp storage might be null, we do not log here, because it might not be
            // required
            Path tmpStorage = nativeStorageProvider.tryGetLocalTmpStorage(task.getDescription(), FORECAST_LOCAL_STORAGE_LIMIT);
            if (tmpStorage != null) {
                paramsBuilder.tmpStorage(tmpStorage.toString());
            }

            if (cppMinAvailableDiskSpaceBytes >= 0) {
                paramsBuilder.minAvailableDiskSpace(cppMinAvailableDiskSpaceBytes);
            }

            ForecastParams params = paramsBuilder.build();
            processManager.forecastJob(task, params, e -> {
                if (e == null) {
                    getForecastRequestStats(request.getJobId(), params.getForecastId(), listener);
                } else {
                    listener.onFailure(e);
                }
            });
        }, listener::onFailure));
    }

    private void getForecastRequestStats(String jobId, String forecastId, ActionListener<ForecastJobAction.Response> listener) {
        Consumer<ForecastRequestStats> forecastRequestStatsHandler = forecastRequestStats -> {
            if (forecastRequestStats == null) {
                // paranoia case, it should not happen that we do not retrieve a result
                listener.onFailure(new ElasticsearchException("Cannot run forecast: internal error, please check the logs"));
            } else if (forecastRequestStats.getStatus() == ForecastRequestStats.ForecastRequestStatus.FAILED) {
                List<String> messages = forecastRequestStats.getMessages();
                if (messages.size() > 0) {
                    String message = messages.get(0);

                    // special case: if forecast failed due to insufficient disk space, log the setting
                    if (message.contains("disk space is insufficient")) {
                        message += " Minimum disk space required: [" + processManager.getMinLocalStorageAvailable() + "]";
                    }

                    listener.onFailure(ExceptionsHelper.badRequestException("Cannot run forecast: " + message));
                } else {
                    // paranoia case, it should not be possible to have an empty message list
                    listener.onFailure(new ElasticsearchException("Cannot run forecast: internal error, please check the logs"));
                }
            } else {
                listener.onResponse(new ForecastJobAction.Response(true, forecastId));
            }
        };

        jobResultsProvider.getForecastRequestStats(jobId, forecastId, forecastRequestStatsHandler, listener::onFailure);
    }

    static Long getAdjustedMemoryLimit(Job job, Long requestedLimit, AbstractAuditor<? extends AbstractAuditMessage> auditor) {
        if (requestedLimit == null) {
            return null;
        }
        long jobLimitMegaBytes = job.getAnalysisLimits() == null || job.getAnalysisLimits().getModelMemoryLimit() == null
            ? AnalysisLimits.PRE_6_1_DEFAULT_MODEL_MEMORY_LIMIT_MB
            : job.getAnalysisLimits().getModelMemoryLimit();
        long allowedMax = (long) (ByteSizeValue.ofMb(jobLimitMegaBytes).getBytes() * 0.40);
        long adjustedMax = Math.min(requestedLimit, allowedMax - 1);
        if (adjustedMax != requestedLimit) {
            String msg = "requested forecast memory limit ["
                + requestedLimit
                + "] bytes is greater than or equal to ["
                + allowedMax
                + "] bytes (40% of the job memory limit). Reducing to ["
                + adjustedMax
                + "].";
            logger.warn("[{}] {}", job.getId(), msg);
            auditor.warning(job.getId(), msg);
        }
        return adjustedMax;
    }

    static void validate(Job job, ForecastJobAction.Request request) {
        if (job.getJobVersion() == null || job.getJobVersion().before(MlConfigVersion.fromString("6.1.0"))) {
            throw ExceptionsHelper.badRequestException("Cannot run forecast because jobs created prior to version 6.1 are not supported");
        }

        if (request.getDuration() != null) {
            TimeValue duration = request.getDuration();
            TimeValue bucketSpan = job.getAnalysisConfig().getBucketSpan();

            if (duration.compareTo(bucketSpan) < 0) {
                throw ExceptionsHelper.badRequestException(
                    "["
                        + DURATION.getPreferredName()
                        + "] must be greater or equal to the bucket span: ["
                        + duration.getStringRep()
                        + "/"
                        + bucketSpan.getStringRep()
                        + "]"
                );
            }
        }
    }
}
