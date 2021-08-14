/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.action.TransportStartDatafeedAction;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetector;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ml.MachineLearning.DELAYED_DATA_CHECK_FREQ;

public class DatafeedJobBuilder {

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final AnomalyDetectionAuditor auditor;
    private final AnnotationPersister annotationPersister;
    private final Supplier<Long> currentTimeSupplier;
    private final JobResultsPersister jobResultsPersister;
    private final boolean remoteClusterClient;
    private final String nodeName;

    private volatile long delayedDataCheckFreq;

    public DatafeedJobBuilder(Client client, NamedXContentRegistry xContentRegistry, AnomalyDetectionAuditor auditor,
                              AnnotationPersister annotationPersister, Supplier<Long> currentTimeSupplier,
                              JobResultsPersister jobResultsPersister, Settings settings, ClusterService clusterService) {
        this.client = client;
        this.xContentRegistry = Objects.requireNonNull(xContentRegistry);
        this.auditor = Objects.requireNonNull(auditor);
        this.annotationPersister = Objects.requireNonNull(annotationPersister);
        this.currentTimeSupplier = Objects.requireNonNull(currentTimeSupplier);
        this.jobResultsPersister = Objects.requireNonNull(jobResultsPersister);
        this.remoteClusterClient = DiscoveryNode.isRemoteClusterClient(settings);
        this.nodeName = clusterService.getNodeName();
        this.delayedDataCheckFreq = DELAYED_DATA_CHECK_FREQ.get(settings).millis();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DELAYED_DATA_CHECK_FREQ, this::setDelayedDataCheckFreq);
    }

    private void setDelayedDataCheckFreq(TimeValue value) {
        this.delayedDataCheckFreq = value.millis();
    }

    void build(TransportStartDatafeedAction.DatafeedTask task, DatafeedContext context, ActionListener<DatafeedJob> listener) {
        final ParentTaskAssigningClient parentTaskAssigningClient = new ParentTaskAssigningClient(client, task.getParentTaskId());
        final DatafeedConfig datafeedConfig = context.getDatafeedConfig();
        final Job job = context.getJob();
        final long latestFinalBucketEndMs = context.getRestartTimeInfo().getLatestFinalBucketTimeMs() == null ?
            -1 : context.getRestartTimeInfo().getLatestFinalBucketTimeMs() + job.getAnalysisConfig().getBucketSpan().millis() - 1;
        final long latestRecordTimeMs = context.getRestartTimeInfo().getLatestRecordTimeMs() == null ?
            -1 : context.getRestartTimeInfo().getLatestRecordTimeMs();
        final DatafeedTimingStatsReporter timingStatsReporter = new DatafeedTimingStatsReporter(context.getTimingStats(),
            jobResultsPersister::persistDatafeedTimingStats);

        // Validate remote indices are available and get the job
        try {
            checkRemoteIndicesAreAvailable(datafeedConfig);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        // Re-validation is required as the config has been re-read since
        // the previous validation
        try {
            DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        ActionListener<DataExtractorFactory> dataExtractorFactoryHandler = ActionListener.wrap(
            dataExtractorFactory -> {
                TimeValue frequency = getFrequencyOrDefault(datafeedConfig, job, xContentRegistry);
                TimeValue queryDelay = datafeedConfig.getQueryDelay();
                DelayedDataDetector delayedDataDetector = DelayedDataDetectorFactory.buildDetector(job,
                    datafeedConfig, parentTaskAssigningClient, xContentRegistry);
                DatafeedJob datafeedJob = new DatafeedJob(
                        job.getId(),
                        buildDataDescription(job),
                        frequency.millis(),
                        queryDelay.millis(),
                        dataExtractorFactory,
                        timingStatsReporter,
                        parentTaskAssigningClient,
                        auditor,
                        annotationPersister,
                        currentTimeSupplier,
                        delayedDataDetector,
                        datafeedConfig.getMaxEmptySearches(),
                        latestFinalBucketEndMs,
                        latestRecordTimeMs,
                        context.getRestartTimeInfo().haveSeenDataPreviously(),
                        delayedDataCheckFreq
                    );

                listener.onResponse(datafeedJob);
            }, e -> {
                auditor.error(job.getId(), e.getMessage());
                listener.onFailure(e);
            }
        );

        DataExtractorFactory.create(
            parentTaskAssigningClient,
            datafeedConfig,
            job,
            xContentRegistry,
            timingStatsReporter,
            dataExtractorFactoryHandler);
    }

    private void checkRemoteIndicesAreAvailable(DatafeedConfig datafeedConfig) {
        if (remoteClusterClient == false) {
            List<String> remoteIndices = RemoteClusterLicenseChecker.remoteIndices(datafeedConfig.getIndices());
            if (remoteIndices.isEmpty() == false) {
                throw ExceptionsHelper.badRequestException(Messages.getMessage(
                        Messages.DATAFEED_NEEDS_REMOTE_CLUSTER_SEARCH,
                        datafeedConfig.getId(),
                        remoteIndices,
                        nodeName));
            }
        }
    }

    private static TimeValue getFrequencyOrDefault(DatafeedConfig datafeed, Job job, NamedXContentRegistry xContentRegistry) {
        TimeValue frequency = datafeed.getFrequency();
        if (frequency == null) {
            TimeValue bucketSpan = job.getAnalysisConfig().getBucketSpan();
            return datafeed.defaultFrequency(bucketSpan, xContentRegistry);
        }
        return frequency;
    }

    private static DataDescription buildDataDescription(Job job) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.XCONTENT);
        if (job.getDataDescription() != null) {
            dataDescription.setTimeField(job.getDataDescription().getTimeField());
        }
        dataDescription.setTimeFormat(DataDescription.EPOCH_MS);
        return dataDescription.build();
    }

}
