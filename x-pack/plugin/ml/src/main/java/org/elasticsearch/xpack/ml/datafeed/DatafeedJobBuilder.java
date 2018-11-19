/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetector;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory;
import org.elasticsearch.xpack.ml.job.persistence.BucketsQueryBuilder;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.util.Collections;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class DatafeedJobBuilder {

    private final Client client;
    private final JobResultsProvider jobResultsProvider;
    private final Auditor auditor;
    private final Supplier<Long> currentTimeSupplier;

    public DatafeedJobBuilder(Client client, JobResultsProvider jobResultsProvider, Auditor auditor, Supplier<Long> currentTimeSupplier) {
        this.client = client;
        this.jobResultsProvider = Objects.requireNonNull(jobResultsProvider);
        this.auditor = Objects.requireNonNull(auditor);
        this.currentTimeSupplier = Objects.requireNonNull(currentTimeSupplier);
    }

    void build(Job job, DatafeedConfig datafeed, ActionListener<DatafeedJob> listener) {

        // Step 5. Build datafeed job object
        Consumer<Context> contextHanlder = context -> {
            TimeValue frequency = getFrequencyOrDefault(datafeed, job);
            TimeValue queryDelay = datafeed.getQueryDelay();
            DelayedDataDetector delayedDataDetector = DelayedDataDetectorFactory.buildDetector(job, datafeed, client);
            DatafeedJob datafeedJob = new DatafeedJob(job.getId(), buildDataDescription(job), frequency.millis(), queryDelay.millis(),
                    context.dataExtractorFactory, client, auditor, currentTimeSupplier, delayedDataDetector,
                    context.latestFinalBucketEndMs, context.latestRecordTimeMs);
            listener.onResponse(datafeedJob);
        };

        final Context context = new Context();

        // Step 4. Context building complete - invoke final listener
        ActionListener<DataExtractorFactory> dataExtractorFactoryHandler = ActionListener.wrap(
                dataExtractorFactory -> {
                    context.dataExtractorFactory = dataExtractorFactory;
                    contextHanlder.accept(context);
                }, e -> {
                    auditor.error(job.getId(), e.getMessage());
                    listener.onFailure(e);
                }
        );

        // Step 3. Create data extractor factory
        Consumer<DataCounts> dataCountsHandler = dataCounts -> {
            if (dataCounts.getLatestRecordTimeStamp() != null) {
                context.latestRecordTimeMs = dataCounts.getLatestRecordTimeStamp().getTime();
            }
            DataExtractorFactory.create(client, datafeed, job, dataExtractorFactoryHandler);
        };

        // Step 2. Collect data counts
        Consumer<QueryPage<Bucket>> bucketsHandler = buckets -> {
            if (buckets.results().size() == 1) {
                TimeValue bucketSpan = job.getAnalysisConfig().getBucketSpan();
                context.latestFinalBucketEndMs = buckets.results().get(0).getTimestamp().getTime() + bucketSpan.millis() - 1;
            }
            jobResultsProvider.dataCounts(job.getId(), dataCountsHandler, listener::onFailure);
        };

        // Step 1. Collect latest bucket
        BucketsQueryBuilder latestBucketQuery = new BucketsQueryBuilder()
                .sortField(Result.TIMESTAMP.getPreferredName())
                .sortDescending(true).size(1)
                .includeInterim(false);
        jobResultsProvider.bucketsViaInternalClient(job.getId(), latestBucketQuery, bucketsHandler, e -> {
            if (e instanceof ResourceNotFoundException) {
                QueryPage<Bucket> empty = new QueryPage<>(Collections.emptyList(), 0, Bucket.RESULT_TYPE_FIELD);
                bucketsHandler.accept(empty);
            } else {
                listener.onFailure(e);
            }
        });
    }

    private static TimeValue getFrequencyOrDefault(DatafeedConfig datafeed, Job job) {
        TimeValue frequency = datafeed.getFrequency();
        if (frequency == null) {
            TimeValue bucketSpan = job.getAnalysisConfig().getBucketSpan();
            return datafeed.defaultFrequency(bucketSpan);
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

    private static class Context {
        volatile long latestFinalBucketEndMs = -1L;
        volatile long latestRecordTimeMs = -1L;
        volatile DataExtractorFactory dataExtractorFactory;
    }
}
