/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

/**
 * Creates extractors for an ES|QL datafeed. {@code timeColumn} is the time column as {@code DESCRIBE} reported it when the task
 * started: extractors filter the source on its name and size chunks by its bucket interval.
 */
public record EsqlDataExtractorFactory(
    Client client,
    DatafeedConfig datafeed,
    Job job,
    DatafeedTimingStatsReporter timingStatsReporter,
    TimeColumn timeColumn
) implements DataExtractorFactory {

    /**
     * Runs the master's {@code DESCRIBE} checks again and keeps the time column it reports, for {@link #timeColumn}. The remote
     * sources are extracted on the master only. A failure of this {@code DESCRIBE} fails the task.
     */
    public static void create(
        Client client,
        DatafeedConfig datafeed,
        Job job,
        DatafeedTimingStatsReporter timingStatsReporter,
        ActionListener<DataExtractorFactory> factoryHandler
    ) {
        create(EsqlQueryRunner.DEFAULT, client, datafeed, job, timingStatsReporter, factoryHandler);
    }

    static void create(
        EsqlQueryRunner queryRunner,
        Client client,
        DatafeedConfig datafeed,
        Job job,
        DatafeedTimingStatsReporter timingStatsReporter,
        ActionListener<DataExtractorFactory> factoryHandler
    ) {
        String timeField = job.getDataDescription().getTimeField();
        String summaryCountField = EsqlDatafeedQueryValidator.requiredSummaryCountField(datafeed, job);
        EsqlDatafeedQueryValidator.describeRequiredColumns(
            queryRunner,
            client,
            datafeed.getHeaders(),
            datafeed.getEsqlQuery(),
            datafeed.getProjectRouting(),
            timeField,
            summaryCountField,
            factoryHandler.delegateFailureAndWrap((handler, response) -> {
                TimeColumn timeColumn = EsqlDatafeedQueryValidator.checkResponse(
                    response,
                    timeField,
                    summaryCountField,
                    job.getAnalysisConfig().getBucketSpan(),
                    datafeed.getFrequency(),
                    datafeed.getId()
                );
                handler.onResponse(new EsqlDataExtractorFactory(client, datafeed, job, timingStatsReporter, timeColumn));
            })
        );
    }

    @Override
    public long timeBucketMillis() {
        return timeColumn.bucketMillis();
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        EsqlDataExtractorContext ctx = new EsqlDataExtractorContext(
            job.getId(),
            datafeed.getEsqlQuery(),
            timeColumn.name(),
            start,
            end,
            datafeed.getHeaders(),
            EsqlDatafeedQueryValidator.requiredSummaryCountField(datafeed, job),
            datafeed.getProjectRouting()
        );
        return new EsqlDataExtractor(client, ctx, timingStatsReporter);
    }
}
