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
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryAnalyzer;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

/** Builds extractors for one ES|QL datafeed; the query is parsed once, when the factory is created. */
public record EsqlDataExtractorFactory(
    Client client,
    DatafeedConfig datafeed,
    Job job,
    DatafeedTimingStatsReporter timingStatsReporter,
    EsqlQueryAnalyzer analyzer,
    EsqlDatafeedQuery query
) implements DataExtractorFactory {

    public static void create(
        Client client,
        DatafeedConfig datafeed,
        Job job,
        DatafeedTimingStatsReporter timingStatsReporter,
        EsqlQueryAnalyzer analyzer,
        ActionListener<DataExtractorFactory> factoryHandler
    ) {
        EsqlDatafeedQuery query;
        try {
            query = EsqlDatafeedQuery.parse(analyzer, datafeed.getEsqlQuery());
        } catch (Exception e) {
            factoryHandler.onFailure(e);
            return;
        }
        factoryHandler.onResponse(new EsqlDataExtractorFactory(client, datafeed, job, timingStatsReporter, analyzer, query));
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        EsqlDataExtractorContext ctx = new EsqlDataExtractorContext(
            job.getId(),
            query,
            analyzer,
            datafeed.getSourceTimeField(),
            job.getDataDescription().getTimeField(),
            datafeed.getGroupingInterval().millis(),
            start,
            end,
            datafeed.getHeaders(),
            EsqlDatafeedQueryValidator.requiredSummaryCountField(datafeed, job),
            datafeed.getProjectRouting()
        );
        return new EsqlDataExtractor(client, ctx, timingStatsReporter);
    }
}
