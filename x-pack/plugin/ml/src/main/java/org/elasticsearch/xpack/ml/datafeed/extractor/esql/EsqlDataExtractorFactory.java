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

public record EsqlDataExtractorFactory(Client client, DatafeedConfig datafeed, Job job, DatafeedTimingStatsReporter timingStatsReporter)
    implements
        DataExtractorFactory {

    public static void create(
        Client client,
        DatafeedConfig datafeed,
        Job job,
        DatafeedTimingStatsReporter timingStatsReporter,
        ActionListener<DataExtractorFactory> factoryHandler
    ) {
        DataExtractorFactory factory = new EsqlDataExtractorFactory(client, datafeed, job, timingStatsReporter);
        factoryHandler.onResponse(factory);
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        EsqlDataExtractorContext ctx = new EsqlDataExtractorContext(
            job.getId(),
            datafeed.getEsqlQuery(),
            job.getDataDescription().getTimeField(),
            start,
            end,
            datafeed.getHeaders(),
            datafeed.getIndices(),
            datafeed.getIndicesOptions()
        );
        return new EsqlDataExtractor(client, ctx, timingStatsReporter);
    }
}
