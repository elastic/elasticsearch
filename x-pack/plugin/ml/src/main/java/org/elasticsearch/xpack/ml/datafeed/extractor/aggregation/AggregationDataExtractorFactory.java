/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.Intervals;

import java.util.Objects;

public class AggregationDataExtractorFactory implements DataExtractorFactory {

    private final Client client;
    private final DatafeedConfig datafeedConfig;
    private final Job job;

    public AggregationDataExtractorFactory(Client client, DatafeedConfig datafeedConfig, Job job) {
        this.client = Objects.requireNonNull(client);
        this.datafeedConfig = Objects.requireNonNull(datafeedConfig);
        this.job = Objects.requireNonNull(job);
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        long histogramInterval = datafeedConfig.getHistogramIntervalMillis();
        AggregationDataExtractorContext dataExtractorContext = new AggregationDataExtractorContext(
                job.getId(),
                job.getDataDescription().getTimeField(),
                job.getAnalysisConfig().analysisFields(),
                datafeedConfig.getIndices(),
                datafeedConfig.getTypes(),
                datafeedConfig.getParsedQuery(),
                datafeedConfig.getParsedAggregations(),
                Intervals.alignToCeil(start, histogramInterval),
                Intervals.alignToFloor(end, histogramInterval),
                job.getAnalysisConfig().getSummaryCountFieldName().equals(DatafeedConfig.DOC_COUNT),
                datafeedConfig.getHeaders());
        return new AggregationDataExtractor(client, dataExtractorContext);
    }
}
