/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

public record AggregationDataExtractorFactory(
    Client client,
    DatafeedConfig datafeedConfig,
    Job job,
    NamedXContentRegistry xContentRegistry,
    DatafeedTimingStatsReporter timingStatsReporter
) implements DataExtractorFactory {

    public static AggregatedSearchRequestBuilder requestBuilder(Client client, String[] indices, IndicesOptions indicesOptions) {
        return (searchSourceBuilder) -> new SearchRequestBuilder(client, SearchAction.INSTANCE).setSource(searchSourceBuilder)
            .setIndicesOptions(indicesOptions)
            .setAllowPartialSearchResults(false)
            .setIndices(indices);
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        return buildExtractor(start, end, datafeedConfig.getParsedQuery(xContentRegistry));
    }

    @Override
    public DataExtractor newExtractor(long start, long end, QueryBuilder queryBuilder) {
        return buildExtractor(
            start,
            end,
            QueryBuilders.boolQuery().filter(datafeedConfig.getParsedQuery(xContentRegistry)).filter(queryBuilder)
        );
    }

    private DataExtractor buildExtractor(long start, long end, QueryBuilder queryBuilder) {
        long histogramInterval = datafeedConfig.getHistogramIntervalMillis(xContentRegistry);
        AggregationDataExtractorContext dataExtractorContext = new AggregationDataExtractorContext(
            job.getId(),
            job.getDataDescription().getTimeField(),
            job.getAnalysisConfig().analysisFields(),
            datafeedConfig.getIndices(),
            queryBuilder,
            datafeedConfig.getParsedAggregations(xContentRegistry),
            Intervals.alignToCeil(start, histogramInterval),
            Intervals.alignToFloor(end, histogramInterval),
            job.getAnalysisConfig().getSummaryCountFieldName().equals(DatafeedConfig.DOC_COUNT),
            datafeedConfig.getHeaders(),
            datafeedConfig.getIndicesOptions(),
            datafeedConfig.getRuntimeMappings()
        );
        return new AggregationDataExtractor(client, dataExtractorContext, timingStatsReporter);
    }
}
