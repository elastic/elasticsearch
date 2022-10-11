/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class CompositeAggregationDataExtractorFactory implements DataExtractorFactory {

    private final Client client;
    private final DatafeedConfig datafeedConfig;
    private final Job job;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private final String compositeAggName;
    private final Collection<AggregationBuilder> subAggs;
    private final Collection<PipelineAggregationBuilder> subPipelineAggs;
    private final String dateHistogramGroupSourceName;
    private final AggregatedSearchRequestBuilder requestBuilder;
    private final int numBuckets;
    private final List<CompositeValuesSourceBuilder<?>> compositeValuesSourceBuilders;
    private final QueryBuilder parsedQuery;

    public CompositeAggregationDataExtractorFactory(
        Client client,
        DatafeedConfig datafeedConfig,
        Job job,
        NamedXContentRegistry xContentRegistry,
        DatafeedTimingStatsReporter timingStatsReporter,
        AggregatedSearchRequestBuilder requestBuilder
    ) {
        this.client = Objects.requireNonNull(client);
        this.datafeedConfig = Objects.requireNonNull(datafeedConfig);
        this.job = Objects.requireNonNull(job);
        this.timingStatsReporter = Objects.requireNonNull(timingStatsReporter);
        this.parsedQuery = datafeedConfig.getParsedQuery(xContentRegistry);
        AggregationBuilder aggregationBuilder = ExtractorUtils.getHistogramAggregation(
            datafeedConfig.getParsedAggregations(xContentRegistry).getAggregatorFactories()
        );
        if (aggregationBuilder instanceof CompositeAggregationBuilder == false) {
            throw new IllegalArgumentException(
                "top level aggregation must be a composite agg ["
                    + aggregationBuilder.getName()
                    + "] is a ["
                    + aggregationBuilder.getType()
                    + "]"
            );
        }
        CompositeAggregationBuilder compositeAggregationBuilder = (CompositeAggregationBuilder) aggregationBuilder;
        this.numBuckets = compositeAggregationBuilder.size();
        this.compositeAggName = compositeAggregationBuilder.getName();
        this.subAggs = compositeAggregationBuilder.getSubAggregations();
        this.subPipelineAggs = compositeAggregationBuilder.getPipelineAggregations();
        // We want to make sure our date_histogram source is first. This way we order at the top level by the timestamp
        this.compositeValuesSourceBuilders = new ArrayList<>(compositeAggregationBuilder.sources().size());
        List<CompositeValuesSourceBuilder<?>> others = new ArrayList<>(compositeAggregationBuilder.sources().size() - 1);
        String dateHistoGroupName = null;
        for (CompositeValuesSourceBuilder<?> sourceBuilder : compositeAggregationBuilder.sources()) {
            if (sourceBuilder instanceof DateHistogramValuesSourceBuilder) {
                this.compositeValuesSourceBuilders.add(sourceBuilder);
                dateHistoGroupName = sourceBuilder.name();
            } else {
                others.add(sourceBuilder);
            }
        }
        this.dateHistogramGroupSourceName = dateHistoGroupName;
        this.compositeValuesSourceBuilders.addAll(others);
        this.requestBuilder = requestBuilder;
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        return buildNewExtractor(start, end, parsedQuery);
    }

    @Override
    public DataExtractor newExtractor(long start, long end, QueryBuilder queryBuilder) {
        return buildNewExtractor(start, end, QueryBuilders.boolQuery().filter(parsedQuery).filter(queryBuilder));
    }

    private DataExtractor buildNewExtractor(long start, long end, QueryBuilder queryBuilder) {
        CompositeAggregationBuilder compositeAggregationBuilder = new CompositeAggregationBuilder(
            compositeAggName,
            compositeValuesSourceBuilders
        );
        compositeAggregationBuilder.size(numBuckets);
        subAggs.forEach(compositeAggregationBuilder::subAggregation);
        subPipelineAggs.forEach(compositeAggregationBuilder::subAggregation);
        long histogramInterval = ExtractorUtils.getHistogramIntervalMillis(compositeAggregationBuilder);
        CompositeAggregationDataExtractorContext dataExtractorContext = new CompositeAggregationDataExtractorContext(
            job.getId(),
            job.getDataDescription().getTimeField(),
            job.getAnalysisConfig().analysisFields(),
            datafeedConfig.getIndices(),
            queryBuilder,
            compositeAggregationBuilder,
            this.dateHistogramGroupSourceName,
            Intervals.alignToCeil(start, histogramInterval),
            Intervals.alignToFloor(end, histogramInterval),
            job.getAnalysisConfig().getSummaryCountFieldName().equals(DatafeedConfig.DOC_COUNT),
            datafeedConfig.getHeaders(),
            datafeedConfig.getIndicesOptions(),
            datafeedConfig.getRuntimeMappings()
        );
        return new CompositeAggregationDataExtractor(
            compositeAggregationBuilder,
            client,
            dataExtractorContext,
            timingStatsReporter,
            requestBuilder
        );
    }
}
