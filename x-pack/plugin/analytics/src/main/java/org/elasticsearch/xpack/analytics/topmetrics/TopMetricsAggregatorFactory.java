/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TopMetricsAggregatorFactory extends AggregatorFactory {
    private final List<SortBuilder<?>> sortBuilders;
    private final MultiValuesSourceFieldConfig metricField;

    public TopMetricsAggregatorFactory(String name, QueryShardContext queryShardContext, AggregatorFactory parent,
            Builder subFactoriesBuilder, Map<String, Object> metaData, List<SortBuilder<?>> sortBuilders,
            MultiValuesSourceFieldConfig metricField) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.sortBuilders = sortBuilders;
        this.metricField = metricField;
    }

    @Override
    protected TopMetricsAggregator createInternal(SearchContext searchContext, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        ValuesSourceConfig<ValuesSource.Numeric> metricFieldSource = ValuesSourceConfig.resolve(queryShardContext, ValueType.NUMERIC,
                metricField.getFieldName(), metricField.getScript(), metricField.getMissing(), metricField.getTimeZone(), null);
        ValuesSource.Numeric metricValueSource = metricFieldSource.toValuesSource(queryShardContext);
        if (metricValueSource == null) {
            return createUnmapped(searchContext, parent, pipelineAggregators, metaData);
        }
        Optional<SortAndFormats> sort = SortBuilder.buildSort(sortBuilders, queryShardContext);
        if (sort.isEmpty()) {
            return createUnmapped(searchContext, parent, pipelineAggregators, metaData);
        }
        if (sort.get().sort.getSort().length != 1) {
            throw new IllegalArgumentException("[top_metrics] only supports sorting on a single field");
        }
        SortField sortField = sort.get().sort.getSort()[0];

        FieldComparator<?> untypedCmp = sortField.getComparator(1, 0);
        /* This check is kind of nasty, but it is the only way we have of making sure we're getting numerics.
         * We would like to drop that requirement but we'll likely do that by getting *more* information
         * about the type, not less. */
        if (false == untypedCmp instanceof FieldComparator.NumericComparator &&
                false == untypedCmp instanceof FieldComparator.RelevanceComparator) {
            throw new IllegalArgumentException("[top_metrics] only supports sorting on numeric values");
        }
        @SuppressWarnings("unchecked") // We checked this with instanceof above
        FieldComparator<? extends Number> sortComparator = (FieldComparator<? extends Number>) untypedCmp;
        return new TopMetricsAggregator(name, searchContext, parent, pipelineAggregators, metaData, sortComparator,
                sortBuilders.get(0).order(), sort.get().formats[0], sortField.needsScores(), metricField.getFieldName(),
                metricValueSource);
    }

    private TopMetricsAggregator createUnmapped(SearchContext searchContext, Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        return new TopMetricsAggregator(name, searchContext, parent, pipelineAggregators, metaData, null,
                sortBuilders.get(0).order(), DocValueFormat.RAW, false, metricField.getFieldName(),
                null);
    }

}
