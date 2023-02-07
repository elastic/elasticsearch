/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations;

import org.elasticsearch.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.elasticsearch.aggregations.bucket.adjacency.InternalAdjacencyMatrix;
import org.elasticsearch.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.elasticsearch.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.elasticsearch.aggregations.bucket.timeseries.InternalTimeSeries;
import org.elasticsearch.aggregations.bucket.timeseries.TimeSeriesAggregationBuilder;
import org.elasticsearch.aggregations.metric.InternalMatrixStats;
import org.elasticsearch.aggregations.metric.MatrixStatsAggregationBuilder;
import org.elasticsearch.aggregations.metric.MatrixStatsParser;
import org.elasticsearch.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.elasticsearch.aggregations.pipeline.Derivative;
import org.elasticsearch.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.aggregations.pipeline.MovFnPipelineAggregationBuilder;
import org.elasticsearch.aggregations.pipeline.MovingFunctionScript;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.List;

public class AggregationsPlugin extends Plugin implements SearchPlugin, ScriptPlugin {
    @Override
    public List<AggregationSpec> getAggregations() {
        List<AggregationSpec> specs = new ArrayList<>();
        specs.add(
            new AggregationSpec(
                AdjacencyMatrixAggregationBuilder.NAME,
                AdjacencyMatrixAggregationBuilder::new,
                AdjacencyMatrixAggregationBuilder::parse
            ).addResultReader(InternalAdjacencyMatrix::new)
        );
        specs.add(
            new AggregationSpec(
                AutoDateHistogramAggregationBuilder.NAME,
                AutoDateHistogramAggregationBuilder::new,
                AutoDateHistogramAggregationBuilder.PARSER
            ).addResultReader(InternalAutoDateHistogram::new)
                .setAggregatorRegistrar(AutoDateHistogramAggregationBuilder::registerAggregators)
        );
        specs.add(
            new AggregationSpec(MatrixStatsAggregationBuilder.NAME, MatrixStatsAggregationBuilder::new, new MatrixStatsParser())
                .addResultReader(InternalMatrixStats::new)
        );
        specs.add(
            new AggregationSpec(TimeSeriesAggregationBuilder.NAME, TimeSeriesAggregationBuilder::new, TimeSeriesAggregationBuilder.PARSER)
                .addResultReader(InternalTimeSeries::new)
        );
        return List.copyOf(specs);
    }

    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {
        return List.of(
            new PipelineAggregationSpec(
                BucketSelectorPipelineAggregationBuilder.NAME,
                BucketSelectorPipelineAggregationBuilder::new,
                BucketSelectorPipelineAggregationBuilder::parse
            ),
            new PipelineAggregationSpec(
                BucketSortPipelineAggregationBuilder.NAME,
                BucketSortPipelineAggregationBuilder::new,
                BucketSortPipelineAggregationBuilder::parse
            ),
            new PipelineAggregationSpec(
                DerivativePipelineAggregationBuilder.NAME,
                DerivativePipelineAggregationBuilder::new,
                DerivativePipelineAggregationBuilder::parse
            ).addResultReader(Derivative::new),
            new PipelineAggregationSpec(
                MovFnPipelineAggregationBuilder.NAME,
                MovFnPipelineAggregationBuilder::new,
                MovFnPipelineAggregationBuilder.PARSER
            )
        );
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return List.of(MovingFunctionScript.CONTEXT);
    }
}
