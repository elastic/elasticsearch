/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.search.aggregations.bucket.children.ChildrenParser;
import org.elasticsearch.search.aggregations.bucket.filter.FilterParser;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersParser;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridParser;
import org.elasticsearch.search.aggregations.bucket.global.GlobalParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramParser;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramParser;
import org.elasticsearch.search.aggregations.bucket.missing.MissingParser;
import org.elasticsearch.search.aggregations.bucket.nested.NestedParser;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedParser;
import org.elasticsearch.search.aggregations.bucket.range.RangeParser;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeParser;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceParser;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IpRangeParser;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerParser;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsParser;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificantTermsHeuristicModule;
import org.elasticsearch.search.aggregations.bucket.terms.TermsParser;
import org.elasticsearch.search.aggregations.metrics.avg.AvgParser;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityParser;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsParser;
import org.elasticsearch.search.aggregations.metrics.max.MaxParser;
import org.elasticsearch.search.aggregations.metrics.min.MinParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesParser;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricParser;
import org.elasticsearch.search.aggregations.metrics.stats.StatsParser;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsParser;
import org.elasticsearch.search.aggregations.metrics.sum.SumParser;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsParser;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountParser;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketParser;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumParser;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptParser;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativeParser;
import org.elasticsearch.search.aggregations.pipeline.having.BucketSelectorParser;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgParser;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModelModule;

import java.util.List;

/**
 * The main module for the get (binding all get components together)
 */
public class AggregationModule extends AbstractModule implements SpawnModules{

    private List<Class<? extends Aggregator.Parser>> aggParsers = Lists.newArrayList();
    private List<Class<? extends PipelineAggregator.Parser>> pipelineAggParsers = Lists.newArrayList();

    public AggregationModule() {
        aggParsers.add(AvgParser.class);
        aggParsers.add(SumParser.class);
        aggParsers.add(MinParser.class);
        aggParsers.add(MaxParser.class);
        aggParsers.add(StatsParser.class);
        aggParsers.add(ExtendedStatsParser.class);
        aggParsers.add(ValueCountParser.class);
        aggParsers.add(PercentilesParser.class);
        aggParsers.add(PercentileRanksParser.class);
        aggParsers.add(CardinalityParser.class);

        aggParsers.add(GlobalParser.class);
        aggParsers.add(MissingParser.class);
        aggParsers.add(FilterParser.class);
        aggParsers.add(FiltersParser.class);
        aggParsers.add(SamplerParser.class);
        aggParsers.add(TermsParser.class);
        aggParsers.add(SignificantTermsParser.class);
        aggParsers.add(RangeParser.class);
        aggParsers.add(DateRangeParser.class);
        aggParsers.add(IpRangeParser.class);
        aggParsers.add(HistogramParser.class);
        aggParsers.add(DateHistogramParser.class);
        aggParsers.add(GeoDistanceParser.class);
        aggParsers.add(GeoHashGridParser.class);
        aggParsers.add(NestedParser.class);
        aggParsers.add(ReverseNestedParser.class);
        aggParsers.add(TopHitsParser.class);
        aggParsers.add(GeoBoundsParser.class);
        aggParsers.add(ScriptedMetricParser.class);
        aggParsers.add(ChildrenParser.class);

        pipelineAggParsers.add(DerivativeParser.class);
        pipelineAggParsers.add(MaxBucketParser.class);
        pipelineAggParsers.add(MinBucketParser.class);
        pipelineAggParsers.add(AvgBucketParser.class);
        pipelineAggParsers.add(SumBucketParser.class);
        pipelineAggParsers.add(MovAvgParser.class);
        pipelineAggParsers.add(CumulativeSumParser.class);
        pipelineAggParsers.add(BucketScriptParser.class);
        pipelineAggParsers.add(BucketSelectorParser.class);
    }

    /**
     * Enabling extending the get module by adding a custom aggregation parser.
     *
     * @param parser The parser for the custom aggregator.
     */
    public void addAggregatorParser(Class<? extends Aggregator.Parser> parser) {
        aggParsers.add(parser);
    }

    @Override
    protected void configure() {
        Multibinder<Aggregator.Parser> multibinderAggParser = Multibinder.newSetBinder(binder(), Aggregator.Parser.class);
        for (Class<? extends Aggregator.Parser> parser : aggParsers) {
            multibinderAggParser.addBinding().to(parser);
        }
        Multibinder<PipelineAggregator.Parser> multibinderPipelineAggParser = Multibinder.newSetBinder(binder(), PipelineAggregator.Parser.class);
        for (Class<? extends PipelineAggregator.Parser> parser : pipelineAggParsers) {
            multibinderPipelineAggParser.addBinding().to(parser);
        }
        bind(AggregatorParsers.class).asEagerSingleton();
        bind(AggregationParseElement.class).asEagerSingleton();
        bind(AggregationPhase.class).asEagerSingleton();
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(new SignificantTermsHeuristicModule(), new MovAvgModelModule());
    }

}
