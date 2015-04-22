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

import java.util.List;

/**
 * The main module for the get (binding all get components together)
 */
public class AggregationModule extends AbstractModule implements SpawnModules{

    private List<Class<? extends Aggregator.Parser>> parsers = Lists.newArrayList();

    public AggregationModule() {
        parsers.add(AvgParser.class);
        parsers.add(SumParser.class);
        parsers.add(MinParser.class);
        parsers.add(MaxParser.class);
        parsers.add(StatsParser.class);
        parsers.add(ExtendedStatsParser.class);
        parsers.add(ValueCountParser.class);
        parsers.add(PercentilesParser.class);
        parsers.add(PercentileRanksParser.class);
        parsers.add(CardinalityParser.class);

        parsers.add(GlobalParser.class);
        parsers.add(MissingParser.class);
        parsers.add(FilterParser.class);
        parsers.add(FiltersParser.class);
        parsers.add(SamplerParser.class);
        parsers.add(TermsParser.class);
        parsers.add(SignificantTermsParser.class);
        parsers.add(RangeParser.class);
        parsers.add(DateRangeParser.class);
        parsers.add(IpRangeParser.class);
        parsers.add(HistogramParser.class);
        parsers.add(DateHistogramParser.class);
        parsers.add(GeoDistanceParser.class);
        parsers.add(GeoHashGridParser.class);
        parsers.add(NestedParser.class);
        parsers.add(ReverseNestedParser.class);
        parsers.add(TopHitsParser.class);
        parsers.add(GeoBoundsParser.class);
        parsers.add(ScriptedMetricParser.class);
        parsers.add(ChildrenParser.class);
    }

    /**
     * Enabling extending the get module by adding a custom aggregation parser.
     *
     * @param parser The parser for the custom aggregator.
     */
    public void addAggregatorParser(Class<? extends Aggregator.Parser> parser) {
        parsers.add(parser);
    }

    @Override
    protected void configure() {
        Multibinder<Aggregator.Parser> multibinder = Multibinder.newSetBinder(binder(), Aggregator.Parser.class);
        for (Class<? extends Aggregator.Parser> parser : parsers) {
            multibinder.addBinding().to(parser);
        }
        bind(AggregatorParsers.class).asEagerSingleton();
        bind(AggregationParseElement.class).asEagerSingleton();
        bind(AggregationPhase.class).asEagerSingleton();
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(new SignificantTermsHeuristicModule());
    }

}
