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

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.children.ChildrenBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.global.GlobalBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IPv4RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.Sampler;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesBuilder;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountBuilder;

/**
 * Utility class to create aggregations.
 */
public class AggregationBuilders {

    private AggregationBuilders() {
    }

    /**
     * Create a new {@link ValueCount} aggregation with the given name.
     */
    public static ValueCountBuilder count(String name) {
        return new ValueCountBuilder(name);
    }

    /**
     * Create a new {@link Avg} aggregation with the given name.
     */
    public static AvgBuilder avg(String name) {
        return new AvgBuilder(name);
    }

    /**
     * Create a new {@link Max} aggregation with the given name.
     */
    public static MaxBuilder max(String name) {
        return new MaxBuilder(name);
    }

    /**
     * Create a new {@link Min} aggregation with the given name.
     */
    public static MinBuilder min(String name) {
        return new MinBuilder(name);
    }

    /**
     * Create a new {@link Sum} aggregation with the given name.
     */
    public static SumBuilder sum(String name) {
        return new SumBuilder(name);
    }

    /**
     * Create a new {@link Stats} aggregation with the given name.
     */
    public static StatsBuilder stats(String name) {
        return new StatsBuilder(name);
    }

    /**
     * Create a new {@link ExtendedStats} aggregation with the given name.
     */
    public static ExtendedStatsBuilder extendedStats(String name) {
        return new ExtendedStatsBuilder(name);
    }

    /**
     * Create a new {@link Filter} aggregation with the given name.
     */
    public static FilterAggregationBuilder filter(String name) {
        return new FilterAggregationBuilder(name);
    }

    /**
     * Create a new {@link Filters} aggregation with the given name.
     */
    public static FiltersAggregationBuilder filters(String name) {
        return new FiltersAggregationBuilder(name);
    }

    /**
     * Create a new {@link Sampler} aggregation with the given name.
     */
    public static SamplerAggregationBuilder sampler(String name) {
        return new SamplerAggregationBuilder(name);
    }

    /**
     * Create a new {@link Global} aggregation with the given name.
     */
    public static GlobalBuilder global(String name) {
        return new GlobalBuilder(name);
    }

    /**
     * Create a new {@link Missing} aggregation with the given name.
     */
    public static MissingBuilder missing(String name) {
        return new MissingBuilder(name);
    }

    /**
     * Create a new {@link Nested} aggregation with the given name.
     */
    public static NestedBuilder nested(String name) {
        return new NestedBuilder(name);
    }

    /**
     * Create a new {@link ReverseNested} aggregation with the given name.
     */
    public static ReverseNestedBuilder reverseNested(String name) {
        return new ReverseNestedBuilder(name);
    }

    /**
     * Create a new {@link Children} aggregation with the given name.
     */
    public static ChildrenBuilder children(String name) {
        return new ChildrenBuilder(name);
    }

    /**
     * Create a new {@link GeoDistance} aggregation with the given name.
     */
    public static GeoDistanceBuilder geoDistance(String name) {
        return new GeoDistanceBuilder(name);
    }

    /**
     * Create a new {@link Histogram} aggregation with the given name.
     */
    public static HistogramBuilder histogram(String name) {
        return new HistogramBuilder(name);
    }

    /**
     * Create a new {@link GeoHashGrid} aggregation with the given name.
     */
    public static GeoHashGridBuilder geohashGrid(String name) {
        return new GeoHashGridBuilder(name);
    }

    /**
     * Create a new {@link SignificantTerms} aggregation with the given name.
     */
    public static SignificantTermsBuilder significantTerms(String name) {
        return new SignificantTermsBuilder(name);
    }

    /**
     * Create a new {@link DateHistogram} aggregation with the given name.
     */
    public static DateHistogramBuilder dateHistogram(String name) {
        return new DateHistogramBuilder(name);
    }

    /**
     * Create a new {@link Range} aggregation with the given name.
     */
    public static RangeBuilder range(String name) {
        return new RangeBuilder(name);
    }

    /**
     * Create a new {@link DateRange} aggregation with the given name.
     */
    public static DateRangeBuilder dateRange(String name) {
        return new DateRangeBuilder(name);
    }

    /**
     * Create a new {@link IPv4Range} aggregation with the given name.
     */
    public static IPv4RangeBuilder ipRange(String name) {
        return new IPv4RangeBuilder(name);
    }

    /**
     * Create a new {@link Terms} aggregation with the given name.
     */
    public static TermsBuilder terms(String name) {
        return new TermsBuilder(name);
    }

    /**
     * Create a new {@link Percentiles} aggregation with the given name.
     */
    public static PercentilesBuilder percentiles(String name) {
        return new PercentilesBuilder(name);
    }

    /**
     * Create a new {@link PercentileRanks} aggregation with the given name.
     */
    public static PercentileRanksBuilder percentileRanks(String name) {
        return new PercentileRanksBuilder(name);
    }

    /**
     * Create a new {@link Cardinality} aggregation with the given name.
     */
    public static CardinalityBuilder cardinality(String name) {
        return new CardinalityBuilder(name);
    }

    /**
     * Create a new {@link TopHits} aggregation with the given name.
     */
    public static TopHitsBuilder topHits(String name) {
        return new TopHitsBuilder(name);
    }

    /**
     * Create a new {@link GeoBounds} aggregation with the given name.
     */
    public static GeoBoundsBuilder geoBounds(String name) {
        return new GeoBoundsBuilder(name);
    }

    /**
     * Create a new {@link ScriptedMetric} aggregation with the given name.
     */
    public static ScriptedMetricBuilder scriptedMetric(String name) {
        return new ScriptedMetricBuilder(name);
    }
}
