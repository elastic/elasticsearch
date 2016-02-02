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
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.children.ParentToChildrenAggregator.ChildrenAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregator.FilterAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator.FiltersAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridParser.GeoGridAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregator.GlobalAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregator.DateHistogramAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregator.HistogramAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregator.MissingAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator.NestedAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregator.ReverseNestedAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.RangeAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceParser.GeoDistanceAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IPv4RangeAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.Sampler;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregator.DiversifiedAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregator.SamplerAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregator.AvgAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsAggregator.GeoBoundsAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidAggregator.GeoCentroidAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregator.MaxAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregator.MinAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricAggregator.ScriptedMetricAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregator.StatsAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregator.ExtendedStatsAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregator.SumAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregator.TopHitsAggregatorBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregator.ValueCountAggregatorBuilder;

/**
 * Utility class to create aggregations.
 */
public class AggregationBuilders {

    private AggregationBuilders() {
    }

    /**
     * Create a new {@link ValueCount} aggregation with the given name.
     */
    public static ValueCountAggregatorBuilder count(String name) {
        return new ValueCountAggregatorBuilder(name, null);
    }

    /**
     * Create a new {@link Avg} aggregation with the given name.
     */
    public static AvgAggregatorBuilder avg(String name) {
        return new AvgAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Max} aggregation with the given name.
     */
    public static MaxAggregatorBuilder max(String name) {
        return new MaxAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Min} aggregation with the given name.
     */
    public static MinAggregatorBuilder min(String name) {
        return new MinAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Sum} aggregation with the given name.
     */
    public static SumAggregatorBuilder sum(String name) {
        return new SumAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Stats} aggregation with the given name.
     */
    public static StatsAggregatorBuilder stats(String name) {
        return new StatsAggregatorBuilder(name);
    }

    /**
     * Create a new {@link ExtendedStats} aggregation with the given name.
     */
    public static ExtendedStatsAggregatorBuilder extendedStats(String name) {
        return new ExtendedStatsAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Filter} aggregation with the given name.
     */
    public static FilterAggregatorBuilder filter(String name, QueryBuilder<?> filter) {
        return new FilterAggregatorBuilder(name, filter);
    }

    /**
     * Create a new {@link Filters} aggregation with the given name.
     */
    public static FiltersAggregatorBuilder filters(String name, KeyedFilter... filters) {
        return new FiltersAggregatorBuilder(name, filters);
    }

    /**
     * Create a new {@link Filters} aggregation with the given name.
     */
    public static FiltersAggregatorBuilder filters(String name, QueryBuilder<?>... filters) {
        return new FiltersAggregatorBuilder(name, filters);
    }

    /**
     * Create a new {@link Sampler} aggregation with the given name.
     */
    public static SamplerAggregatorBuilder sampler(String name) {
        return new SamplerAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Sampler} aggregation with the given name.
     */
    public static DiversifiedAggregatorBuilder diversifiedSampler(String name) {
        return new DiversifiedAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Global} aggregation with the given name.
     */
    public static GlobalAggregatorBuilder global(String name) {
        return new GlobalAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Missing} aggregation with the given name.
     */
    public static MissingAggregatorBuilder missing(String name) {
        return new MissingAggregatorBuilder(name, null);
    }

    /**
     * Create a new {@link Nested} aggregation with the given name.
     */
    public static NestedAggregatorBuilder nested(String name, String path) {
        return new NestedAggregatorBuilder(name, path);
    }

    /**
     * Create a new {@link ReverseNested} aggregation with the given name.
     */
    public static ReverseNestedAggregatorBuilder reverseNested(String name) {
        return new ReverseNestedAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Children} aggregation with the given name.
     */
    public static ChildrenAggregatorBuilder children(String name, String childType) {
        return new ChildrenAggregatorBuilder(name, childType);
    }

    /**
     * Create a new {@link GeoDistance} aggregation with the given name.
     */
    public static GeoDistanceAggregatorBuilder geoDistance(String name, GeoPoint origin) {
        return new GeoDistanceAggregatorBuilder(name, origin);
    }

    /**
     * Create a new {@link Histogram} aggregation with the given name.
     */
    public static HistogramAggregatorBuilder histogram(String name) {
        return new HistogramAggregatorBuilder(name);
    }

    /**
     * Create a new {@link GeoHashGrid} aggregation with the given name.
     */
    public static GeoGridAggregatorBuilder geohashGrid(String name) {
        return new GeoGridAggregatorBuilder(name);
    }

    /**
     * Create a new {@link SignificantTerms} aggregation with the given name.
     */
    public static SignificantTermsAggregatorBuilder significantTerms(String name) {
        return new SignificantTermsAggregatorBuilder(name, null);
    }

    /**
     * Create a new {@link DateHistogramAggregatorBuilder} aggregation with the given
     * name.
     */
    public static DateHistogramAggregatorBuilder dateHistogram(String name) {
        return new DateHistogramAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Range} aggregation with the given name.
     */
    public static RangeAggregatorBuilder range(String name) {
        return new RangeAggregatorBuilder(name);
    }

    /**
     * Create a new {@link DateRangeAggregatorBuilder} aggregation with the
     * given name.
     */
    public static DateRangeAggregatorBuilder dateRange(String name) {
        return new DateRangeAggregatorBuilder(name);
    }

    /**
     * Create a new {@link IPv4RangeAggregatorBuilder} aggregation with the
     * given name.
     */
    public static IPv4RangeAggregatorBuilder ipRange(String name) {
        return new IPv4RangeAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Terms} aggregation with the given name.
     */
    public static TermsAggregatorBuilder terms(String name) {
        return new TermsAggregatorBuilder(name, null);
    }

    /**
     * Create a new {@link Percentiles} aggregation with the given name.
     */
    public static PercentilesAggregatorBuilder percentiles(String name) {
        return new PercentilesAggregatorBuilder(name);
    }

    /**
     * Create a new {@link PercentileRanks} aggregation with the given name.
     */
    public static PercentileRanksAggregatorBuilder percentileRanks(String name) {
        return new PercentileRanksAggregatorBuilder(name);
    }

    /**
     * Create a new {@link Cardinality} aggregation with the given name.
     */
    public static CardinalityAggregatorBuilder cardinality(String name) {
        return new CardinalityAggregatorBuilder(name, null);
    }

    /**
     * Create a new {@link TopHits} aggregation with the given name.
     */
    public static TopHitsAggregatorBuilder topHits(String name) {
        return new TopHitsAggregatorBuilder(name);
    }

    /**
     * Create a new {@link GeoBounds} aggregation with the given name.
     */
    public static GeoBoundsAggregatorBuilder geoBounds(String name) {
        return new GeoBoundsAggregatorBuilder(name);
    }

    /**
     * Create a new {@link GeoCentroid} aggregation with the given name.
     */
    public static GeoCentroidAggregatorBuilder geoCentroid(String name) {
        return new GeoCentroidAggregatorBuilder(name);
    }

    /**
     * Create a new {@link ScriptedMetric} aggregation with the given name.
     */
    public static ScriptedMetricAggregatorBuilder scriptedMetric(String name) {
        return new ScriptedMetricAggregatorBuilder(name);
    }
}
