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
import org.elasticsearch.search.aggregations.bucket.children.ParentToChildrenAggregator;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregator;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridParser;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregator.DateHistogramFactory;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregator;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregator;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceParser.GeoDistanceFactory;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IPv4RangeAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.sampler.Sampler;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregator;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregator;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsAggregator;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidAggregator;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregator;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregator;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksAggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricAggregator;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregator;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregator;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregator;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregator;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregator;

/**
 * Utility class to create aggregations.
 */
public class AggregationBuilders {

    private AggregationBuilders() {
    }

    /**
     * Create a new {@link ValueCount} aggregation with the given name.
     */
    public static ValueCountAggregator.Factory count(String name) {
        return new ValueCountAggregator.Factory(name, null);
    }

    /**
     * Create a new {@link Avg} aggregation with the given name.
     */
    public static AvgAggregator.Factory avg(String name) {
        return new AvgAggregator.Factory(name);
    }

    /**
     * Create a new {@link Max} aggregation with the given name.
     */
    public static MaxAggregator.Factory max(String name) {
        return new MaxAggregator.Factory(name);
    }

    /**
     * Create a new {@link Min} aggregation with the given name.
     */
    public static MinAggregator.Factory min(String name) {
        return new MinAggregator.Factory(name);
    }

    /**
     * Create a new {@link Sum} aggregation with the given name.
     */
    public static SumAggregator.Factory sum(String name) {
        return new SumAggregator.Factory(name);
    }

    /**
     * Create a new {@link Stats} aggregation with the given name.
     */
    public static StatsAggregator.Factory stats(String name) {
        return new StatsAggregator.Factory(name);
    }

    /**
     * Create a new {@link ExtendedStats} aggregation with the given name.
     */
    public static ExtendedStatsAggregator.Factory extendedStats(String name) {
        return new ExtendedStatsAggregator.Factory(name);
    }

    /**
     * Create a new {@link Filter} aggregation with the given name.
     */
    public static FilterAggregator.Factory filter(String name, QueryBuilder<?> filter) {
        return new FilterAggregator.Factory(name, filter);
    }

    /**
     * Create a new {@link Filters} aggregation with the given name.
     */
    public static FiltersAggregator.Factory filters(String name, KeyedFilter... filters) {
        return new FiltersAggregator.Factory(name, filters);
    }

    /**
     * Create a new {@link Filters} aggregation with the given name.
     */
    public static FiltersAggregator.Factory filters(String name, QueryBuilder<?>... filters) {
        return new FiltersAggregator.Factory(name, filters);
    }

    /**
     * Create a new {@link Sampler} aggregation with the given name.
     */
    public static SamplerAggregator.Factory sampler(String name) {
        return new SamplerAggregator.Factory(name);
    }

    /**
     * Create a new {@link Sampler} aggregation with the given name.
     */
    public static SamplerAggregator.DiversifiedFactory diversifiedSampler(String name) {
        return new SamplerAggregator.DiversifiedFactory(name);
    }

    /**
     * Create a new {@link Global} aggregation with the given name.
     */
    public static GlobalAggregator.Factory global(String name) {
        return new GlobalAggregator.Factory(name);
    }

    /**
     * Create a new {@link Missing} aggregation with the given name.
     */
    public static MissingAggregator.Factory missing(String name) {
        return new MissingAggregator.Factory(name, null);
    }

    /**
     * Create a new {@link Nested} aggregation with the given name.
     */
    public static NestedAggregator.Factory nested(String name, String path) {
        return new NestedAggregator.Factory(name, path);
    }

    /**
     * Create a new {@link ReverseNested} aggregation with the given name.
     */
    public static ReverseNestedAggregator.Factory reverseNested(String name) {
        return new ReverseNestedAggregator.Factory(name);
    }

    /**
     * Create a new {@link Children} aggregation with the given name.
     */
    public static ParentToChildrenAggregator.Factory children(String name, String childType) {
        return new ParentToChildrenAggregator.Factory(name, childType);
    }

    /**
     * Create a new {@link GeoDistance} aggregation with the given name.
     */
    public static GeoDistanceFactory geoDistance(String name, GeoPoint origin) {
        return new GeoDistanceFactory(name, origin);
    }

    /**
     * Create a new {@link Histogram} aggregation with the given name.
     */
    public static HistogramAggregator.Factory<?> histogram(String name) {
        return new HistogramAggregator.Factory<>(name);
    }

    /**
     * Create a new {@link GeoHashGrid} aggregation with the given name.
     */
    public static GeoHashGridParser.GeoGridFactory geohashGrid(String name) {
        return new GeoHashGridParser.GeoGridFactory(name);
    }

    /**
     * Create a new {@link SignificantTerms} aggregation with the given name.
     */
    public static SignificantTermsAggregatorFactory significantTerms(String name) {
        return new SignificantTermsAggregatorFactory(name, null);
    }

    /**
     * Create a new {@link DateHistogramFactory} aggregation with the given
     * name.
     */
    public static DateHistogramFactory dateHistogram(String name) {
        return new DateHistogramFactory(name);
    }

    /**
     * Create a new {@link Range} aggregation with the given name.
     */
    public static RangeAggregator.Factory range(String name) {
        return new RangeAggregator.Factory(name);
    }

    /**
     * Create a new {@link DateRangeAggregatorFactory} aggregation with the
     * given name.
     */
    public static DateRangeAggregatorFactory dateRange(String name) {
        return new DateRangeAggregatorFactory(name);
    }

    /**
     * Create a new {@link IPv4RangeAggregatorFactory} aggregation with the
     * given name.
     */
    public static IPv4RangeAggregatorFactory ipRange(String name) {
        return new IPv4RangeAggregatorFactory(name);
    }

    /**
     * Create a new {@link Terms} aggregation with the given name.
     */
    public static TermsAggregatorFactory terms(String name) {
        return new TermsAggregatorFactory(name, null);
    }

    /**
     * Create a new {@link Percentiles} aggregation with the given name.
     */
    public static PercentilesAggregatorFactory percentiles(String name) {
        return new PercentilesAggregatorFactory(name);
    }

    /**
     * Create a new {@link PercentileRanks} aggregation with the given name.
     */
    public static PercentileRanksAggregatorFactory percentileRanks(String name) {
        return new PercentileRanksAggregatorFactory(name);
    }

    /**
     * Create a new {@link Cardinality} aggregation with the given name.
     */
    public static CardinalityAggregatorFactory cardinality(String name) {
        return new CardinalityAggregatorFactory(name, null);
    }

    /**
     * Create a new {@link TopHits} aggregation with the given name.
     */
    public static TopHitsAggregator.Factory topHits(String name) {
        return new TopHitsAggregator.Factory(name);
    }

    /**
     * Create a new {@link GeoBounds} aggregation with the given name.
     */
    public static GeoBoundsAggregator.Factory geoBounds(String name) {
        return new GeoBoundsAggregator.Factory(name);
    }

    /**
     * Create a new {@link GeoCentroid} aggregation with the given name.
     */
    public static GeoCentroidAggregator.Factory geoCentroid(String name) {
        return new GeoCentroidAggregator.Factory(name);
    }

    /**
     * Create a new {@link ScriptedMetric} aggregation with the given name.
     */
    public static ScriptedMetricAggregator.Factory scriptedMetric(String name) {
        return new ScriptedMetricAggregator.Factory(name);
    }
}
