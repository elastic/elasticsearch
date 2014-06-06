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

import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;
import org.elasticsearch.search.aggregations.bucket.global.GlobalBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.MissingBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IPv4RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.StatsBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountBuilder;

/**
 *
 */
public class AggregationBuilders {

    protected AggregationBuilders() {
    }

    public static ValueCountBuilder count(String name) {
        return new ValueCountBuilder(name);
    }

    public static AvgBuilder avg(String name) {
        return new AvgBuilder(name);
    }

    public static MaxBuilder max(String name) {
        return new MaxBuilder(name);
    }

    public static MinBuilder min(String name) {
        return new MinBuilder(name);
    }

    public static SumBuilder sum(String name) {
        return new SumBuilder(name);
    }

    public static StatsBuilder stats(String name) {
        return new StatsBuilder(name);
    }

    public static ExtendedStatsBuilder extendedStats(String name) {
        return new ExtendedStatsBuilder(name);
    }

    public static FilterAggregationBuilder filter(String name) {
        return new FilterAggregationBuilder(name);
    }

    public static GlobalBuilder global(String name) {
        return new GlobalBuilder(name);
    }

    public static MissingBuilder missing(String name) {
        return new MissingBuilder(name);
    }

    public static NestedBuilder nested(String name) {
        return new NestedBuilder(name);
    }

    public static ReverseNestedBuilder reverseNested(String name) {
        return new ReverseNestedBuilder(name);
    }

    public static GeoDistanceBuilder geoDistance(String name) {
        return new GeoDistanceBuilder(name);
    }

    public static HistogramBuilder histogram(String name) {
        return new HistogramBuilder(name);
    }
    
    public static GeoHashGridBuilder geohashGrid(String name) {
        return new GeoHashGridBuilder(name);
    }

    public static SignificantTermsBuilder significantTerms(String name) {
        return new SignificantTermsBuilder(name);
    }

    public static DateHistogramBuilder dateHistogram(String name) {
        return new DateHistogramBuilder(name);
    }

    public static RangeBuilder range(String name) {
        return new RangeBuilder(name);
    }

    public static DateRangeBuilder dateRange(String name) {
        return new DateRangeBuilder(name);
    }

    public static IPv4RangeBuilder ipRange(String name) {
        return new IPv4RangeBuilder(name);
    }

    public static TermsBuilder terms(String name) {
        return new TermsBuilder(name);
    }

    public static PercentilesBuilder percentiles(String name) {
        return new PercentilesBuilder(name);
    }

    public static PercentileRanksBuilder percentileRanks(String name) {
        return new PercentileRanksBuilder(name);
    }

    public static CardinalityBuilder cardinality(String name) {
        return new CardinalityBuilder(name);
    }

    public static TopHitsBuilder topHits(String name) {
        return new TopHitsBuilder(name);
    }
    
    public static GeoBoundsBuilder geoBounds(String name) {
        return new GeoBoundsBuilder(name);
    }
}
