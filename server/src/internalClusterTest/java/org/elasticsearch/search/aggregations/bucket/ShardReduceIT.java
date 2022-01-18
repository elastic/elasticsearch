/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGrid;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geohashGrid;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geotileGrid;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.ipRange;
import static org.elasticsearch.search.aggregations.AggregationBuilders.missing;
import static org.elasticsearch.search.aggregations.AggregationBuilders.nested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.range;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests making sure that the reduce is propagated to all aggregations in the hierarchy when executing on a single shard
 * These tests are based on the date histogram in combination of min_doc_count=0. In order for the date histogram to
 * compute empty buckets, its {@code reduce()} method must be called. So by adding the date histogram under other buckets,
 * we can make sure that the reduce is properly propagated by checking that empty buckets were created.
 */
@ESIntegTestCase.SuiteScopeTestCase
public class ShardReduceIT extends ESIntegTestCase {

    private IndexRequestBuilder indexDoc(String date, int value) throws Exception {
        return client().prepareIndex("idx")
            .setSource(
                jsonBuilder().startObject()
                    .field("value", value)
                    .field("ip", "10.0.0." + value)
                    .field("location", Geohash.stringEncode(5, 52, Geohash.PRECISION))
                    .field("date", date)
                    .field("term-l", 1)
                    .field("term-d", 1.5)
                    .field("term-s", "term")
                    .startObject("nested")
                    .field("date", date)
                    .endObject()
                    .endObject()
            );
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            prepareCreate("idx").setMapping(
                "nested",
                "type=nested",
                "ip",
                "type=ip",
                "location",
                "type=geo_point",
                "term-s",
                "type=keyword"
            )
        );

        indexRandom(true, indexDoc("2014-01-01", 1), indexDoc("2014-01-02", 2), indexDoc("2014-01-04", 3));
        ensureSearchable();
    }

    public void testGlobal() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                global("global").subAggregation(
                    dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0)
                )
            )
            .get();

        assertSearchResponse(response);

        Global global = response.getAggregations().get("global");
        Histogram histo = global.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testFilter() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                filter("filter", QueryBuilders.matchAllQuery()).subAggregation(
                    dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0)
                )
            )
            .get();

        assertSearchResponse(response);

        Filter filter = response.getAggregations().get("filter");
        Histogram histo = filter.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testMissing() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                missing("missing").field("foobar")
                    .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Missing missing = response.getAggregations().get("missing");
        Histogram histo = missing.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testGlobalWithFilterWithMissing() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                global("global").subAggregation(
                    filter("filter", QueryBuilders.matchAllQuery()).subAggregation(
                        missing("missing").field("foobar")
                            .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
                    )
                )
            )
            .get();

        assertSearchResponse(response);

        Global global = response.getAggregations().get("global");
        Filter filter = global.getAggregations().get("filter");
        Missing missing = filter.getAggregations().get("missing");
        Histogram histo = missing.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testNested() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                nested("nested", "nested").subAggregation(
                    dateHistogram("histo").field("nested.date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0)
                )
            )
            .get();

        assertSearchResponse(response);

        Nested nested = response.getAggregations().get("nested");
        Histogram histo = nested.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testStringTerms() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                terms("terms").field("term-s")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        Histogram histo = terms.getBucketByKey("term").getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testLongTerms() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                terms("terms").field("term-l")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        Histogram histo = terms.getBucketByKey("1").getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testDoubleTerms() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                terms("terms").field("term-d")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        Histogram histo = terms.getBucketByKey("1.5").getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testRange() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                range("range").field("value")
                    .addRange("r1", 0, 10)
                    .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        Histogram histo = range.getBuckets().get(0).getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testDateRange() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                dateRange("range").field("date")
                    .addRange("r1", "2014-01-01", "2014-01-10")
                    .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        Histogram histo = range.getBuckets().get(0).getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testIpRange() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                ipRange("range").field("ip")
                    .addRange("r1", "10.0.0.1", "10.0.0.10")
                    .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        Histogram histo = range.getBuckets().get(0).getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testHistogram() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                histogram("topHisto").field("value")
                    .interval(5)
                    .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Histogram topHisto = response.getAggregations().get("topHisto");
        Histogram histo = topHisto.getBuckets().get(0).getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testDateHistogram() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                dateHistogram("topHisto").field("date")
                    .calendarInterval(DateHistogramInterval.MONTH)
                    .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Histogram topHisto = response.getAggregations().get("topHisto");
        Histogram histo = topHisto.getBuckets().iterator().next().getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));

    }

    public void testGeoHashGrid() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                geohashGrid("grid").field("location")
                    .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        GeoGrid grid = response.getAggregations().get("grid");
        Histogram histo = grid.getBuckets().iterator().next().getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testGeoTileGrid() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                geotileGrid("grid").field("location")
                    .subAggregation(dateHistogram("histo").field("date").fixedInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        GeoGrid grid = response.getAggregations().get("grid");
        Histogram histo = grid.getBuckets().iterator().next().getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

}
