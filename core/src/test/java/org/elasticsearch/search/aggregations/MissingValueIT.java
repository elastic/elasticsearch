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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.search.aggregations.AggregationBuilders.cardinality;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geoBounds;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

@ESIntegTestCase.SuiteScopeTestCase
public class MissingValueIT extends ESIntegTestCase {

    @Override
    protected int maximumNumberOfShards() {
        return 2;
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").addMapping("type", "date", "type=date", "location", "type=geo_point").get());
        indexRandom(true,
                client().prepareIndex("idx", "type", "1").setSource(),
                client().prepareIndex("idx", "type", "2").setSource("str", "foo", "long", 3L, "double", 5.5, "date", "2015-05-07", "location", "1,2"));
    }

    public void testUnmappedTerms() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("non_existing_field").missing("bar")).get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(1, terms.getBuckets().size());
        assertEquals(2, terms.getBucketByKey("bar").getDocCount());
    }

    public void testStringTerms() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("str").missing("bar")).get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(2, terms.getBuckets().size());
        assertEquals(1, terms.getBucketByKey("foo").getDocCount());
        assertEquals(1, terms.getBucketByKey("bar").getDocCount());

        response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("str").missing("foo")).get();
        assertSearchResponse(response);
        terms = response.getAggregations().get("my_terms");
        assertEquals(1, terms.getBuckets().size());
        assertEquals(2, terms.getBucketByKey("foo").getDocCount());
    }

    public void testLongTerms() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("long").missing(4)).get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(2, terms.getBuckets().size());
        assertEquals(1, terms.getBucketByKey("3").getDocCount());
        assertEquals(1, terms.getBucketByKey("4").getDocCount());

        response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("long").missing(3)).get();
        assertSearchResponse(response);
        terms = response.getAggregations().get("my_terms");
        assertEquals(1, terms.getBuckets().size());
        assertEquals(2, terms.getBucketByKey("3").getDocCount());
    }

    public void testDoubleTerms() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("double").missing(4.5)).get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(2, terms.getBuckets().size());
        assertEquals(1, terms.getBucketByKey("4.5").getDocCount());
        assertEquals(1, terms.getBucketByKey("5.5").getDocCount());

        response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("double").missing(5.5)).get();
        assertSearchResponse(response);
        terms = response.getAggregations().get("my_terms");
        assertEquals(1, terms.getBuckets().size());
        assertEquals(2, terms.getBucketByKey("5.5").getDocCount());
    }

    public void testUnmappedHistogram() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(histogram("my_histogram").field("non-existing_field").interval(5).missing(12)).get();
        assertSearchResponse(response);
        Histogram histogram = response.getAggregations().get("my_histogram");
        assertEquals(1, histogram.getBuckets().size());
        assertEquals(10L, histogram.getBuckets().get(0).getKey());
        assertEquals(2, histogram.getBuckets().get(0).getDocCount());
    }

    public void testHistogram() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(histogram("my_histogram").field("long").interval(5).missing(7)).get();
        assertSearchResponse(response);
        Histogram histogram = response.getAggregations().get("my_histogram");
        assertEquals(2, histogram.getBuckets().size());
        assertEquals(0L, histogram.getBuckets().get(0).getKey());
        assertEquals(1, histogram.getBuckets().get(0).getDocCount());
        assertEquals(5L, histogram.getBuckets().get(1).getKey());
        assertEquals(1, histogram.getBuckets().get(1).getDocCount());

        response = client().prepareSearch("idx").addAggregation(histogram("my_histogram").field("long").interval(5).missing(3)).get();
        assertSearchResponse(response);
        histogram = response.getAggregations().get("my_histogram");
        assertEquals(1, histogram.getBuckets().size());
        assertEquals(0L, histogram.getBuckets().get(0).getKey());
        assertEquals(2, histogram.getBuckets().get(0).getDocCount());
    }

    public void testDateHistogram() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(dateHistogram("my_histogram").field("date").interval(DateHistogramInterval.YEAR).missing("2014-05-07")).get();
        assertSearchResponse(response);
        Histogram histogram = response.getAggregations().get("my_histogram");
        assertEquals(2, histogram.getBuckets().size());
        assertEquals("2014-01-01T00:00:00.000Z", histogram.getBuckets().get(0).getKeyAsString());
        assertEquals(1, histogram.getBuckets().get(0).getDocCount());
        assertEquals("2015-01-01T00:00:00.000Z", histogram.getBuckets().get(1).getKeyAsString());
        assertEquals(1, histogram.getBuckets().get(1).getDocCount());

        response = client().prepareSearch("idx").addAggregation(dateHistogram("my_histogram").field("date").interval(DateHistogramInterval.YEAR).missing("2015-05-07")).get();
        assertSearchResponse(response);
        histogram = response.getAggregations().get("my_histogram");
        assertEquals(1, histogram.getBuckets().size());
        assertEquals("2015-01-01T00:00:00.000Z", histogram.getBuckets().get(0).getKeyAsString());
        assertEquals(2, histogram.getBuckets().get(0).getDocCount());
    }

    public void testCardinality() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(cardinality("card").field("long").missing(2)).get();
        assertSearchResponse(response);
        Cardinality cardinality = response.getAggregations().get("card");
        assertEquals(2, cardinality.getValue());
    }

    public void testPercentiles() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(percentiles("percentiles").field("long").missing(1000)).get();
        assertSearchResponse(response);
        Percentiles percentiles = response.getAggregations().get("percentiles");
        assertEquals(1000, percentiles.percentile(100), 0);
    }

    public void testStats() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(stats("stats").field("long").missing(5)).get();
        assertSearchResponse(response);
        Stats stats = response.getAggregations().get("stats");
        assertEquals(2, stats.getCount());
        assertEquals(4, stats.getAvg(), 0);
    }

    public void testUnmappedGeoBounds() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(geoBounds("bounds").field("non_existing_field").missing("2,1")).get();
        assertSearchResponse(response);
        GeoBounds bounds = response.getAggregations().get("bounds");
        assertEquals(new GeoPoint(2,1), bounds.bottomRight());
        assertEquals(new GeoPoint(2,1), bounds.topLeft());
    }

    public void testGeoBounds() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(geoBounds("bounds").field("location").missing("2,1")).get();
        assertSearchResponse(response);
        GeoBounds bounds = response.getAggregations().get("bounds");
        assertEquals(new GeoPoint(1,2), bounds.bottomRight());
        assertEquals(new GeoPoint(2,1), bounds.topLeft());
    }

}
