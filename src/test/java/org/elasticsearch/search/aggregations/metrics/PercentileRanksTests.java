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
package org.elasticsearch.search.aggregations.metrics;

import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentileRanks;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class PercentileRanksTests extends AbstractNumericTests {

    private static double[] randomPercents(long minValue, long maxValue) {
        
        final int length = randomIntBetween(1, 20);
        final double[] percents = new double[length];
        for (int i = 0; i < percents.length; ++i) {
            switch (randomInt(20)) {
            case 0:
                percents[i] = minValue;
                break;
            case 1:
                percents[i] = maxValue;
                break;
            default:
                percents[i] = (randomDouble() * (maxValue - minValue)) + minValue;
                break;
            }
        }
        Arrays.sort(percents);
        Loggers.getLogger(PercentileRanksTests.class).info("Using percentiles={}", Arrays.toString(percents));
        return percents;
    }

    private static PercentileRanksBuilder randomCompression(PercentileRanksBuilder builder) {
        if (randomBoolean()) {
            builder.compression(randomIntBetween(20, 120) + randomDouble());
        }
        return builder;
    }

    private void assertConsistent(double[] pcts, PercentileRanks percentiles, long minValue, long maxValue) {
        final List<Percentile> percentileList = Lists.newArrayList(percentiles);
        assertEquals(pcts.length, percentileList.size());
        for (int i = 0; i < pcts.length; ++i) {
            final Percentile percentile = percentileList.get(i);
            assertThat(percentile.getValue(), equalTo(pcts[i]));
            assertThat(percentile.getPercent(), greaterThanOrEqualTo(0.0));
            assertThat(percentile.getPercent(), lessThanOrEqualTo(100.0));

            if (percentile.getPercent() == 0) {
                assertThat(percentile.getValue(), lessThanOrEqualTo((double) minValue));
            }
            if (percentile.getPercent() == 100) {
                assertThat(percentile.getValue(), greaterThanOrEqualTo((double) maxValue));
            }
        }

        for (int i = 1; i < percentileList.size(); ++i) {
            assertThat(percentileList.get(i).getValue(), greaterThanOrEqualTo(percentileList.get(i - 1).getValue()));
        }
    }

    @Test
    public void testEmptyAggregation() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0)
                        .subAggregation(randomCompression(percentileRanks("percentile_ranks"))
                                .percentiles(10, 15)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBucketByKey(1l);
        assertThat(bucket, notNullValue());

        PercentileRanks reversePercentiles = bucket.getAggregations().get("percentile_ranks");
        assertThat(reversePercentiles, notNullValue());
        assertThat(reversePercentiles.getName(), equalTo("percentile_ranks"));
        assertThat(reversePercentiles.percent(10), equalTo(Double.NaN));
        assertThat(reversePercentiles.percent(15), equalTo(Double.NaN));
    }

    @Test
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .field("value")
                        .percentiles(0, 10, 15, 100))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        PercentileRanks reversePercentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertThat(reversePercentiles, notNullValue());
        assertThat(reversePercentiles.getName(), equalTo("percentile_ranks"));
        assertThat(reversePercentiles.percent(0), equalTo(Double.NaN));
        assertThat(reversePercentiles.percent(10), equalTo(Double.NaN));
        assertThat(reversePercentiles.percent(15), equalTo(Double.NaN));
        assertThat(reversePercentiles.percent(100), equalTo(Double.NaN));
    }

    @Test
    public void testSingleValuedField() throws Exception {
        final double[] pcts = randomPercents(minValue, maxValue);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .field("value")
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValue, maxValue);
    }

    @Test
    public void testSingleValuedFieldOutsideRange() throws Exception {
        final double[] pcts = new double[] {minValue - 1, maxValue + 1};
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .field("value")
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValue, maxValue);
    }

    @Test
    public void testSingleValuedField_PartiallyUnmapped() throws Exception {
        final double[] pcts = randomPercents(minValue, maxValue);
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .field("value")
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValue, maxValue);
    }

    @Test
    public void testSingleValuedField_WithValueScript() throws Exception {
        final double[] pcts = randomPercents(minValue - 1, maxValue - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .field("value").script("_value - 1")
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValue - 1, maxValue - 1);
    }

    @Test
    public void testSingleValuedField_WithValueScript_WithParams() throws Exception {
        final double[] pcts = randomPercents(minValue - 1, maxValue - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .field("value").script("_value - dec").param("dec", 1)
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValue - 1, maxValue - 1);
    }

    @Test
    public void testMultiValuedField() throws Exception {
        final double[] pcts = randomPercents(minValues, maxValues);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .field("values")
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValues, maxValues);
    }

    @Test
    public void testMultiValuedField_WithValueScript() throws Exception {
        final double[] pcts = randomPercents(minValues - 1, maxValues - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .field("values").script("_value - 1")
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValues - 1, maxValues - 1);
    }

    @Test
    public void testMultiValuedField_WithValueScript_Reverse() throws Exception {
        final double[] pcts = randomPercents(-maxValues, -minValues);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .field("values").script("_value * -1")
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, -maxValues, -minValues);
    }

    @Test
    public void testMultiValuedField_WithValueScript_WithParams() throws Exception {
        final double[] pcts = randomPercents(minValues - 1, maxValues - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .field("values").script("_value - dec").param("dec", 1)
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValues - 1, maxValues - 1);
    }

    @Test
    public void testScript_SingleValued() throws Exception {
        final double[] pcts = randomPercents(minValue, maxValue);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .script("doc['value'].value")
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValue, maxValue);
    }

    @Test
    public void testScript_SingleValued_WithParams() throws Exception {
        final double[] pcts = randomPercents(minValue - 1, maxValue - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .script("doc['value'].value - dec").param("dec", 1)
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValue - 1, maxValue - 1);
    }

    @Test
    public void testScript_ExplicitSingleValued_WithParams() throws Exception {
        final double[] pcts = randomPercents(minValue -1 , maxValue - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .script("doc['value'].value - dec").param("dec", 1)
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValue - 1, maxValue - 1);
    }

    @Test
    public void testScript_MultiValued() throws Exception {
        final double[] pcts = randomPercents(minValues, maxValues);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .script("doc['values'].values")
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValues, maxValues);
    }

    @Test
    public void testScript_ExplicitMultiValued() throws Exception {
        final double[] pcts = randomPercents(minValues, maxValues);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .script("doc['values'].values")
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValues, maxValues);
    }

    @Test
    public void testScript_MultiValued_WithParams() throws Exception {
        final double[] pcts = randomPercents(minValues - 1, maxValues - 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks"))
                        .script("List values = doc['values'].values; double[] res = new double[values.length]; for (int i = 0; i < res.length; i++) { res[i] = values.get(i) - dec; }; return res;").param("dec", 1)
                        .percentiles(pcts))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        final PercentileRanks percentiles = searchResponse.getAggregations().get("percentile_ranks");
        assertConsistent(pcts, percentiles, minValues - 1, maxValues - 1);
    }

    @Test
    public void testOrderBySubAggregation() {
        boolean asc = randomBoolean();
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        histogram("histo").field("value").interval(2l)
                            .subAggregation(randomCompression(percentileRanks("percentile_ranks").percentiles(99)))
                            .order(Order.aggregation("percentile_ranks", "99", asc)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Histogram histo = searchResponse.getAggregations().get("histo");
        double previous = asc ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            PercentileRanks percentiles = bucket.getAggregations().get("percentile_ranks");
            double p99 = percentiles.percent(99);
            if (asc) {
                assertThat(p99, greaterThanOrEqualTo(previous));
            } else {
                assertThat(p99, lessThanOrEqualTo(previous));
            }
            previous = p99;
        }
    }

}