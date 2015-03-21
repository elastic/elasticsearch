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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.util.Comparators;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.core.IsNull.notNullValue;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class NaNSortingTests extends ElasticsearchIntegrationTest {

    private enum SubAggregation {
        AVG("avg") {
            @Override
            public MetricsAggregationBuilder<?> builder() {
                return avg(name).field("numeric_field");
            }
            @Override
            public double getValue(Aggregation aggregation) {
                return ((Avg) aggregation).getValue();
            }
        },
        VARIANCE("variance") {
            @Override
            public MetricsAggregationBuilder<?> builder() {
                return extendedStats(name).field("numeric_field");
            }
            @Override
            public String sortKey() {
                return name + ".variance";
            }
            @Override
            public double getValue(Aggregation aggregation) {
                return ((ExtendedStats) aggregation).getVariance();
            }
        },
        STD_DEVIATION("std_deviation"){
            @Override
            public MetricsAggregationBuilder<?> builder() {
                return extendedStats(name).field("numeric_field");
            }
            @Override
            public String sortKey() {
                return name + ".std_deviation";
            }
            @Override
            public double getValue(Aggregation aggregation) {
                return ((ExtendedStats) aggregation).getStdDeviation();
            }
        };

        SubAggregation(String name) {
            this.name = name;
        }

        public String name;

        public abstract MetricsAggregationBuilder<?> builder();

        public String sortKey() {
            return name;
        }

        public abstract double getValue(Aggregation aggregation);
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        final int numDocs = randomIntBetween(2, 10);
        for (int i = 0; i < numDocs; ++i) {
            final long value = randomInt(5);
            XContentBuilder source = jsonBuilder().startObject().field("long_value", value).field("double_value", value + 0.05).field("string_value", "str_" + value);
            if (randomBoolean()) {
                source.field("numeric_value", randomDouble());
            }
            client().prepareIndex("idx", "type").setSource(source.endObject()).execute().actionGet();
        }
        refresh();
        ensureSearchable();
    }

    private void assertCorrectlySorted(Terms terms, boolean asc, SubAggregation agg) {
        assertThat(terms, notNullValue());
        double previousValue = asc ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Aggregation sub = bucket.getAggregations().get(agg.name);
            double value = agg.getValue(sub);
            assertTrue(Comparators.compareDiscardNaN(previousValue, value, asc) <= 0);
            previousValue = value;
        }
    }

    private void assertCorrectlySorted(Histogram histo, boolean asc, SubAggregation agg) {
        assertThat(histo, notNullValue());
        double previousValue = asc ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            Aggregation sub = bucket.getAggregations().get(agg.name);
            double value = agg.getValue(sub);
            assertTrue(Comparators.compareDiscardNaN(previousValue, value, asc) <= 0);
            previousValue = value;
        }
    }

    public void testTerms(String fieldName) {
        final boolean asc = randomBoolean();
        SubAggregation agg = randomFrom(SubAggregation.values());
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms").field(fieldName).collectMode(randomFrom(SubAggCollectionMode.values())).subAggregation(agg.builder()).order(Terms.Order.aggregation(agg.sortKey(), asc)))
                .execute().actionGet();

        assertSearchResponse(response);
        final Terms terms = response.getAggregations().get("terms");
        assertCorrectlySorted(terms, asc, agg);
    }

    @Test
    public void stringTerms() {
        testTerms("string_value");
    }

    @Test
    public void longTerms() {
        testTerms("long_value");
    }

    @Test
    public void doubleTerms() {
        testTerms("double_value");
    }

    @Test
    public void longHistogram() {
        final boolean asc = randomBoolean();
        SubAggregation agg = randomFrom(SubAggregation.values());
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo")
                        .field("long_value").interval(randomIntBetween(1, 2)).subAggregation(agg.builder()).order(Histogram.Order.aggregation(agg.sortKey(), asc)))
                .execute().actionGet();

        assertSearchResponse(response);
        final Histogram histo = response.getAggregations().get("histo");
        assertCorrectlySorted(histo, asc, agg);
    }

}
