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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Comparators;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.hamcrest.core.IsNull.notNullValue;

public class NaNSortingTests extends ElasticsearchIntegrationTest {

    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
                .put("index.number_of_shards", between(1, 5))
                .put("index.number_of_replicas", between(0, 1))
                .build();
    }

    @Before
    public void init() throws Exception {
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

    private void assertCorrectlySorted(Terms terms, boolean asc) {
        assertThat(terms, notNullValue());
        double previousAvg = asc ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Avg avg = bucket.getAggregations().get("avg");
            assertTrue(Comparators.compareDiscardNaN(previousAvg, avg.getValue(), asc) <= 0);
            previousAvg = avg.getValue();
        }
    }

    private void assertCorrectlySorted(Histogram histo, boolean asc) {
        assertThat(histo, notNullValue());
        double previousAvg = asc ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            Avg avg = bucket.getAggregations().get("avg");
            assertTrue(Comparators.compareDiscardNaN(previousAvg, avg.getValue(), asc) <= 0);
            previousAvg = avg.getValue();
        }
    }

    @Test
    public void stringTerms() {
        final boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                        .field("string_value").subAggregation(avg("avg").field("numeric_value")).order(Terms.Order.aggregation("avg", asc)))
                .execute().actionGet();

        final Terms terms = response.getAggregations().get("terms");
        assertCorrectlySorted(terms, asc);
    }

    @Test
    public void longTerms() {
        final boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                        .field("long_value").subAggregation(avg("avg").field("numeric_value")).order(Terms.Order.aggregation("avg", asc)))
                .execute().actionGet();

        final Terms terms = response.getAggregations().get("terms");
        assertCorrectlySorted(terms, asc);
    }

    @Test
    public void doubleTerms() {
        final boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                        .field("double_value").subAggregation(avg("avg").field("numeric_value")).order(Terms.Order.aggregation("avg", asc)))
                .execute().actionGet();

        final Terms terms = response.getAggregations().get("terms");
        assertCorrectlySorted(terms, asc);
    }

    @Test
    public void longHistogram() {
        final boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo")
                        .field("long_value").interval(randomIntBetween(1, 2)).subAggregation(avg("avg").field("numeric_value")).order(Histogram.Order.aggregation("avg", asc)))
                .execute().actionGet();

        final Histogram histo = response.getAggregations().get("histo");
        assertCorrectlySorted(histo, asc);
    }

}
