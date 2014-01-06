/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class LongTermsTests extends ElasticsearchIntegrationTest {

    private static final int NUM_DOCS = 5; // TODO randomize the size?

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
        IndexRequestBuilder[] lowCardBuilders = new IndexRequestBuilder[NUM_DOCS];
        for (int i = 0; i < lowCardBuilders.length; i++) {
            lowCardBuilders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", i)
                    .startArray("values").value(i).value(i + 1).endArray()
                    .endObject());
        }
        indexRandom(randomBoolean(), lowCardBuilders);
        IndexRequestBuilder[] highCardBuilders = new IndexRequestBuilder[100]; // TODO randomize the size?
        for (int i = 0; i < highCardBuilders.length; i++) {
           highCardBuilders[i] = client().prepareIndex("idx", "high_card_type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", i)
                    .startArray("values").value(i).value(i + 1).endArray()
                    .endObject());
                    
        }
        indexRandom(true, highCardBuilders);
        createIndex("idx_unmapped");
        ensureSearchable();
    }

    @Test
    public void singleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void singleValueField_WithMaxSize() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .field("value")
                        .size(20)
                        .order(Terms.Order.TERM_ASC)) // we need to sort by terms cause we're checking the first 20 values
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(20));

        for (int i = 0; i < 20; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void singleValueField_OrderedByTermAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .order(Terms.Order.TERM_ASC))
                .execute().actionGet();
        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.buckets()) {
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            i++;
        }
    }

    @Test
    public void singleValueField_OrderedByTermDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .order(Terms.Order.TERM_DESC))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        int i = 4;
        for (Terms.Bucket bucket : terms.buckets()) {
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .subAggregation(sum("sum").field("values")))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat((long) sum.getValue(), equalTo(i+i+1l));
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo((double) i));
        }
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .script("_value + 1"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (i+1d));
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + (i+1d)));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i+1));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("values"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("values")
                        .script("_value - 1"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (i-1d));
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + (i-1d)));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i-1));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    @Test
    public void multiValuedField_WithValueScript_NotUnique() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("values")
                        .script("floor(_value / 1000 + 1)"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(1));

        Terms.Bucket bucket = terms.getByTerm("1.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey().string(), equalTo("1.0"));
        assertThat(bucket.getKeyAsNumber().intValue(), equalTo(1));
        assertThat(bucket.getDocCount(), equalTo(5l));
    }

    /*

    [1, 2]
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]

    1 - count: 1 - sum: 1
    2 - count: 2 - sum: 4
    3 - count: 2 - sum: 6
    4 - count: 2 - sum: 8
    5 - count: 2 - sum: 10
    6 - count: 1 - sum: 6

    */

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("values")
                        .script("_value + 1")
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (i+1d));
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + (i+1d)));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i+1));
            final long count = i == 0 || i == 5 ? 1 : 2;
            double s = 0;
            for (int j = 0; j < NUM_DOCS; ++j) {
                if (i == j || i == j+1) {
                    s += j + 1;
                    s += j+1 + 1;
                }
            }
            assertThat(bucket.getDocCount(), equalTo(count));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo(s));
        }
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .script("doc['value'].value"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo((double) i));
        }
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .script("doc['values'].values"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited_NoExplicitType() throws Exception {

        // since no type ie explicitly defined, es will assume all values returned by the script to be strings (bytes),
        // so the aggregation should fail, since the "sum" aggregation can only operation on numeric values.

        try {

            SearchResponse response = client().prepareSearch("idx").setTypes("type")
                    .addAggregation(terms("terms")
                            .script("doc['values'].values")
                            .subAggregation(sum("sum")))
                    .execute().actionGet();

            fail("expected to fail as sub-aggregation sum requires a numeric value source context, but there is none");

        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited_WithExplicitType() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .script("doc['values'].values")
                        .valueType(Terms.ValueType.LONG)
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            final long count = i == 0 || i == 5 ? 1 : 2;
            double s = 0;
            for (int j = 0; j < NUM_DOCS; ++j) {
                if (i == j || i == j+1) {
                    s += j;
                    s += j+1;
                }
            }
            assertThat(bucket.getDocCount(), equalTo(count));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo(s));
        }
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped", "idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("" + i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void emptyAggregation() throws Exception {
        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer").execute().actionGet();
        List<IndexRequestBuilder> builders = new ArrayList<IndexRequestBuilder>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i*2)
                    .endObject()));
        }
        indexRandom(true, builders.toArray(new IndexRequestBuilder[builders.size()]));

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).emptyBuckets(true)
                        .subAggregation(terms("terms")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getByKey(1l);
        assertThat(bucket, Matchers.notNullValue());

        Terms terms = bucket.getAggregations().get("terms");
        assertThat(terms, Matchers.notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().isEmpty(), is(true));

    }

}
