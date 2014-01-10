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

import com.google.common.base.Strings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class StringTermsTests extends ElasticsearchIntegrationTest {


    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
                .put("index.number_of_shards", between(1, 5))
                .put("index.number_of_replicas", between(0, 1))
                .build();
    }

    private String randomExecutionHint() {
        return randomFrom(Arrays.asList(null, TermsAggregatorFactory.EXECUTION_HINT_VALUE_MAP, TermsAggregatorFactory.EXECUTION_HINT_VALUE_ORDINALS));
    }

    @Before
    public void init() throws Exception {
        createIndex("idx");
        IndexRequestBuilder[] lowCardBuilders = new IndexRequestBuilder[5]; // TODO randomize the size?
        for (int i = 0; i < lowCardBuilders.length; i++) {
            lowCardBuilders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", "val" + i)
                    .field("i", i)
                    .startArray("values").value("val" + i).value("val" + (i + 1)).endArray()
                    .endObject());
        }
        indexRandom(true, lowCardBuilders);
        IndexRequestBuilder[] highCardBuilders = new IndexRequestBuilder[100]; // TODO randomize the size?

        for (int i = 0; i < highCardBuilders.length; i++) {
            highCardBuilders[i] = client().prepareIndex("idx", "high_card_type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", "val" + Strings.padStart(i+"", 3, '0'))
                    .startArray("values").value("val" + Strings.padStart(i+"", 3, '0')).value("val" + Strings.padStart((i+1)+"", 3, '0')).endArray()
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
                        .executionHint(randomExecutionHint())
                        .field("value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void singleValueField_WithRegexFiltering() throws Exception {

        // include without exclude
        // we should be left with: val000, val001, val002, val003, val004, val005, val006, val007, val008, val009

        SearchResponse response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .field("value").include("val00.+"))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(10));

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = terms.getByTerm("val00" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val00" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }

        // include and exclude
        // we should be left with: val002, val003, val004, val005, val006, val007, val008, val009

        response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .field("value").include("val00.+").exclude("(val000|val001)"))
                .execute().actionGet();

        assertSearchResponse(response);

        terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(8));

        for (int i = 2; i < 10; i++) {
            Terms.Bucket bucket = terms.getByTerm("val00" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val00" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }

        // exclude without include
        // we should be left with: val000, val001, val002, val003, val004, val005, val006, val007, val008, val009

        response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .field("value").exclude("val0[1-9]+.+"))
                .execute().actionGet();

        assertSearchResponse(response);

        terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(10));

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = terms.getByTerm("val00" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val00" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void singleValueField_WithRegexFiltering_WithFlags() throws Exception {

        // include without exclude
        // we should be left with: val000, val001, val002, val003, val004, val005, val006, val007, val008, val009
        // with case insensitive flag on the include regex

        SearchResponse response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .field("value").include("VAL00.+", Pattern.CASE_INSENSITIVE))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(10));

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = terms.getByTerm("val00" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val00" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }

        // include and exclude
        // we should be left with: val002, val003, val004, val005, val006, val007, val008, val009
        // with multi-flag masking on the exclude regex

        response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .field("value").include("val00.+").exclude("( val000 | VAL001 )#this is a comment", Pattern.CASE_INSENSITIVE | Pattern.COMMENTS))
                .execute().actionGet();

        assertSearchResponse(response);

        terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(8));

        for (int i = 2; i < 10; i++) {
            Terms.Bucket bucket = terms.getByTerm("val00" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val00" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }

        // exclude without include
        // we should be left with: val000, val001, val002, val003, val004, val005, val006, val007, val008, val009
        // with a "no flag" flag

        response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .field("value").exclude("val0[1-9]+.+", 0))
                .execute().actionGet();

        assertSearchResponse(response);

        terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(10));

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = terms.getByTerm("val00" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val00" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }


    @Test
    public void singleValueField_WithMaxSize() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value")
                        .size(20)
                        .order(Terms.Order.term(true))) // we need to sort by terms cause we're checking the first 20 values
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(20));

        for (int i = 0; i < 20; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + Strings.padStart(i+"", 3, '0'));
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + Strings.padStart(i+"", 3, '0')));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void singleValueField_OrderedByTermAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value")
                        .order(Terms.Order.term(true)))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.buckets()) {
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            i++;
        }
    }

    @Test
    public void singleValueField_OrderedByTermDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value")
                        .order(Terms.Order.term(false)))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        int i = 4;
        for (Terms.Bucket bucket : terms.buckets()) {
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value")
                        .subAggregation(count("count").field("values")))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            ValueCount valueCount = bucket.getAggregations().get("count");
            assertThat(valueCount, notNullValue());
            assertThat(valueCount.getValue(), equalTo(2l));
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value")
                        .subAggregation(count("count")))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            ValueCount valueCount = bucket.getAggregations().get("count");
            assertThat(valueCount, notNullValue());
            assertThat(valueCount.getValue(), equalTo(1l));
        }
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value")
                        .script("'foo_' + _value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("foo_val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void multiValuedField_WithValueScript_NotUnique() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("values")
                        .script("_value.substring(0,3)"))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(1));

        Terms.Bucket bucket = terms.getByTerm("val");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey().string(), equalTo("val"));
        assertThat(bucket.getDocCount(), equalTo(5l));
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("values"))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
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
                        .executionHint(randomExecutionHint())
                        .field("values")
                        .script("'foo_' + _value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("foo_val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    /*

    [foo_val0, foo_val1]
    [foo_val1, foo_val2]
    [foo_val2, foo_val3]
    [foo_val3, foo_val4]
    [foo_val4, foo_val5]


    foo_val0 - doc_count: 1 - val_count: 2
    foo_val1 - doc_count: 2 - val_count: 4
    foo_val2 - doc_count: 2 - val_count: 4
    foo_val3 - doc_count: 2 - val_count: 4
    foo_val4 - doc_count: 2 - val_count: 4
    foo_val5 - doc_count: 1 - val_count: 2

    */

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("values")
                        .script("'foo_' + _value")
                        .subAggregation(count("count")))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("foo_val" + i));
            if (i == 0 | i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
                ValueCount valueCount = bucket.getAggregations().get("count");
                assertThat(valueCount, notNullValue());
                assertThat(valueCount.getValue(), equalTo(2l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
                ValueCount valueCount = bucket.getAggregations().get("count");
                assertThat(valueCount, notNullValue());
                assertThat("term[" + bucket.getKey().string() + "]", valueCount.getValue(), equalTo(4l));
            }
        }
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .script("doc['value'].value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void script_SingleValue_ExplicitSingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .script("doc['value'].value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .script("doc['value'].value")
                        .subAggregation(count("count")))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            ValueCount valueCount = bucket.getAggregations().get("count");
            assertThat(valueCount, notNullValue());
            assertThat(valueCount.getValue(), equalTo(1l));
        }
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .script("doc['values'].values"))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .script("doc['values'].values")
                        .subAggregation(count("count")))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            if (i == 0 | i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
                ValueCount valueCount = bucket.getAggregations().get("count");
                assertThat(valueCount, notNullValue());
                assertThat(valueCount.getValue(), equalTo(2l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
                ValueCount valueCount = bucket.getAggregations().get("count");
                assertThat(valueCount, notNullValue());
                assertThat(valueCount.getValue(), equalTo(4l));
            }
        }
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
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
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void stringTermsNestedIntoPerBucketAggregator() throws Exception {
        // no execution hint so that the logic that decides whether or not to use ordinals is executed
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(filter("filter").filter(termFilter("values", "val3")).subAggregation(terms("terms").field("values")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Filter filter = response.getAggregations().get("filter");

        Terms terms = filter.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(3));

        for (int i = 2; i <= 4; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(i == 3 ? 2L : 1L));
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

    @Test
    public void singleValuedField_OrderedBySingleValueSubAggregationAsc() throws Exception {
        boolean asc = true;
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value")
                        .order(Terms.Order.aggregation("avg_i", asc))
                        .subAggregation(avg("avg_i").field("i"))
                ).execute().actionGet();


        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Avg avg = bucket.getAggregations().get("avg_i");
            assertThat(avg, notNullValue());
            assertThat(avg.getValue(), equalTo((double) i));
        }
    }

    @Test
    public void singleValuedField_OrderedByMissingSubAggregation() throws Exception {

        try {

            client().prepareSearch("idx").setTypes("type")
                    .addAggregation(terms("terms")
                            .executionHint(randomExecutionHint())
                            .field("value")
                            .order(Terms.Order.aggregation("avg_i", true))
                    ).execute().actionGet();

            fail("Expected search to fail when trying to sort terms aggregation by sug-aggregation that doesn't exist");

        } catch (ElasticsearchException e) {
            // expected
        }
    }

    @Test
    public void singleValuedField_OrderedByNonMetricsSubAggregation() throws Exception {

        try {

            client().prepareSearch("idx").setTypes("type")
                    .addAggregation(terms("terms")
                            .executionHint(randomExecutionHint())
                            .field("value")
                            .order(Terms.Order.aggregation("filter", true))
                            .subAggregation(filter("filter").filter(FilterBuilders.termFilter("foo", "bar")))
                    ).execute().actionGet();

            fail("Expected search to fail when trying to sort terms aggregation by sug-aggregation which is not of a metrics type");

        } catch (ElasticsearchException e) {
            // expected
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregation_WithUknownMetric() throws Exception {

        try {

            client().prepareSearch("idx").setTypes("type")
                    .addAggregation(terms("terms")
                            .executionHint(randomExecutionHint())
                            .field("value")
                            .order(Terms.Order.aggregation("stats.foo", true))
                            .subAggregation(stats("stats").field("i"))
                    ).execute().actionGet();

            fail("Expected search to fail when trying to sort terms aggregation by multi-valued sug-aggregation " +
                    "with an unknown specified metric to order by");

        } catch (ElasticsearchException e) {
            // expected
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregation_WithoutMetric() throws Exception {

        try {

            client().prepareSearch("idx").setTypes("type")
                    .addAggregation(terms("terms")
                            .executionHint(randomExecutionHint())
                            .field("value")
                            .order(Terms.Order.aggregation("stats", true))
                            .subAggregation(stats("stats").field("i"))
                    ).execute().actionGet();

            fail("Expected search to fail when trying to sort terms aggregation by multi-valued sug-aggregation " +
                    "where the metric name is not specified");

        } catch (ElasticsearchException e) {
            // expected
        }
    }

    @Test
    public void singleValuedField_OrderedBySingleValueSubAggregationDesc() throws Exception {
        boolean asc = false;
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value")
                        .order(Terms.Order.aggregation("avg_i", asc))
                        .subAggregation(avg("avg_i").field("i"))
                ).execute().actionGet();


        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 4; i >= 0; i--) {

            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));

            Avg avg = bucket.getAggregations().get("avg_i");
            assertThat(avg, notNullValue());
            assertThat(avg.getValue(), equalTo((double) i));
        }

    }

    @Test
    public void singleValuedField_OrderedByMultiValueSubAggregationAsc() throws Exception {
        boolean asc = true;
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value")
                        .order(Terms.Order.aggregation("stats.avg", asc))
                        .subAggregation(stats("stats").field("i"))
                ).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));

            Stats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
        }

    }

    @Test
    public void singleValuedField_OrderedByMultiValueSubAggregationDesc() throws Exception {
        boolean asc = false;
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value")
                        .order(Terms.Order.aggregation("stats.avg", asc))
                        .subAggregation(stats("stats").field("i"))
                ).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 4; i >= 0; i--) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));

            Stats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
        }

    }

    @Test
    public void singleValuedField_OrderedByMultiValueExtendedStatsAsc() throws Exception {
        boolean asc = true;
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field("value")
                        .order(Terms.Order.aggregation("stats.variance", asc))
                        .subAggregation(extendedStats("stats").field("i"))
                ).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKey().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));

            ExtendedStats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
        }

    }
}
