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
package org.elasticsearch.messy.tests;

import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.AbstractTermsTestCase;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.count;
import static org.elasticsearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

/**
 *
 */
@ESIntegTestCase.SuiteScopeTestCase
public class StringTermsTests extends AbstractTermsTestCase {
    private static final String SINGLE_VALUED_FIELD_NAME = "s_value";
    private static final String MULTI_VALUED_FIELD_NAME = "s_values";
    private static Map<String, Map<String, Object>> expectedMultiSortBuckets;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GroovyPlugin.class);
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("idx")
                .addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=keyword",
                        MULTI_VALUED_FIELD_NAME, "type=keyword",
                        "tag", "type=keyword").get());
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            builders.add(client().prepareIndex("idx", "type").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val" + i).field("i", i)
                            .field("tag", i < 5 / 2 + 1 ? "more" : "less").startArray(MULTI_VALUED_FIELD_NAME).value("val" + i)
                            .value("val" + (i + 1)).endArray().endObject()));
        }

        getMultiSortDocs(builders);

        for (int i = 0; i < 100; i++) {
            builders.add(client().prepareIndex("idx", "high_card_type").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val" + Strings.padStart(i + "", 3, '0'))
                            .startArray(MULTI_VALUED_FIELD_NAME).value("val" + Strings.padStart(i + "", 3, '0'))
                            .value("val" + Strings.padStart((i + 1) + "", 3, '0')).endArray().endObject()));
        }
        prepareCreate("empty_bucket_idx").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=integer").execute().actionGet();

        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + i).setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i * 2).endObject()));
        }
        indexRandom(true, builders);
        createIndex("idx_unmapped");
        ensureSearchable();
    }

    private void getMultiSortDocs(List<IndexRequestBuilder> builders) throws IOException {
        expectedMultiSortBuckets = new HashMap<>();
        Map<String, Object> bucketProps = new HashMap<>();
        bucketProps.put("_term", "val1");
        bucketProps.put("_count", 3L);
        bucketProps.put("avg_l", 1d);
        bucketProps.put("sum_d", 6d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val2");
        bucketProps.put("_count", 3L);
        bucketProps.put("avg_l", 2d);
        bucketProps.put("sum_d", 6d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val3");
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 3d);
        bucketProps.put("sum_d", 3d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val4");
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 3d);
        bucketProps.put("sum_d", 4d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val5");
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 3d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val6");
        bucketProps.put("_count", 1L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 1d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val7");
        bucketProps.put("_count", 1L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 1d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);

        assertAcked(client().admin().indices().prepareCreate("sort_idx")
                .addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=keyword",
                        MULTI_VALUED_FIELD_NAME, "type=keyword",
                        "tag", "type=keyword").get());
        for (int i = 1; i <= 3; i++) {
            builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val1").field("l", 1).field("d", i).endObject()));
            builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val2").field("l", 2).field("d", i).endObject()));
        }
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val3").field("l", 3).field("d", 1).endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val3").field("l", 3).field("d", 2).endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val4").field("l", 3).field("d", 1).endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val4").field("l", 3).field("d", 3).endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val5").field("l", 5).field("d", 1).endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val5").field("l", 5).field("d", 2).endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val6").field("l", 5).field("d", 1).endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(
                jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val7").field("l", 5).field("d", 1).endObject()));
    }

    private String key(Terms.Bucket bucket) {
        return bucket.getKeyAsString();
    }

    // the main purpose of this test is to make sure we're not allocating 2GB of memory per shard
    public void testSizeIsZero() {
        final int minDocCount = randomInt(1);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> client()
                .prepareSearch("idx")
                .setTypes("high_card_type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).minDocCount(minDocCount).size(0)).execute()
                .actionGet());
        assertThat(exception.getMessage(), containsString("[size] must be greater than 0. Found [0] in [terms]"));
    }

    public void testSingleValueField() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values()))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));
        Object[] propertiesKeys = (Object[]) terms.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) terms.getProperty("_count");

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
            assertThat((String) propertiesKeys[i], equalTo("val" + i));
            assertThat((long) propertiesDocCounts[i], equalTo(1L));
        }
    }

    public void testSingleValueFieldWithGlobalOrdinals() throws Exception {
        ExecutionMode[] executionModes = new ExecutionMode[] { null, ExecutionMode.GLOBAL_ORDINALS, ExecutionMode.GLOBAL_ORDINALS_HASH,
                ExecutionMode.GLOBAL_ORDINALS_LOW_CARDINALITY };
        for (ExecutionMode executionMode : executionModes) {
            logger.info("Execution mode: {}", executionMode);
            SearchResponse response = client()
                    .prepareSearch("idx")
                    .setTypes("type")
                    .addAggregation(
                            terms("terms").executionHint(executionMode == null ? null : executionMode.toString())
                                    .field(SINGLE_VALUED_FIELD_NAME).collectMode(randomFrom(SubAggCollectionMode.values()))).execute()
                    .actionGet();
            assertSearchResponse(response);

            Terms terms = response.getAggregations().get("terms");
            assertThat(terms, notNullValue());
            assertThat(terms.getName(), equalTo("terms"));
            assertThat(terms.getBuckets().size(), equalTo(5));
            for (int i = 0; i < 5; i++) {
                Terms.Bucket bucket = terms.getBucketByKey("val" + i);
                assertThat(bucket, notNullValue());
                assertThat(key(bucket), equalTo("val" + i));
                assertThat(bucket.getDocCount(), equalTo(1L));
            }
        }
    }

    public void testSingleValueFieldWithRegexFiltering() throws Exception {
        // include without exclude
        // we should be left with: val000, val001, val002, val003, val004, val005, val006, val007, val008, val009

        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("high_card_type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).includeExclude(new IncludeExclude("val00.+", null)))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(10));

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val00" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val00" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }

        // include and exclude
        // we should be left with: val002, val003, val004, val005, val006, val007, val008, val009

        response = client()
                .prepareSearch("idx")
                .setTypes("high_card_type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .includeExclude(new IncludeExclude("val00.+", "(val000|val001)")))
                .execute().actionGet();

        assertSearchResponse(response);

        terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(8));

        for (int i = 2; i < 10; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val00" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val00" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }

        // exclude without include
        // we should be left with: val000, val001, val002, val003, val004, val005, val006, val007, val008, val009

        response = client()
                .prepareSearch("idx")
                .setTypes("high_card_type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .includeExclude(new IncludeExclude(null, new RegExp("val0[1-9]+.+"))))
                .execute().actionGet();

        assertSearchResponse(response);

        terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(10));

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val00" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val00" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testSingleValueFieldWithExactTermFiltering() throws Exception {
        // include without exclude
        String incVals[] = { "val000", "val001", "val002", "val003", "val004", "val005", "val006", "val007", "val008", "val009" };
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("high_card_type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).includeExclude(new IncludeExclude(incVals, null)))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(incVals.length));

        for (String incVal : incVals) {
            Terms.Bucket bucket = terms.getBucketByKey(incVal);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo(incVal));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }

        // include and exclude
        // Slightly illogical example with exact terms below as include and exclude sets
        // are made to overlap but the exclude set should have priority over matches.
        // we should be left with: val002, val003, val004, val005, val006, val007, val008, val009
        String excVals[] = { "val000", "val001" };

        response = client()
                .prepareSearch("idx")
                .setTypes("high_card_type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).includeExclude(new IncludeExclude(incVals, excVals)))
                .execute()
                .actionGet();

        assertSearchResponse(response);

        terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(8));

        for (int i = 2; i < 10; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val00" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val00" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }

        // Check case with only exact term exclude clauses
        response = client()
                .prepareSearch("idx")
                .setTypes("high_card_type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values())).includeExclude(new IncludeExclude(null, excVals)))
                .execute().actionGet();

        assertSearchResponse(response);

        terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(10));
        for (String key : excVals) {
            Terms.Bucket bucket = terms.getBucketByKey(key);
            assertThat(bucket, nullValue());
        }
        NumberFormat nf = NumberFormat.getIntegerInstance(Locale.ENGLISH);
        nf.setMinimumIntegerDigits(3);
        for (int i = 2; i < 12; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + nf.format(i));
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + nf.format(i)));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }

    }

    public void testSingleValueFieldWithMaxSize() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("high_card_type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME).size(20)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.term(true))) // we need to sort by terms cause we're checking the first 20 values
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(20));

        for (int i = 0; i < 20; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + Strings.padStart(i + "", 3, '0'));
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + Strings.padStart(i + "", 3, '0')));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testSingleValueFieldOrderedByTermAsc() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.term(true))).execute()
                .actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
            i++;
        }
    }

    public void testSingleValueFieldOrderedByTermDesc() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.term(false))).execute()
                .actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 4;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
            i--;
        }
    }

    public void testSingleValuedFieldWithSubAggregation() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                .subAggregation(count("count").field(MULTI_VALUED_FIELD_NAME))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));
        Object[] propertiesKeys = (Object[]) terms.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) terms.getProperty("_count");
        Object[] propertiesCounts = (Object[]) terms.getProperty("count.value");

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
            ValueCount valueCount = bucket.getAggregations().get("count");
            assertThat(valueCount, notNullValue());
            assertThat(valueCount.getValue(), equalTo(2L));
            assertThat((String) propertiesKeys[i], equalTo("val" + i));
            assertThat((long) propertiesDocCounts[i], equalTo(1L));
            assertThat((double) propertiesCounts[i], equalTo(2.0));
        }
    }

    public void testSingleValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).script(new Script("'foo_' + _value"))).execute()
                .actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("foo_val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testMultiValuedFieldWithValueScriptNotUnique() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(MULTI_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).script(new Script("_value.substring(0,3)")))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(1));

        Terms.Bucket bucket = terms.getBucketByKey("val");
        assertThat(bucket, notNullValue());
        assertThat(key(bucket), equalTo("val"));
        assertThat(bucket.getDocCount(), equalTo(5L));
    }

    public void testMultiValuedField() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(MULTI_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values()))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1L));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2L));
            }
        }
    }

    public void testMultiValuedScript() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).script(new Script("doc['" + MULTI_VALUED_FIELD_NAME + "']"))
                                .collectMode(randomFrom(SubAggCollectionMode.values()))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1L));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2L));
            }
        }
    }

    public void testMultiValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(MULTI_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).script(new Script("'foo_' + _value"))).execute()
                .actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("foo_val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1L));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2L));
            }
        }
    }

    /*
     *
     * [foo_val0, foo_val1] [foo_val1, foo_val2] [foo_val2, foo_val3] [foo_val3,
     * foo_val4] [foo_val4, foo_val5]
     *
     *
     * foo_val0 - doc_count: 1 - val_count: 2 foo_val1 - doc_count: 2 -
     * val_count: 4 foo_val2 - doc_count: 2 - val_count: 4 foo_val3 - doc_count:
     * 2 - val_count: 4 foo_val4 - doc_count: 2 - val_count: 4 foo_val5 -
     * doc_count: 1 - val_count: 2
     */

    public void testScriptSingleValue() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").collectMode(randomFrom(SubAggCollectionMode.values())).executionHint(randomExecutionHint())
                                .script(new Script("doc['" + SINGLE_VALUED_FIELD_NAME + "'].value"))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testScriptSingleValueExplicitSingleValue() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").collectMode(randomFrom(SubAggCollectionMode.values())).executionHint(randomExecutionHint())
                                .script(new Script("doc['" + SINGLE_VALUED_FIELD_NAME + "'].value"))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testScriptMultiValued() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").collectMode(randomFrom(SubAggCollectionMode.values())).executionHint(randomExecutionHint())
                                .script(new Script("doc['" + MULTI_VALUED_FIELD_NAME + "']"))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1L));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2L));
            }
        }
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx_unmapped")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).size(randomIntBetween(1, 5)).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values()))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(0));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx", "idx_unmapped")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values()))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testStringTermsNestedIntoPerBucketAggregator() throws Exception {
        // no execution hint so that the logic that decides whether or not to use ordinals is executed
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        filter("filter", termQuery(MULTI_VALUED_FIELD_NAME, "val3")).subAggregation(
                                terms("terms").field(MULTI_VALUED_FIELD_NAME).collectMode(randomFrom(SubAggCollectionMode.values()))))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Filter filter = response.getAggregations().get("filter");

        Terms terms = filter.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(3));

        for (int i = 2; i <= 4; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(i == 3 ? 2L : 1L));
        }
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1L).minDocCount(0).subAggregation(terms("terms").field("value")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Terms terms = bucket.getAggregations().get("terms");
        assertThat(terms, Matchers.notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().isEmpty(), is(true));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAsc() throws Exception {
        boolean asc = true;
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.aggregation("avg_i", asc))
                                .subAggregation(avg("avg_i").field("i"))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
            Avg avg = bucket.getAggregations().get("avg_i");
            assertThat(avg, notNullValue());
            assertThat(avg.getValue(), equalTo((double) i));
            i++;
        }
    }

    public void testSingleValuedFieldOrderedByIllegalAgg() throws Exception {
        boolean asc = true;
        try {
            client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                .order(Terms.Order.aggregation("inner_terms>avg", asc))
                                .subAggregation(terms("inner_terms").field(MULTI_VALUED_FIELD_NAME).subAggregation(avg("avg").field("i"))))
                .execute().actionGet();
            fail("Expected an exception");
        } catch (SearchPhaseExecutionException e) {
            ElasticsearchException[] rootCauses = e.guessRootCauses();
            if (rootCauses.length == 1) {
                ElasticsearchException rootCause = rootCauses[0];
                if (rootCause instanceof AggregationExecutionException) {
                    AggregationExecutionException aggException = (AggregationExecutionException) rootCause;
                    assertThat(aggException.getMessage(), Matchers.startsWith("Invalid terms aggregation order path"));
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    public void testSingleValuedFieldOrderedBySingleBucketSubAggregationAsc() throws Exception {
        boolean asc = randomBoolean();
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("tags").executionHint(randomExecutionHint()).field("tag")
                                .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.aggregation("filter", asc))
                                .subAggregation(filter("filter", QueryBuilders.matchAllQuery()))).execute().actionGet();

        assertSearchResponse(response);

        Terms tags = response.getAggregations().get("tags");
        assertThat(tags, notNullValue());
        assertThat(tags.getName(), equalTo("tags"));
        assertThat(tags.getBuckets().size(), equalTo(2));

        Iterator<Terms.Bucket> iters = tags.getBuckets().iterator();

        Terms.Bucket tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "less" : "more"));
        assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
        Filter filter = tag.getAggregations().get("filter");
        assertThat(filter, notNullValue());
        assertThat(filter.getDocCount(), equalTo(asc ? 2L : 3L));

        tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "more" : "less"));
        assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
        filter = tag.getAggregations().get("filter");
        assertThat(filter, notNullValue());
        assertThat(filter.getDocCount(), equalTo(asc ? 3L : 2L));
    }

    public void testSingleValuedFieldOrderedBySubAggregationAscMultiHierarchyLevels() throws Exception {
        boolean asc = randomBoolean();
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("tags")
                                .executionHint(randomExecutionHint())
                                .field("tag")
                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                .order(Terms.Order.aggregation("filter1>filter2>stats.max", asc))
                                .subAggregation(
                                        filter("filter1", QueryBuilders.matchAllQuery()).subAggregation(
                                                filter("filter2", QueryBuilders.matchAllQuery()).subAggregation(
                                                        stats("stats").field("i"))))).execute().actionGet();

        assertSearchResponse(response);

        Terms tags = response.getAggregations().get("tags");
        assertThat(tags, notNullValue());
        assertThat(tags.getName(), equalTo("tags"));
        assertThat(tags.getBuckets().size(), equalTo(2));

        Iterator<Terms.Bucket> iters = tags.getBuckets().iterator();

        // the max for "more" is 2
        // the max for "less" is 4

        Terms.Bucket tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "more" : "less"));
        assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter2 = filter1.getAggregations().get("filter2");
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 3L : 2L));
        Stats stats = filter2.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 2.0 : 4.0));

        tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "less" : "more"));
        assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
        filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 2L : 3L));
        filter2 = filter1.getAggregations().get("filter2");
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 2L : 3L));
        stats = filter2.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 4.0 : 2.0));
    }

    public void testSingleValuedFieldOrderedBySubAggregationAscMultiHierarchyLevelsSpecialChars() throws Exception {
        StringBuilder filter2NameBuilder = new StringBuilder("filt.er2");
        filter2NameBuilder.append(randomAsciiOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String filter2Name = filter2NameBuilder.toString();
        StringBuilder statsNameBuilder = new StringBuilder("st.ats");
        statsNameBuilder.append(randomAsciiOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String statsName = statsNameBuilder.toString();
        boolean asc = randomBoolean();
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("tags")
                                .executionHint(randomExecutionHint())
                                .field("tag")
                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                .order(Terms.Order.aggregation("filter1>" + filter2Name + ">" + statsName + ".max", asc))
                                .subAggregation(
                                        filter("filter1", QueryBuilders.matchAllQuery()).subAggregation(
                                                filter(filter2Name, QueryBuilders.matchAllQuery()).subAggregation(
                                                        stats(statsName).field("i"))))).execute().actionGet();

        assertSearchResponse(response);

        Terms tags = response.getAggregations().get("tags");
        assertThat(tags, notNullValue());
        assertThat(tags.getName(), equalTo("tags"));
        assertThat(tags.getBuckets().size(), equalTo(2));

        Iterator<Terms.Bucket> iters = tags.getBuckets().iterator();

        // the max for "more" is 2
        // the max for "less" is 4

        Terms.Bucket tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "more" : "less"));
        assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter2 = filter1.getAggregations().get(filter2Name);
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 3L : 2L));
        Stats stats = filter2.getAggregations().get(statsName);
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 2.0 : 4.0));

        tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "less" : "more"));
        assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
        filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 2L : 3L));
        filter2 = filter1.getAggregations().get(filter2Name);
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 2L : 3L));
        stats = filter2.getAggregations().get(statsName);
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 4.0 : 2.0));
    }

    public void testSingleValuedFieldOrderedBySubAggregationAscMultiHierarchyLevelsSpecialCharsNoDotNotation() throws Exception {
        StringBuilder filter2NameBuilder = new StringBuilder("filt.er2");
        filter2NameBuilder.append(randomAsciiOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String filter2Name = filter2NameBuilder.toString();
        StringBuilder statsNameBuilder = new StringBuilder("st.ats");
        statsNameBuilder.append(randomAsciiOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String statsName = statsNameBuilder.toString();
        boolean asc = randomBoolean();
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("tags")
                                .executionHint(randomExecutionHint())
                                .field("tag")
                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                .order(Terms.Order.aggregation("filter1>" + filter2Name + ">" + statsName + "[max]", asc))
                                .subAggregation(
                                        filter("filter1", QueryBuilders.matchAllQuery()).subAggregation(
                                                filter(filter2Name, QueryBuilders.matchAllQuery()).subAggregation(
                                                        stats(statsName).field("i"))))).execute().actionGet();

        assertSearchResponse(response);

        Terms tags = response.getAggregations().get("tags");
        assertThat(tags, notNullValue());
        assertThat(tags.getName(), equalTo("tags"));
        assertThat(tags.getBuckets().size(), equalTo(2));

        Iterator<Terms.Bucket> iters = tags.getBuckets().iterator();

        // the max for "more" is 2
        // the max for "less" is 4

        Terms.Bucket tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "more" : "less"));
        assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter2 = filter1.getAggregations().get(filter2Name);
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 3L : 2L));
        Stats stats = filter2.getAggregations().get(statsName);
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 2.0 : 4.0));

        tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "less" : "more"));
        assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
        filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 2L : 3L));
        filter2 = filter1.getAggregations().get(filter2Name);
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 2L : 3L));
        stats = filter2.getAggregations().get(statsName);
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 4.0 : 2.0));
    }

    public void testSingleValuedFieldOrderedByMissingSubAggregation() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                client().prepareSearch(index)
                        .setTypes("type")
                        .addAggregation(
                                terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                                        .order(Terms.Order.aggregation("avg_i", true))).execute().actionGet();

                fail("Expected search to fail when trying to sort terms aggregation by sug-aggregation that doesn't exist");

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByNonMetricsOrMultiBucketSubAggregation() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                client().prepareSearch(index)
                        .setTypes("type")
                        .addAggregation(
                                terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                                        .order(Terms.Order.aggregation("values", true))
                                        .subAggregation(terms("values").field("i").collectMode(randomFrom(SubAggCollectionMode.values()))))
                        .execute().actionGet();

                fail("Expected search to fail when trying to sort terms aggregation by sug-aggregation which is not of a metrics or single-bucket type");

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByMultiValuedSubAggregationWithUknownMetric() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                SearchResponse response = client()
                        .prepareSearch(index)
                        .setTypes("type")
                        .addAggregation(
                                terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                                        .order(Terms.Order.aggregation("stats.foo", true)).subAggregation(stats("stats").field("i")))
                        .execute().actionGet();
                fail("Expected search to fail when trying to sort terms aggregation by multi-valued sug-aggregation "
                        + "with an unknown specified metric to order by. response had " + response.getFailedShards() + " failed shards.");

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByMultiValuedSubAggregationWithoutMetric() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                client().prepareSearch(index)
                        .setTypes("type")
                        .addAggregation(
                                terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                                        .order(Terms.Order.aggregation("stats", true)).subAggregation(stats("stats").field("i"))).execute()
                        .actionGet();

                fail("Expected search to fail when trying to sort terms aggregation by multi-valued sug-aggregation "
                        + "where the metric name is not specified");

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationDesc() throws Exception {
        boolean asc = false;
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.aggregation("avg_i", asc))
                                .subAggregation(avg("avg_i").field("i"))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 4;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));

            Avg avg = bucket.getAggregations().get("avg_i");
            assertThat(avg, notNullValue());
            assertThat(avg.getValue(), equalTo((double) i));
            i--;
        }

    }

    public void testSingleValuedFieldOrderedByMultiValueSubAggregationAsc() throws Exception {
        boolean asc = true;
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.aggregation("stats.avg", asc))
                                .subAggregation(stats("stats").field("i"))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));

            Stats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
            i++;
        }
    }

    public void testSingleValuedFieldOrderedByMultiValueSubAggregationDesc() throws Exception {
        boolean asc = false;
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.aggregation("stats.avg", asc))
                                .subAggregation(stats("stats").field("i"))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 4;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));

            Stats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
            i--;
        }

    }

    public void testSingleValuedFieldOrderedByMultiValueExtendedStatsAsc() throws Exception {
        boolean asc = true;
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                .order(Terms.Order.aggregation("stats.sum_of_squares", asc))
                                .subAggregation(extendedStats("stats").field("i"))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));

            ExtendedStats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
            i++;
        }

    }

    public void testSingleValuedFieldOrderedByStatsAggAscWithTermsSubAgg() throws Exception {
        boolean asc = true;
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                .order(Terms.Order.aggregation("stats.sum_of_squares", asc))
                                .subAggregation(extendedStats("stats").field("i"))
                                .subAggregation(terms("subTerms").field("s_values").collectMode(randomFrom(SubAggCollectionMode.values()))))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));

            ExtendedStats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));

            Terms subTermsAgg = bucket.getAggregations().get("subTerms");
            assertThat(subTermsAgg, notNullValue());
            assertThat(subTermsAgg.getBuckets().size(), equalTo(2));
            int j = i;
            for (Terms.Bucket subBucket : subTermsAgg.getBuckets()) {
                assertThat(subBucket, notNullValue());
                assertThat(key(subBucket), equalTo("val" + j));
                assertThat(subBucket.getDocCount(), equalTo(1L));
                j++;
            }
            i++;
        }

    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAndTermsDesc() throws Exception {
        String[] expectedKeys = new String[] { "val1", "val2", "val4", "val3", "val7", "val6", "val5" };
        assertMultiSortResponse(expectedKeys, Terms.Order.aggregation("avg_l", true), Terms.Order.term(false));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAndTermsAsc() throws Exception {
        String[] expectedKeys = new String[] { "val1", "val2", "val3", "val4", "val5", "val6", "val7" };
        assertMultiSortResponse(expectedKeys, Terms.Order.aggregation("avg_l", true), Terms.Order.term(true));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationDescAndTermsAsc() throws Exception {
        String[] expectedKeys = new String[] { "val5", "val6", "val7", "val3", "val4", "val2", "val1" };
        assertMultiSortResponse(expectedKeys, Terms.Order.aggregation("avg_l", false), Terms.Order.term(true));
    }

    public void testSingleValuedFieldOrderedByCountAscAndSingleValueSubAggregationAsc() throws Exception {
        String[] expectedKeys = new String[] { "val6", "val7", "val3", "val4", "val5", "val1", "val2" };
        assertMultiSortResponse(expectedKeys, Terms.Order.count(true), Terms.Order.aggregation("avg_l", true));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscSingleValueSubAggregationAsc() throws Exception {
        String[] expectedKeys = new String[] { "val6", "val7", "val3", "val5", "val4", "val1", "val2" };
        assertMultiSortResponse(expectedKeys, Terms.Order.aggregation("sum_d", true), Terms.Order.aggregation("avg_l", true));
    }

    public void testSingleValuedFieldOrderedByThreeCriteria() throws Exception {
        String[] expectedKeys = new String[] { "val2", "val1", "val4", "val5", "val3", "val6", "val7" };
        assertMultiSortResponse(expectedKeys, Terms.Order.count(false), Terms.Order.aggregation("sum_d", false),
                Terms.Order.aggregation("avg_l", false));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAsCompound() throws Exception {
        String[] expectedKeys = new String[] { "val1", "val2", "val3", "val4", "val5", "val6", "val7" };
        assertMultiSortResponse(expectedKeys, Terms.Order.aggregation("avg_l", true));
    }

    private void assertMultiSortResponse(String[] expectedKeys, Terms.Order... order) {
        SearchResponse response = client()
                .prepareSearch("sort_idx")
                .setTypes("multi_sort_type")
                .addAggregation(
                        terms("terms").executionHint(randomExecutionHint()).field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())).order(Terms.Order.compound(order))
                                .subAggregation(avg("avg_l").field("l")).subAggregation(sum("sum_d").field("d"))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(expectedKeys.length));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo(expectedKeys[i]));
            assertThat(bucket.getDocCount(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("_count")));
            Avg avg = bucket.getAggregations().get("avg_l");
            assertThat(avg, notNullValue());
            assertThat(avg.getValue(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("avg_l")));
            Sum sum = bucket.getAggregations().get("sum_d");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("sum_d")));
            i++;
        }
    }

    public void testIndexMetaField() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx", "empty_bucket_idx")
                .setTypes("type")
                .addAggregation(
                        terms("terms").collectMode(randomFrom(SubAggCollectionMode.values())).executionHint(randomExecutionHint())
                                .field(IndexFieldMapper.NAME)).execute().actionGet();

        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(2));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo(i == 0 ? "idx" : "empty_bucket_idx"));
            assertThat(bucket.getDocCount(), equalTo(i == 0 ? 5L : 2L));
            i++;
        }
    }

    public void testOtherDocCount() {
        testOtherDocCount(SINGLE_VALUED_FIELD_NAME, MULTI_VALUED_FIELD_NAME);
    }
}
