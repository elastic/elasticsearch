/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationTestScriptsPlugin;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.Range.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.range;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class RangeIT extends ESIntegTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";
    private static final String MULTI_VALUED_FIELD_NAME = "l_values";

    static int numDocs;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends AggregationTestScriptsPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = super.pluginScripts();

            scripts.put("doc['" + SINGLE_VALUED_FIELD_NAME + "'].value", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Longs value = (ScriptDocValues.Longs) doc.get(SINGLE_VALUED_FIELD_NAME);
                return value.getValue();
            });

            scripts.put("doc['" + MULTI_VALUED_FIELD_NAME + "']", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Longs value = (ScriptDocValues.Longs) doc.get(MULTI_VALUED_FIELD_NAME);
                return value;
            });

            return scripts;
        }

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> nonDeterministicPluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("Math.random()", vars -> RangeIT.randomDouble());

            return scripts;
        }
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        numDocs = randomIntBetween(10, 20);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            builders.add(
                client().prepareIndex("idx", "type")
                    .setSource(
                        jsonBuilder().startObject()
                            .field(SINGLE_VALUED_FIELD_NAME, i + 1)
                            .startArray(MULTI_VALUED_FIELD_NAME)
                            .value(i + 1)
                            .value(i + 2)
                            .endArray()
                            .endObject()
                    )
            );
        }
        createIndex("idx_unmapped");
        prepareCreate("empty_bucket_idx").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=integer").get();
        for (int i = 0; i < 2; i++) {
            builders.add(
                client().prepareIndex("empty_bucket_idx", "type", "" + i)
                    .setSource(
                        jsonBuilder().startObject()
                            // shift sequence by 1, to ensure we have negative values, and value 3 on the edge of the tested ranges
                            .field(SINGLE_VALUED_FIELD_NAME, i * 2 - 1)
                            .endObject()
                    )
            );
        }

        // Create two indices and add the field 'route_length_miles' as an alias in
        // one, and a concrete field in the other.
        prepareCreate("old_index").addMapping("_doc", "distance", "type=double", "route_length_miles", "type=alias,path=distance").get();
        prepareCreate("new_index").addMapping("_doc", "route_length_miles", "type=double").get();

        builders.add(client().prepareIndex("old_index", "_doc").setSource("distance", 42.0));
        builders.add(client().prepareIndex("old_index", "_doc").setSource("distance", 50.5));
        builders.add(client().prepareIndex("new_index", "_doc").setSource("route_length_miles", 100.2));
        builders.add(client().prepareIndex("new_index", "_doc").setSource(Collections.emptyMap()));

        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testRangeAsSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field(MULTI_VALUED_FIELD_NAME)
                    .size(100)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(range("range").field(SINGLE_VALUED_FIELD_NAME).addUnboundedTo(3).addRange(3, 6).addUnboundedFrom(6))
            )
            .get();

        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), equalTo(numDocs + 1));
        for (int i = 1; i < numDocs + 2; ++i) {
            Terms.Bucket bucket = terms.getBucketByKey("" + i);
            assertThat(bucket, notNullValue());
            final long docCount = i == 1 || i == numDocs + 1 ? 1 : 2;
            assertThat(bucket.getDocCount(), equalTo(docCount));
            Range range = bucket.getAggregations().get("range");
            List<? extends Bucket> buckets = range.getBuckets();
            Range.Bucket rangeBucket = buckets.get(0);
            assertThat(rangeBucket.getKey(), equalTo("*-3.0"));
            assertThat(rangeBucket.getKeyAsString(), equalTo("*-3.0"));
            assertThat(rangeBucket, notNullValue());
            assertThat(rangeBucket.getFromAsString(), nullValue());
            assertThat(rangeBucket.getToAsString(), equalTo("3.0"));
            if (i == 1 || i == 3) {
                assertThat(rangeBucket.getDocCount(), equalTo(1L));
            } else if (i == 2) {
                assertThat(rangeBucket.getDocCount(), equalTo(2L));
            } else {
                assertThat(rangeBucket.getDocCount(), equalTo(0L));
            }
            rangeBucket = buckets.get(1);
            assertThat(rangeBucket.getKey(), equalTo("3.0-6.0"));
            assertThat(rangeBucket.getKeyAsString(), equalTo("3.0-6.0"));
            assertThat(rangeBucket, notNullValue());
            assertThat(rangeBucket.getFromAsString(), equalTo("3.0"));
            assertThat(rangeBucket.getToAsString(), equalTo("6.0"));
            if (i == 3 || i == 6) {
                assertThat(rangeBucket.getDocCount(), equalTo(1L));
            } else if (i == 4 || i == 5) {
                assertThat(rangeBucket.getDocCount(), equalTo(2L));
            } else {
                assertThat(rangeBucket.getDocCount(), equalTo(0L));
            }
            rangeBucket = buckets.get(2);
            assertThat(rangeBucket.getKey(), equalTo("6.0-*"));
            assertThat(rangeBucket.getKeyAsString(), equalTo("6.0-*"));
            assertThat(rangeBucket, notNullValue());
            assertThat(rangeBucket.getFromAsString(), equalTo("6.0"));
            assertThat(rangeBucket.getToAsString(), nullValue());
            if (i == 6 || i == numDocs + 1) {
                assertThat(rangeBucket.getDocCount(), equalTo(1L));
            } else if (i < 6) {
                assertThat(rangeBucket.getDocCount(), equalTo(0L));
            } else {
                assertThat(rangeBucket.getDocCount(), equalTo(2L));
            }
        }
    }

    public void testSingleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(range("range").field(SINGLE_VALUED_FIELD_NAME).addUnboundedTo(3).addRange(3, 6).addUnboundedFrom(6))
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(3.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("3.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3.0"));
        assertThat(bucket.getToAsString(), equalTo("6.0"));
        assertThat(bucket.getDocCount(), equalTo(3L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("6.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5L));
    }

    public void testSingleValueFieldWithFormat() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(range("range").field(SINGLE_VALUED_FIELD_NAME).addUnboundedTo(3).addRange(3, 6).addUnboundedFrom(6).format("#"))
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(3.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("3"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3-6"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3"));
        assertThat(bucket.getToAsString(), equalTo("6"));
        assertThat(bucket.getDocCount(), equalTo(3L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("6"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5L));
    }

    public void testSingleValueFieldWithCustomKey() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                range("range").field(SINGLE_VALUED_FIELD_NAME).addUnboundedTo("r1", 3).addRange("r2", 3, 6).addUnboundedFrom("r3", 6)
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r1"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(3.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("3.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r2"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3.0"));
        assertThat(bucket.getToAsString(), equalTo("6.0"));
        assertThat(bucket.getDocCount(), equalTo(3L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r3"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("6.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5L));
    }

    public void testSingleValuedFieldWithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                range("range").field(SINGLE_VALUED_FIELD_NAME)
                    .addUnboundedTo(3)
                    .addRange(3, 6)
                    .addUnboundedFrom(6)
                    .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));
        Object[] propertiesKeys = (Object[]) ((InternalAggregation) range).getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) range).getProperty("_count");
        Object[] propertiesCounts = (Object[]) ((InternalAggregation) range).getProperty("sum.value");

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(3.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("3.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(3.0)); // 1 + 2
        assertThat(propertiesKeys[0], equalTo("*-3.0"));
        assertThat(propertiesDocCounts[0], equalTo(2L));
        assertThat(propertiesCounts[0], equalTo(3.0));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3.0"));
        assertThat(bucket.getToAsString(), equalTo("6.0"));
        assertThat(bucket.getDocCount(), equalTo(3L));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(12.0)); // 3 + 4 + 5
        assertThat(propertiesKeys[1], equalTo("3.0-6.0"));
        assertThat(propertiesDocCounts[1], equalTo(3L));
        assertThat(propertiesCounts[1], equalTo(12.0));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("6.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5L));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        long total = 0;
        for (int i = 5; i < numDocs; ++i) {
            total += i + 1;
        }
        assertThat(sum.getValue(), equalTo((double) total));
        assertThat(propertiesKeys[2], equalTo("6.0-*"));
        assertThat(propertiesDocCounts[2], equalTo(numDocs - 5L));
        assertThat(propertiesCounts[2], equalTo((double) total));
    }

    public void testSingleValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                range("range").field(SINGLE_VALUED_FIELD_NAME)
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value + 1", Collections.emptyMap()))
                    .addUnboundedTo(3)
                    .addRange(3, 6)
                    .addUnboundedFrom(6)
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(3.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("3.0"));
        assertThat(bucket.getDocCount(), equalTo(1L)); // 2

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3.0"));
        assertThat(bucket.getToAsString(), equalTo("6.0"));
        assertThat(bucket.getDocCount(), equalTo(3L)); // 3, 4, 5

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("6.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
    }

    /*
    [1, 2]
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]
     */

    public void testMultiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(range("range").field(MULTI_VALUED_FIELD_NAME).addUnboundedTo(3).addRange(3, 6).addUnboundedFrom(6))
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(3.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("3.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3.0"));
        assertThat(bucket.getToAsString(), equalTo("6.0"));
        assertThat(bucket.getDocCount(), equalTo(4L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("6.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
    }

    /*
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]
    [11, 12]
     */

    public void testMultiValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                range("range").field(MULTI_VALUED_FIELD_NAME)
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value + 1", Collections.emptyMap()))
                    .addUnboundedTo(3)
                    .addRange(3, 6)
                    .addUnboundedFrom(6)
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(3.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("3.0"));
        assertThat(bucket.getDocCount(), equalTo(1L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3.0"));
        assertThat(bucket.getToAsString(), equalTo("6.0"));
        assertThat(bucket.getDocCount(), equalTo(4L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("6.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 3L));
    }

    /*
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]
    [11, 12]

    r1: 2
    r2: 3, 3, 4, 4, 5, 5
    r3: 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12
     */

    public void testScriptSingleValue() throws Exception {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "doc['" + SINGLE_VALUED_FIELD_NAME + "'].value",
            Collections.emptyMap()
        );
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(range("range").script(script).addUnboundedTo(3).addRange(3, 6).addUnboundedFrom(6))
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(3.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("3.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3.0"));
        assertThat(bucket.getToAsString(), equalTo("6.0"));
        assertThat(bucket.getDocCount(), equalTo(3L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("6.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5L));
    }

    public void testEmptyRange() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(range("range").field(MULTI_VALUED_FIELD_NAME).addUnboundedTo(-1).addUnboundedFrom(1000))
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(2));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*--1.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(-1.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("-1.0"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("1000.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(1000d));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("1000.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(0L));
    }

    public void testNoRangesInQuery() {
        try {
            client().prepareSearch("idx").addAggregation(range("foobar").field(SINGLE_VALUED_FIELD_NAME)).get();
            fail();
        } catch (SearchPhaseExecutionException spee) {
            Throwable rootCause = spee.getCause().getCause();
            assertThat(rootCause, instanceOf(IllegalArgumentException.class));
            assertEquals(rootCause.getMessage(), "No [ranges] specified for the [foobar] aggregation");
        }
    }

    public void testScriptMultiValued() throws Exception {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "doc['" + MULTI_VALUED_FIELD_NAME + "']",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(range("range").script(script).addUnboundedTo(3).addRange(3, 6).addUnboundedFrom(6))
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(3.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("3.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3.0"));
        assertThat(bucket.getToAsString(), equalTo("6.0"));
        assertThat(bucket.getDocCount(), equalTo(4L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("6.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
    }

    /*
    [1, 2]
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]

    r1: 1, 2, 2
    r2: 3, 3, 4, 4, 5, 5
    r3: 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11
     */

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
            .addAggregation(range("range").field(SINGLE_VALUED_FIELD_NAME).addUnboundedTo(3).addRange(3, 6).addUnboundedFrom(6))
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(3.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("3.0"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3.0"));
        assertThat(bucket.getToAsString(), equalTo("6.0"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("6.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(0L));
    }

    public void testPartiallyUnmapped() throws Exception {
        client().admin().cluster().prepareHealth("idx_unmapped").setWaitForYellowStatus().get();

        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(range("range").field(SINGLE_VALUED_FIELD_NAME).addUnboundedTo(3).addRange(3, 6).addUnboundedFrom(6))
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(3.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("3.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3.0"));
        assertThat(bucket.getToAsString(), equalTo("6.0"));
        assertThat(bucket.getDocCount(), equalTo(3L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("6.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 5L));
    }

    public void testOverlappingRanges() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                range("range").field(MULTI_VALUED_FIELD_NAME).addUnboundedTo(5).addRange(3, 6).addRange(4, 5).addUnboundedFrom(4)
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(4));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-5.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(5.0));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("5.0"));
        assertThat(bucket.getDocCount(), equalTo(4L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(3.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getFromAsString(), equalTo("3.0"));
        assertThat(bucket.getToAsString(), equalTo("6.0"));
        assertThat(bucket.getDocCount(), equalTo(4L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("4.0-5.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(4.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(5.0));
        assertThat(bucket.getFromAsString(), equalTo("4.0"));
        assertThat(bucket.getToAsString(), equalTo("5.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("4.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(4.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("4.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 2L));
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                    .interval(1L)
                    .minDocCount(0)
                    .subAggregation(range("range").field(SINGLE_VALUED_FIELD_NAME).addRange("0-2", 0.0, 2.0))
            )
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Range range = bucket.getAggregations().get("range");
        // TODO: use diamond once JI-9019884 is fixed
        List<Range.Bucket> buckets = new ArrayList<>(range.getBuckets());
        assertThat(range, Matchers.notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(buckets.size(), is(1));
        assertThat(buckets.get(0).getKey(), equalTo("0-2"));
        assertThat(((Number) buckets.get(0).getFrom()).doubleValue(), equalTo(0.0));
        assertThat(((Number) buckets.get(0).getTo()).doubleValue(), equalTo(2.0));
        assertThat(buckets.get(0).getFromAsString(), equalTo("0.0"));
        assertThat(buckets.get(0).getToAsString(), equalTo("2.0"));
        assertThat(buckets.get(0).getDocCount(), equalTo(0L));

    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        assertAcked(
            prepareCreate("cache_test_idx").addMapping("type", "i", "type=integer")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
                .get()
        );
        indexRandom(
            true,
            client().prepareIndex("cache_test_idx", "type", "1").setSource(jsonBuilder().startObject().field("i", 1).endObject()),
            client().prepareIndex("cache_test_idx", "type", "2").setSource(jsonBuilder().startObject().field("i", 2).endObject())
        );

        // Make sure we are starting with a clear cache
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a nondeterministic script does not get cached
        Map<String, Object> params = new HashMap<>();
        params.put("fieldname", "date");
        SearchResponse r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                range("foo").field("i")
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "Math.random()", Collections.emptyMap()))
                    .addRange(0, 10)
            )
            .get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a deterministic script gets cached
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                range("foo").field("i")
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value + 1", Collections.emptyMap()))
                    .addRange(0, 10)
            )
            .get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(1L)
        );

        // Ensure that non-scripted requests are cached as normal
        r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(range("foo").field("i").addRange(0, 10)).get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(2L)
        );
    }

    public void testFieldAlias() {
        SearchResponse response = client().prepareSearch("old_index", "new_index")
            .addAggregation(range("range").field("route_length_miles").addUnboundedTo(50.0).addRange(50.0, 150.0).addUnboundedFrom(150.0))
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Range.Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-50.0"));
        assertThat(bucket.getDocCount(), equalTo(1L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("50.0-150.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("150.0-*"));
        assertThat(bucket.getDocCount(), equalTo(0L));
    }

    public void testFieldAliasWithMissingValue() {
        SearchResponse response = client().prepareSearch("old_index", "new_index")
            .addAggregation(
                range("range").field("route_length_miles").missing(0.0).addUnboundedTo(50.0).addRange(50.0, 150.0).addUnboundedFrom(150.0)
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Range.Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-50.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("50.0-150.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("150.0-*"));
        assertThat(bucket.getDocCount(), equalTo(0L));
    }
}
