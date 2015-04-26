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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.nested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class NestedTests extends ElasticsearchIntegrationTest {

    static int numParents;
    static int[] numChildren;
    static SubAggCollectionMode aggCollectionMode;

    @Override
    public void setupSuiteScopeCluster() throws Exception {

        assertAcked(prepareCreate("idx")
                .addMapping("type", "nested", "type=nested", "incorrect", "type=object"));
        ensureGreen("idx");

        List<IndexRequestBuilder> builders = new ArrayList<>();

        numParents = randomIntBetween(3, 10);
        numChildren = new int[numParents];
        aggCollectionMode = randomFrom(SubAggCollectionMode.values());
        logger.info("AGG COLLECTION MODE: " + aggCollectionMode);
        int totalChildren = 0;
        for (int i = 0; i < numParents; ++i) {
            if (i == numParents - 1 && totalChildren == 0) {
                // we need at least one child overall
                numChildren[i] = randomIntBetween(1, 5);
            } else {
                numChildren[i] = randomInt(5);
            }
            totalChildren += numChildren[i];
        }
        assertTrue(totalChildren > 0);

        for (int i = 0; i < numParents; i++) {
            XContentBuilder source = jsonBuilder()
                    .startObject()
                    .field("value", i + 1)
                    .startArray("nested");
            for (int j = 0; j < numChildren[i]; ++j) {
                source = source.startObject().field("value", i + 1 + j).endObject();
            }
            source = source.endArray().endObject();
            builders.add(client().prepareIndex("idx", "type", ""+i+1).setSource(source));
        }

        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer", "nested", "type=nested").execute().actionGet();
        ensureGreen("empty_bucket_idx");
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i*2)
                    .startArray("nested")
                    .startObject().field("value", i + 1).endObject()
                    .startObject().field("value", i + 2).endObject()
                    .startObject().field("value", i + 3).endObject()
                    .startObject().field("value", i + 4).endObject()
                    .startObject().field("value", i + 5).endObject()
                    .endArray()
                    .endObject()));
        }

        assertAcked(prepareCreate("idx_nested_nested_aggs")
                .addMapping("type", jsonBuilder().startObject().startObject("type").startObject("properties")
                        .startObject("nested1")
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject("nested2")
                                    .field("type", "nested")
                                .endObject()
                            .endObject()
                        .endObject()
                        .endObject().endObject().endObject()));
        ensureGreen("idx_nested_nested_aggs");

        builders.add(
                client().prepareIndex("idx_nested_nested_aggs", "type", "1")
                        .setSource(jsonBuilder().startObject()
                                .startArray("nested1")
                                    .startObject()
                                    .field("a", "a")
                                        .startArray("nested2")
                                            .startObject()
                                                .field("b", 2)
                                            .endObject()
                                        .endArray()
                                    .endObject()
                                    .startObject()
                                        .field("a", "b")
                                        .startArray("nested2")
                                            .startObject()
                                                .field("b", 2)
                                            .endObject()
                                        .endArray()
                                    .endObject()
                                .endArray()
                            .endObject())
        );
        indexRandom(true, builders);
        ensureSearchable();
    }

    @Test
    public void simple() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(nested("nested").path("nested")
                        .subAggregation(stats("nested_value_stats").field("nested.value")))
                .execute().actionGet();

        assertSearchResponse(response);


        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        long sum = 0;
        long count = 0;
        for (int i = 0; i < numParents; ++i) {
            for (int j = 0; j < numChildren[i]; ++j) {
                final long value = i + 1 + j;
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                ++count;
            }
        }

        Nested nested = response.getAggregations().get("nested");
        assertThat(nested, notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), equalTo(count));
        assertThat(nested.getAggregations().asList().isEmpty(), is(false));

        Stats stats = nested.getAggregations().get("nested_value_stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getMin(), equalTo(min));
        assertThat(stats.getMax(), equalTo(max));
        assertThat(stats.getCount(), equalTo(count));
        assertThat(stats.getSum(), equalTo((double) sum));
        assertThat(stats.getAvg(), equalTo((double) sum / count));
    }

    @Test
    public void nonExistingNestedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .addAggregation(nested("nested").path("value")
                        .subAggregation(stats("nested_value_stats").field("nested.value")))
                .execute().actionGet();

        Nested nested = searchResponse.getAggregations().get("nested");
        assertThat(nested, Matchers.notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), is(0l));
    }

    @Test
    public void nestedWithSubTermsAgg() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(nested("nested").path("nested")
                        .subAggregation(terms("values").field("nested.value").size(100)
                                .collectMode(aggCollectionMode)))
                .execute().actionGet();

        assertSearchResponse(response);


        long docCount = 0;
        long[] counts = new long[numParents + 6];
        for (int i = 0; i < numParents; ++i) {
            for (int j = 0; j < numChildren[i]; ++j) {
                final int value = i + 1 + j;
                ++counts[value];
                ++docCount;
            }
        }
        int uniqueValues = 0;
        for (long count : counts) {
            if (count > 0) {
                ++uniqueValues;
            }
        }

        Nested nested = response.getAggregations().get("nested");
        assertThat(nested, notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), equalTo(docCount));
        assertThat((long) nested.getProperty("_count"), equalTo(docCount));
        assertThat(nested.getAggregations().asList().isEmpty(), is(false));

        LongTerms values = nested.getAggregations().get("values");
        assertThat(values, notNullValue());
        assertThat(values.getName(), equalTo("values"));
        assertThat(values.getBuckets(), notNullValue());
        assertThat(values.getBuckets().size(), equalTo(uniqueValues));
        for (int i = 0; i < counts.length; ++i) {
            final String key = Long.toString(i);
            if (counts[i] == 0) {
                assertNull(values.getBucketByKey(key));
            } else {
                Bucket bucket = values.getBucketByKey(key);
                assertNotNull(bucket);
                assertEquals(counts[i], bucket.getDocCount());
            }
        }
        assertThat((LongTerms) nested.getProperty("values"), sameInstance(values));
    }

    @Test
    public void nestedAsSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("top_values").field("value").size(100)
                        .collectMode(aggCollectionMode)
                        .subAggregation(nested("nested").path("nested")
                                .subAggregation(max("max_value").field("nested.value"))))
                .execute().actionGet();

        assertSearchResponse(response);


        LongTerms values = response.getAggregations().get("top_values");
        assertThat(values, notNullValue());
        assertThat(values.getName(), equalTo("top_values"));
        assertThat(values.getBuckets(), notNullValue());
        assertThat(values.getBuckets().size(), equalTo(numParents));

        for (int i = 0; i < numParents; i++) {
            String topValue = "" + (i + 1);
            assertThat(values.getBucketByKey(topValue), notNullValue());
            Nested nested = values.getBucketByKey(topValue).getAggregations().get("nested");
            assertThat(nested, notNullValue());
            Max max = nested.getAggregations().get("max_value");
            assertThat(max, notNullValue());
            assertThat(max.getValue(), equalTo(numChildren[i] == 0 ? Double.NEGATIVE_INFINITY : (double) i + numChildren[i]));
        }
    }

    @Test
    public void nestNestedAggs() throws Exception {
        SearchResponse response = client().prepareSearch("idx_nested_nested_aggs")
                .addAggregation(nested("level1").path("nested1")
                        .subAggregation(terms("a").field("nested1.a")
                                .collectMode(aggCollectionMode)
                                .subAggregation(nested("level2").path("nested1.nested2")
                                        .subAggregation(sum("sum").field("nested1.nested2.b")))))
                .get();
        assertSearchResponse(response);


        Nested level1 = response.getAggregations().get("level1");
        assertThat(level1, notNullValue());
        assertThat(level1.getName(), equalTo("level1"));
        assertThat(level1.getDocCount(), equalTo(2l));

        StringTerms a = level1.getAggregations().get("a");
        Terms.Bucket bBucket = a.getBucketByKey("a");
        assertThat(bBucket.getDocCount(), equalTo(1l));

        Nested level2 = bBucket.getAggregations().get("level2");
        assertThat(level2.getDocCount(), equalTo(1l));
        Sum sum = level2.getAggregations().get("sum");
        assertThat(sum.getValue(), equalTo(2d));

        a = level1.getAggregations().get("a");
        bBucket = a.getBucketByKey("b");
        assertThat(bBucket.getDocCount(), equalTo(1l));

        level2 = bBucket.getAggregations().get("level2");
        assertThat(level2.getDocCount(), equalTo(1l));
        sum = level2.getAggregations().get("sum");
        assertThat(sum.getValue(), equalTo(2d));
    }


    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0)
                        .subAggregation(nested("nested").path("nested")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Nested nested = bucket.getAggregations().get("nested");
        assertThat(nested, Matchers.notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), is(0l));
    }

    @Test
    public void nestedOnObjectField() throws Exception {
        try {
            client().prepareSearch("idx")
                    .setQuery(matchAllQuery())
                    .addAggregation(nested("object_field").path("incorrect"))
                    .execute().actionGet();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString(), containsString("[nested] nested path [incorrect] is not nested"));
        }
    }

    @Test
    // Test based on: https://github.com/elasticsearch/elasticsearch/issues/9280
    public void testParentFilterResolvedCorrectly() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("provider").startObject("properties")
                    .startObject("comments")
                        .field("type", "nested")
                        .startObject("properties")
                            .startObject("cid").field("type", "long").endObject()
                            .startObject("identifier").field("type", "string").field("index", "not_analyzed").endObject()
                            .startObject("tags")
                                .field("type", "nested")
                                .startObject("properties")
                                    .startObject("tid").field("type", "long").endObject()
                                    .startObject("name").field("type", "string").field("index", "not_analyzed").endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("dates")
                        .field("type", "object")
                        .startObject("properties")
                            .startObject("day").field("type", "date").field("format", "dateOptionalTime").endObject()
                            .startObject("month")
                                .field("type", "object")
                                .startObject("properties")
                                    .startObject("end").field("type", "date").field("format", "dateOptionalTime").endObject()
                                    .startObject("start").field("type", "date").field("format", "dateOptionalTime").endObject()
                                    .startObject("label").field("type", "string").field("index", "not_analyzed").endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject().endObject().endObject();
        assertAcked(prepareCreate("idx2")
                .setSettings(ImmutableSettings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("provider", mapping));
        ensureGreen("idx2");

        List<IndexRequestBuilder> indexRequests = new ArrayList<>(2);
        indexRequests.add(client().prepareIndex("idx2", "provider", "1").setSource("{\"dates\": {\"month\": {\"label\": \"2014-11\", \"end\": \"2014-11-30\", \"start\": \"2014-11-01\"}, \"day\": \"2014-11-30\"}, \"comments\": [{\"cid\": 3,\"identifier\": \"29111\"}, {\"cid\": 4,\"tags\": [{\"tid\" :44,\"name\": \"Roles\"}], \"identifier\": \"29101\"}]}"));
        indexRequests.add(client().prepareIndex("idx2", "provider", "2").setSource("{\"dates\": {\"month\": {\"label\": \"2014-12\", \"end\": \"2014-12-31\", \"start\": \"2014-12-01\"}, \"day\": \"2014-12-03\"}, \"comments\": [{\"cid\": 1, \"identifier\": \"29111\"}, {\"cid\": 2,\"tags\": [{\"tid\" : 22, \"name\": \"DataChannels\"}], \"identifier\": \"29101\"}]}"));
        indexRandom(true, indexRequests);

        SearchResponse response = client().prepareSearch("idx2").setTypes("provider")
                .addAggregation(
                        terms("startDate").field("dates.month.start").subAggregation(
                                terms("endDate").field("dates.month.end").subAggregation(
                                        terms("period").field("dates.month.label").subAggregation(
                                                nested("ctxt_idfier_nested").path("comments").subAggregation(
                                                        filter("comment_filter").filter(termFilter("comments.identifier", "29111")).subAggregation(
                                                                nested("nested_tags").path("comments.tags").subAggregation(
                                                                        terms("tag").field("comments.tags.name")
                                                                )
                                                        )
                                                )
                                        )
                                )
                        )
                ).get();
        assertNoFailures(response);
        assertHitCount(response, 2);

        Terms startDate = response.getAggregations().get("startDate");
        assertThat(startDate.getBuckets().size(), equalTo(2));
        Terms.Bucket bucket = startDate.getBucketByKey("1414800000000"); // 2014-11-01T00:00:00.000Z
        assertThat(bucket.getDocCount(), equalTo(1l));
        Terms endDate = bucket.getAggregations().get("endDate");
        bucket = endDate.getBucketByKey("1417305600000"); // 2014-11-30T00:00:00.000Z
        assertThat(bucket.getDocCount(), equalTo(1l));
        Terms period = bucket.getAggregations().get("period");
        bucket = period.getBucketByKey("2014-11");
        assertThat(bucket.getDocCount(), equalTo(1l));
        Nested comments = bucket.getAggregations().get("ctxt_idfier_nested");
        assertThat(comments.getDocCount(), equalTo(2l));
        Filter filter = comments.getAggregations().get("comment_filter");
        assertThat(filter.getDocCount(), equalTo(1l));
        Nested nestedTags = filter.getAggregations().get("nested_tags");
        assertThat(nestedTags.getDocCount(), equalTo(0l)); // This must be 0
        Terms tags = nestedTags.getAggregations().get("tag");
        assertThat(tags.getBuckets().size(), equalTo(0)); // and this must be empty

        bucket = startDate.getBucketByKey("1417392000000"); // 2014-12-01T00:00:00.000Z
        assertThat(bucket.getDocCount(), equalTo(1l));
        endDate = bucket.getAggregations().get("endDate");
        bucket = endDate.getBucketByKey("1419984000000"); // 2014-12-31T00:00:00.000Z
        assertThat(bucket.getDocCount(), equalTo(1l));
        period = bucket.getAggregations().get("period");
        bucket = period.getBucketByKey("2014-12");
        assertThat(bucket.getDocCount(), equalTo(1l));
        comments = bucket.getAggregations().get("ctxt_idfier_nested");
        assertThat(comments.getDocCount(), equalTo(2l));
        filter = comments.getAggregations().get("comment_filter");
        assertThat(filter.getDocCount(), equalTo(1l));
        nestedTags = filter.getAggregations().get("nested_tags");
        assertThat(nestedTags.getDocCount(), equalTo(0l)); // This must be 0
        tags = nestedTags.getAggregations().get("tag");
        assertThat(tags.getBuckets().size(), equalTo(0)); // and this must be empty
    }

    @Test
    public void nestedSameDocIdProcessedMultipleTime() throws Exception {
        assertAcked(
                prepareCreate("idx4")
                        .setSettings(ImmutableSettings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
                        .addMapping("product", "categories", "type=string", "name", "type=string", "property", "type=nested")
        );
        ensureGreen("idx4");

        client().prepareIndex("idx4", "product", "1").setSource(jsonBuilder().startObject()
                    .field("name", "product1")
                    .field("categories", "1", "2", "3", "4")
                    .startArray("property")
                        .startObject().field("id", 1).endObject()
                        .startObject().field("id", 2).endObject()
                        .startObject().field("id", 3).endObject()
                    .endArray()
                .endObject()).get();
        client().prepareIndex("idx4", "product", "2").setSource(jsonBuilder().startObject()
                .field("name", "product2")
                .field("categories", "1", "2")
                .startArray("property")
                .startObject().field("id", 1).endObject()
                .startObject().field("id", 5).endObject()
                .startObject().field("id", 4).endObject()
                .endArray()
                .endObject()).get();
        refresh();

        SearchResponse response = client().prepareSearch("idx4").setTypes("product")
                .addAggregation(terms("category").field("categories").subAggregation(
                        nested("property").path("property").subAggregation(
                                terms("property_id").field("property.id")
                        )
                ))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 2);

        Terms category = response.getAggregations().get("category");
        assertThat(category.getBuckets().size(), equalTo(4));

        Terms.Bucket bucket = category.getBucketByKey("1");
        assertThat(bucket.getDocCount(), equalTo(2l));
        Nested property = bucket.getAggregations().get("property");
        assertThat(property.getDocCount(), equalTo(6l));
        Terms propertyId = property.getAggregations().get("property_id");
        assertThat(propertyId.getBuckets().size(), equalTo(5));
        assertThat(propertyId.getBucketByKey("1").getDocCount(), equalTo(2l));
        assertThat(propertyId.getBucketByKey("2").getDocCount(), equalTo(1l));
        assertThat(propertyId.getBucketByKey("3").getDocCount(), equalTo(1l));
        assertThat(propertyId.getBucketByKey("4").getDocCount(), equalTo(1l));
        assertThat(propertyId.getBucketByKey("5").getDocCount(), equalTo(1l));

        bucket = category.getBucketByKey("2");
        assertThat(bucket.getDocCount(), equalTo(2l));
        property = bucket.getAggregations().get("property");
        assertThat(property.getDocCount(), equalTo(6l));
        propertyId = property.getAggregations().get("property_id");
        assertThat(propertyId.getBuckets().size(), equalTo(5));
        assertThat(propertyId.getBucketByKey("1").getDocCount(), equalTo(2l));
        assertThat(propertyId.getBucketByKey("2").getDocCount(), equalTo(1l));
        assertThat(propertyId.getBucketByKey("3").getDocCount(), equalTo(1l));
        assertThat(propertyId.getBucketByKey("4").getDocCount(), equalTo(1l));
        assertThat(propertyId.getBucketByKey("5").getDocCount(), equalTo(1l));

        bucket = category.getBucketByKey("3");
        assertThat(bucket.getDocCount(), equalTo(1l));
        property = bucket.getAggregations().get("property");
        assertThat(property.getDocCount(), equalTo(3l));
        propertyId = property.getAggregations().get("property_id");
        assertThat(propertyId.getBuckets().size(), equalTo(3));
        assertThat(propertyId.getBucketByKey("1").getDocCount(), equalTo(1l));
        assertThat(propertyId.getBucketByKey("2").getDocCount(), equalTo(1l));
        assertThat(propertyId.getBucketByKey("3").getDocCount(), equalTo(1l));

        bucket = category.getBucketByKey("4");
        assertThat(bucket.getDocCount(), equalTo(1l));
        property = bucket.getAggregations().get("property");
        assertThat(property.getDocCount(), equalTo(3l));
        propertyId = property.getAggregations().get("property_id");
        assertThat(propertyId.getBuckets().size(), equalTo(3));
        assertThat(propertyId.getBucketByKey("1").getDocCount(), equalTo(1l));
        assertThat(propertyId.getBucketByKey("2").getDocCount(), equalTo(1l));
        assertThat(propertyId.getBucketByKey("3").getDocCount(), equalTo(1l));
    }
}
