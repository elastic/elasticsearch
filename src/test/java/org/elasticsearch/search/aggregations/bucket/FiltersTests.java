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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.matchAllFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filters;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class FiltersTests extends ElasticsearchIntegrationTest {

    static int numDocs, numTag1Docs, numTag2Docs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx2");
        numDocs = randomIntBetween(5, 20);
        numTag1Docs = randomIntBetween(1, numDocs - 1);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numTag1Docs; i++) {
            XContentBuilder source = jsonBuilder()
                    .startObject()
                    .field("value", i + 1)
                    .field("tag", "tag1")
                    .endObject();
            builders.add(client().prepareIndex("idx", "type", ""+i).setSource(source));
            if (randomBoolean()) {
                // randomly index the document twice so that we have deleted docs that match the filter
                builders.add(client().prepareIndex("idx", "type", ""+i).setSource(source));
            }
        }
        for (int i = numTag1Docs; i < numDocs; i++) {
            numTag2Docs++;
            XContentBuilder source = jsonBuilder()
                    .startObject()
                    .field("value", i)
                    .field("tag", "tag2")
                    .field("name", "name" + i)
                    .endObject();
            builders.add(client().prepareIndex("idx", "type", ""+i).setSource(source));
            if (randomBoolean()) {
                builders.add(client().prepareIndex("idx", "type", ""+i).setSource(source));
            }
        }
        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer").execute().actionGet();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i*2)
                    .endObject()));
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    @Test
    public void simple() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(
                        filters("tags")
                                .filter("tag1", termFilter("tag", "tag1"))
                                .filter("tag2", termFilter("tag", "tag2")))
                .execute().actionGet();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        assertThat(filters.getName(), equalTo("tags"));

        assertThat(filters.getBuckets().size(), equalTo(2));

        Filters.Bucket bucket = filters.getBucketByKey("tag1");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

        bucket = filters.getBucketByKey("tag2");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));
    }

    // See NullPointer issue when filters are empty:
    // https://github.com/elasticsearch/elasticsearch/issues/8438
    @Test
    public void emptyFilterDeclarations() throws Exception {
        FilterBuilder emptyFilter = new AndFilterBuilder();
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(filters("tags").filter("all", emptyFilter).filter("tag1", termFilter("tag", "tag1"))).execute()
                .actionGet();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        Filters.Bucket allBucket = filters.getBucketByKey("all");
        assertThat(allBucket.getDocCount(), equalTo((long) numDocs));

        Filters.Bucket bucket = filters.getBucketByKey("tag1");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));
    }

    @Test
    public void withSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(
                        filters("tags")
                                .filter("tag1", termFilter("tag", "tag1"))
                                .filter("tag2", termFilter("tag", "tag2"))
                                .subAggregation(avg("avg_value").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        assertThat(filters.getName(), equalTo("tags"));

        assertThat(filters.getBuckets().size(), equalTo(2));
        Object[] propertiesKeys = (Object[]) filters.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) filters.getProperty("_count");
        Object[] propertiesCounts = (Object[]) filters.getProperty("avg_value.value");

        Filters.Bucket bucket = filters.getBucketByKey("tag1");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));
        long sum = 0;
        for (int i = 0; i < numTag1Docs; ++i) {
            sum += i + 1;
        }
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        Avg avgValue = bucket.getAggregations().get("avg_value");
        assertThat(avgValue, notNullValue());
        assertThat(avgValue.getName(), equalTo("avg_value"));
        assertThat(avgValue.getValue(), equalTo((double) sum / numTag1Docs));
        assertThat((String) propertiesKeys[0], equalTo("tag1"));
        assertThat((long) propertiesDocCounts[0], equalTo((long) numTag1Docs));
        assertThat((double) propertiesCounts[0], equalTo((double) sum / numTag1Docs));

        bucket = filters.getBucketByKey("tag2");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));
        sum = 0;
        for (int i = numTag1Docs; i < numDocs; ++i) {
            sum += i;
        }
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        avgValue = bucket.getAggregations().get("avg_value");
        assertThat(avgValue, notNullValue());
        assertThat(avgValue.getName(), equalTo("avg_value"));
        assertThat(avgValue.getValue(), equalTo((double) sum / numTag2Docs));
        assertThat((String) propertiesKeys[1], equalTo("tag2"));
        assertThat((long) propertiesDocCounts[1], equalTo((long) numTag2Docs));
        assertThat((double) propertiesCounts[1], equalTo((double) sum / numTag2Docs));
    }

    @Test
    public void withContextBasedSubAggregation() throws Exception {

        try {
            client().prepareSearch("idx")
                    .addAggregation(
                            filters("tags")
                                    .filter("tag1", termFilter("tag", "tag1"))
                                    .filter("tag2", termFilter("tag", "tag2"))
                                    .subAggregation(avg("avg_value"))
                    )
                    .execute().actionGet();

            fail("expected execution to fail - an attempt to have a context based numeric sub-aggregation, but there is not value source" +
                    "context which the sub-aggregation can inherit");

        } catch (ElasticsearchException ese) {
        }
    }

    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0)
                        .subAggregation(filters("filters").filter("all", matchAllFilter())))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Filters filters = bucket.getAggregations().get("filters");
        assertThat(filters, notNullValue());
        Filters.Bucket all = filters.getBucketByKey("all");
        assertThat(all, Matchers.notNullValue());
        assertThat(all.getKeyAsString(), equalTo("all"));
        assertThat(all.getDocCount(), is(0l));
    }
    
    @Test
    public void simple_nonKeyed() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(
                        filters("tags")
                                .filter(termFilter("tag", "tag1"))
                                .filter(termFilter("tag", "tag2")))
                .execute().actionGet();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        assertThat(filters.getName(), equalTo("tags"));

        assertThat(filters.getBuckets().size(), equalTo(2));

        Collection<? extends Filters.Bucket> buckets = filters.getBuckets();
        Iterator<? extends Filters.Bucket> itr = buckets.iterator();

        Filters.Bucket bucket = itr.next();
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

        bucket = itr.next();
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));
    }

}
