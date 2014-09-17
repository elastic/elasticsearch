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

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.transport.AssertingLocalTransport;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.hamcrest.Matchers.equalTo;

/**
 * The serialisation of pre and post offsets for the date histogram aggregation was corrected in version 1.4 to allow negative offsets and as such the
 * serialisation of negative offsets in these tests would break in pre 1.4 versions.  These tests are separated from the other DateHistogramTests so the 
 * AssertingLocalTransport for these tests can be set to only use versions 1.4 onwards while keeping the other tests using all versions
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
@ElasticsearchIntegrationTest.ClusterScope(scope=ElasticsearchIntegrationTest.Scope.SUITE)
public class DateHistogramOffsetTests extends ElasticsearchIntegrationTest {

    private DateTime date(String date) {
        return DateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime(date);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(AssertingLocalTransport.ASSERTING_TRANSPORT_MIN_VERSION_KEY, Version.V_1_4_0_Beta1).build();
    }

    @After
    public void afterEachTest() throws IOException {
        internalCluster().wipeIndices("idx2");
    }

    @Test
    public void singleValue_WithPreOffset() throws Exception {
        prepareCreate("idx2").addMapping("type", "date", "type=date").execute().actionGet();
        IndexRequestBuilder[] reqs = new IndexRequestBuilder[5];
        DateTime date = date("2014-03-11T00:00:00+00:00");
        for (int i = 0; i < reqs.length; i++) {
            reqs[i] = client().prepareIndex("idx2", "type", "" + i).setSource(jsonBuilder().startObject().field("date", date).endObject());
            date = date.plusHours(1);
        }
        indexRandom(true, reqs);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .preOffset("-2h")
                        .interval(DateHistogram.Interval.DAY)
                        .format("yyyy-MM-dd"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        DateHistogram histo = response.getAggregations().get("date_histo");
        Collection<? extends DateHistogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        DateHistogram.Bucket bucket = histo.getBucketByKey("2014-03-10");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = histo.getBucketByKey("2014-03-11");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void singleValue_WithPreOffset_MinDocCount() throws Exception {
        prepareCreate("idx2").addMapping("type", "date", "type=date").execute().actionGet();
        IndexRequestBuilder[] reqs = new IndexRequestBuilder[5];
        DateTime date = date("2014-03-11T00:00:00+00:00");
        for (int i = 0; i < reqs.length; i++) {
            reqs[i] = client().prepareIndex("idx2", "type", "" + i).setSource(jsonBuilder().startObject().field("date", date).endObject());
            date = date.plusHours(1);
        }
        indexRandom(true, reqs);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .preOffset("-2h")
                        .minDocCount(0)
                        .interval(DateHistogram.Interval.DAY)
                        .format("yyyy-MM-dd"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        DateHistogram histo = response.getAggregations().get("date_histo");
        Collection<? extends DateHistogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        DateHistogram.Bucket bucket = histo.getBucketByKey("2014-03-10");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = histo.getBucketByKey("2014-03-11");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void singleValue_WithPostOffset() throws Exception {
        prepareCreate("idx2").addMapping("type", "date", "type=date").execute().actionGet();
        IndexRequestBuilder[] reqs = new IndexRequestBuilder[5];
        DateTime date = date("2014-03-11T00:00:00+00:00");
        for (int i = 0; i < reqs.length; i++) {
            reqs[i] = client().prepareIndex("idx2", "type", "" + i).setSource(jsonBuilder().startObject().field("date", date).endObject());
            date = date.plusHours(6);
        }
        indexRandom(true, reqs);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .postOffset("2d")
                        .interval(DateHistogram.Interval.DAY)
                        .format("yyyy-MM-dd"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        DateHistogram histo = response.getAggregations().get("date_histo");
        Collection<? extends DateHistogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        DateHistogram.Bucket bucket = histo.getBucketByKey("2014-03-13");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(4l));

        bucket = histo.getBucketByKey("2014-03-14");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(1l));
    }

    @Test
    public void singleValue_WithPostOffset_MinDocCount() throws Exception {
        prepareCreate("idx2").addMapping("type", "date", "type=date").execute().actionGet();
        IndexRequestBuilder[] reqs = new IndexRequestBuilder[5];
        DateTime date = date("2014-03-11T00:00:00+00:00");
        for (int i = 0; i < reqs.length; i++) {
            reqs[i] = client().prepareIndex("idx2", "type", "" + i).setSource(jsonBuilder().startObject().field("date", date).endObject());
            date = date.plusHours(6);
        }
        indexRandom(true, reqs);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .postOffset("2d")
                        .minDocCount(0)
                        .interval(DateHistogram.Interval.DAY)
                        .format("yyyy-MM-dd"))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        DateHistogram histo = response.getAggregations().get("date_histo");
        Collection<? extends DateHistogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        DateHistogram.Bucket bucket = histo.getBucketByKey("2014-03-13");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(4l));

        bucket = histo.getBucketByKey("2014-03-14");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo(1l));
    }
}
