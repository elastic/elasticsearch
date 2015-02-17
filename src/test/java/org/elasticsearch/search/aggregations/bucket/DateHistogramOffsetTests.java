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
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.transport.AssertingLocalTransport;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * The serialisation of offsets for the date histogram aggregation was corrected in version 1.4 to allow negative offsets and as such the
 * serialisation of negative offsets in these tests would break in pre 1.4 versions.  These tests are separated from the other DateHistogramTests so the
 * AssertingLocalTransport for these tests can be set to only use versions 1.4 onwards while keeping the other tests using all versions
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
@ElasticsearchIntegrationTest.ClusterScope(scope=ElasticsearchIntegrationTest.Scope.SUITE)
public class DateHistogramOffsetTests extends ElasticsearchIntegrationTest {

    private static final String DATE_FORMAT = "yyyy-MM-dd:hh-mm-ss";

    private DateTime date(String date) {
        return DateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime(date);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(AssertingLocalTransport.ASSERTING_TRANSPORT_MIN_VERSION_KEY, Version.V_1_4_0_Beta1).build();
    }

    @Before
    public void beforeEachTest() throws IOException {
        prepareCreate("idx2").addMapping("type", "date", "type=date").execute().actionGet();
    }

    @After
    public void afterEachTest() throws IOException {
        internalCluster().wipeIndices("idx2");
    }

    private void prepareIndex(DateTime date, int numHours, int stepSizeHours, int idxIdStart) throws IOException, InterruptedException, ExecutionException {
        IndexRequestBuilder[] reqs = new IndexRequestBuilder[numHours];
        for (int i = idxIdStart; i < idxIdStart + reqs.length; i++) {
            reqs[i - idxIdStart] = client().prepareIndex("idx2", "type", "" + i).setSource(jsonBuilder().startObject().field("date", date).endObject());
            date = date.plusHours(stepSizeHours);
        }
        indexRandom(true, reqs);
    }

    @Test
    public void singleValue_WithPositiveOffset() throws Exception {
        prepareIndex(date("2014-03-11T00:00:00+00:00"), 5, 1, 0);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .offset("2h")
                        .format(DATE_FORMAT)
                        .interval(DateHistogramInterval.DAY))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        Histogram histo = response.getAggregations().get("date_histo");
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        checkBucketFor(buckets.get(0), new DateTime(2014, 3, 10, 2, 0, DateTimeZone.UTC), 2l);
        checkBucketFor(buckets.get(1), new DateTime(2014, 3, 11, 2, 0, DateTimeZone.UTC), 3l);
    }

    @Test
    public void singleValue_WithNegativeOffset() throws Exception {
        prepareIndex(date("2014-03-11T00:00:00+00:00"), 5, -1, 0);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .offset("-2h")
                        .format(DATE_FORMAT)
                        .interval(DateHistogramInterval.DAY))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(5l));

        Histogram histo = response.getAggregations().get("date_histo");
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        checkBucketFor(buckets.get(0), new DateTime(2014, 3, 9, 22, 0, DateTimeZone.UTC), 2l);
        checkBucketFor(buckets.get(1), new DateTime(2014, 3, 10, 22, 0, DateTimeZone.UTC), 3l);
    }

    /**
     * Set offset so day buckets start at 6am. Index first 12 hours for two days, with one day gap.
     * @throws Exception
     */
    @Test
    public void singleValue_WithOffset_MinDocCount() throws Exception {
        prepareIndex(date("2014-03-11T00:00:00+00:00"), 12, 1, 0);
        prepareIndex(date("2014-03-14T00:00:00+00:00"), 12, 1, 13);

        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(dateHistogram("date_histo")
                        .field("date")
                        .offset("6h")
                        .minDocCount(0)
                        .format(DATE_FORMAT)
                        .interval(DateHistogramInterval.DAY))
                .execute().actionGet();

        assertThat(response.getHits().getTotalHits(), equalTo(24l));

        Histogram histo = response.getAggregations().get("date_histo");
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(5));

        checkBucketFor(buckets.get(0), new DateTime(2014, 3, 10, 6, 0, DateTimeZone.UTC), 6L);
        checkBucketFor(buckets.get(1), new DateTime(2014, 3, 11, 6, 0, DateTimeZone.UTC), 6L);
        checkBucketFor(buckets.get(2), new DateTime(2014, 3, 12, 6, 0, DateTimeZone.UTC), 0L);
        checkBucketFor(buckets.get(3), new DateTime(2014, 3, 13, 6, 0, DateTimeZone.UTC), 6L);
        checkBucketFor(buckets.get(4), new DateTime(2014, 3, 14, 6, 0, DateTimeZone.UTC), 6L);
    }

    /**
     * @param bucket the bucket to check asssertions for
     * @param key the expected key
     * @param expectedSize the expected size of the bucket
     */
    private static void checkBucketFor(Histogram.Bucket bucket, DateTime key, long expectedSize) {
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(key.toString(DATE_FORMAT)));
        assertThat(((DateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(expectedSize));
    }
}
