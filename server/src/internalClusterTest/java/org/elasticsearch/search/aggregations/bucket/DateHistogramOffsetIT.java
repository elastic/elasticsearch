/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * The serialisation of offsets for the date histogram aggregation was corrected in version 1.4 to allow negative offsets and as such the
 * serialisation of negative offsets in these tests would break in pre 1.4 versions.  These tests are separated from the other
 * DateHistogramTests so the AssertingLocalTransport for these tests can be set to only use versions 1.4 onwards while keeping the other
 * tests using all versions
 */
@ESIntegTestCase.SuiteScopeTestCase
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class DateHistogramOffsetIT extends ESIntegTestCase {

    private static final String DATE_FORMAT = "yyyy-MM-dd:hh-mm-ss";
    private static final DateFormatter FORMATTER = DateFormatter.forPattern(DATE_FORMAT);

    private ZonedDateTime date(String date) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(date));
    }

    @Before
    public void beforeEachTest() throws IOException {
        prepareCreate("idx2").setMapping("date", "type=date").get();
    }

    @After
    public void afterEachTest() throws IOException {
        internalCluster().wipeIndices("idx2");
    }

    private void prepareIndex(ZonedDateTime date, int numHours, int stepSizeHours, int idxIdStart) throws IOException,
        InterruptedException {

        IndexRequestBuilder[] reqs = new IndexRequestBuilder[numHours];
        for (int i = idxIdStart; i < idxIdStart + reqs.length; i++) {
            reqs[i - idxIdStart] = prepareIndex("idx2").setId("" + i)
                .setSource(jsonBuilder().startObject().timestampField("date", date).endObject());
            date = date.plusHours(stepSizeHours);
        }
        indexRandom(true, reqs);
    }

    public void testSingleValueWithPositiveOffset() throws Exception {
        prepareIndex(date("2014-03-11T00:00:00+00:00"), 5, 1, 0);

        assertResponse(
            prepareSearch("idx2").setQuery(matchAllQuery())
                .addAggregation(
                    dateHistogram("date_histo").field("date").offset("2h").format(DATE_FORMAT).fixedInterval(DateHistogramInterval.DAY)
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(5L));

                Histogram histo = response.getAggregations().get("date_histo");
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();
                assertThat(buckets.size(), equalTo(2));

                checkBucketFor(buckets.get(0), ZonedDateTime.of(2014, 3, 10, 2, 0, 0, 0, ZoneOffset.UTC), 2L);
                checkBucketFor(buckets.get(1), ZonedDateTime.of(2014, 3, 11, 2, 0, 0, 0, ZoneOffset.UTC), 3L);
            }
        );
    }

    public void testSingleValueWithNegativeOffset() throws Exception {
        prepareIndex(date("2014-03-11T00:00:00+00:00"), 5, -1, 0);

        assertResponse(
            prepareSearch("idx2").setQuery(matchAllQuery())
                .addAggregation(
                    dateHistogram("date_histo").field("date").offset("-2h").format(DATE_FORMAT).fixedInterval(DateHistogramInterval.DAY)
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(5L));

                Histogram histo = response.getAggregations().get("date_histo");
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();
                assertThat(buckets.size(), equalTo(2));

                checkBucketFor(buckets.get(0), ZonedDateTime.of(2014, 3, 9, 22, 0, 0, 0, ZoneOffset.UTC), 2L);
                checkBucketFor(buckets.get(1), ZonedDateTime.of(2014, 3, 10, 22, 0, 0, 0, ZoneOffset.UTC), 3L);
            }
        );
    }

    /**
     * Set offset so day buckets start at 6am. Index first 12 hours for two days, with one day gap.
     */
    public void testSingleValueWithOffsetMinDocCount() throws Exception {
        prepareIndex(date("2014-03-11T00:00:00+00:00"), 12, 1, 0);
        prepareIndex(date("2014-03-14T00:00:00+00:00"), 12, 1, 13);

        assertResponse(
            prepareSearch("idx2").setQuery(matchAllQuery())
                .addAggregation(
                    dateHistogram("date_histo").field("date")
                        .offset("6h")
                        .minDocCount(0)
                        .format(DATE_FORMAT)
                        .fixedInterval(DateHistogramInterval.DAY)
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(24L));

                Histogram histo = response.getAggregations().get("date_histo");
                List<? extends Histogram.Bucket> buckets = histo.getBuckets();
                assertThat(buckets.size(), equalTo(5));

                checkBucketFor(buckets.get(0), ZonedDateTime.of(2014, 3, 10, 6, 0, 0, 0, ZoneOffset.UTC), 6L);
                checkBucketFor(buckets.get(1), ZonedDateTime.of(2014, 3, 11, 6, 0, 0, 0, ZoneOffset.UTC), 6L);
                checkBucketFor(buckets.get(2), ZonedDateTime.of(2014, 3, 12, 6, 0, 0, 0, ZoneOffset.UTC), 0L);
                checkBucketFor(buckets.get(3), ZonedDateTime.of(2014, 3, 13, 6, 0, 0, 0, ZoneOffset.UTC), 6L);
                checkBucketFor(buckets.get(4), ZonedDateTime.of(2014, 3, 14, 6, 0, 0, 0, ZoneOffset.UTC), 6L);
            }
        );
    }

    /**
     * @param bucket the bucket to check assertions for
     * @param key the expected key
     * @param expectedSize the expected size of the bucket
     */
    private static void checkBucketFor(Histogram.Bucket bucket, ZonedDateTime key, long expectedSize) {
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(FORMATTER.format(key)));
        assertThat(((ZonedDateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(expectedSize));
    }
}
