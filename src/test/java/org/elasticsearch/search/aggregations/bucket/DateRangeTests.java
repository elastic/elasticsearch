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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.Range.Bucket;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class DateRangeTests extends ElasticsearchIntegrationTest {

    private static IndexRequestBuilder indexDoc(int month, int day, int value) throws Exception {
        return client().prepareIndex("idx", "type").setSource(jsonBuilder()
                .startObject()
                .field("value", value)
                .field("date", date(month, day))
                .startArray("dates").value(date(month, day)).value(date(month + 1, day + 1)).endArray()
                .endObject());
    }

    private static DateTime date(int month, int day) {
        return new DateTime(2012, month, day, 0, 0, DateTimeZone.UTC);
    }

    private static int numDocs;
    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        numDocs = randomIntBetween(7, 20);

        List<IndexRequestBuilder> docs = new ArrayList<>();
        docs.addAll(Arrays.asList(
                indexDoc(1, 2, 1),  // Jan 2
                indexDoc(2, 2, 2),  // Feb 2
                indexDoc(2, 15, 3), // Feb 15
                indexDoc(3, 2, 4),  // Mar 2
                indexDoc(3, 15, 5), // Mar 15
                indexDoc(3, 23, 6))); // Mar 23

        // dummy docs
        for (int i = docs.size(); i < numDocs; ++i) {
            docs.add(indexDoc(randomIntBetween(6, 10), randomIntBetween(1, 20), randomInt(100)));
        }
        assertAcked(prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer"));
        for (int i = 0; i < 2; i++) {
            docs.add(client().prepareIndex("empty_bucket_idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i*2)
                    .endObject()));
        }
        indexRandom(true, docs);
        ensureSearchable();
    }

    @Test
    public void dateMath() throws Exception {
        DateRangeBuilder rangeBuilder = dateRange("range");
        if (randomBoolean()) {
            rangeBuilder.field("date");
        } else {
            rangeBuilder.script("doc['date'].value");
        }
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(rangeBuilder
                        .addUnboundedTo("a long time ago", "now-50y")
                        .addRange("recently", "now-50y", "now-1y")
                        .addUnboundedFrom("last year", "now-1y"))
                .execute().actionGet();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.getBuckets().size(), equalTo(3));

        // TODO: use diamond once JI-9019884 is fixed
        List<Range.Bucket> buckets = new ArrayList<Range.Bucket>(range.getBuckets());

        Range.Bucket bucket = buckets.get(0);
        assertThat((String) bucket.getKey(), equalTo("a long time ago"));
        assertThat(bucket.getKeyAsString(), equalTo("a long time ago"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(1);
        assertThat((String) bucket.getKey(), equalTo("recently"));
        assertThat(bucket.getKeyAsString(), equalTo("recently"));
        assertThat(bucket.getDocCount(), equalTo((long) numDocs));

        bucket = buckets.get(2);
        assertThat((String) bucket.getKey(), equalTo("last year"));
        assertThat(bucket.getKeyAsString(), equalTo("last year"));
        assertThat(bucket.getDocCount(), equalTo(0L));
    }

    @Test
    public void singleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
    }

    @Test
    public void singleValueField_WithStringDates() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("2012-02-15")
                        .addRange("2012-02-15", "2012-03-15")
                        .addUnboundedFrom("2012-03-15"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
    }

    @Test
    public void singleValueField_WithStringDates_WithCustomFormat() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .format("yyyy-MM-dd")
                        .addUnboundedTo("2012-02-15")
                        .addRange("2012-02-15", "2012-03-15")
                        .addUnboundedFrom("2012-03-15"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15-2012-03-15"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
    }

    @Test
    public void singleValueField_WithDateMath() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("2012-02-15")
                        .addRange("2012-02-15", "2012-02-15||+1M")
                        .addUnboundedFrom("2012-02-15||+1M"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
    }

    @Test
    public void singleValueField_WithCustomKey() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("r1", date(2, 15))
                        .addRange("r2", date(2, 15), date(3, 15))
                        .addUnboundedFrom("r3", date(3, 15)))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r1"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r2"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r3"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
    }

    /*
        Jan 2,      1
        Feb 2,      2
        Feb 15,     3
        Mar 2,      4
        Mar 15,     5
        Mar 23,     6
     */

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("r1", date(2, 15))
                        .addRange("r2", date(2, 15), date(3, 15))
                        .addUnboundedFrom("r3", date(3, 15))
                        .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Object[] propertiesKeys = (Object[]) range.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) range.getProperty("_count");
        Object[] propertiesCounts = (Object[]) range.getProperty("sum.value");

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r1"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 1 + 2));
        assertThat((String) propertiesKeys[0], equalTo("r1"));
        assertThat((long) propertiesDocCounts[0], equalTo(2l));
        assertThat((double) propertiesCounts[0], equalTo((double) 1 + 2));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r2"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 3 + 4));
        assertThat((String) propertiesKeys[1], equalTo("r2"));
        assertThat((long) propertiesDocCounts[1], equalTo(2l));
        assertThat((double) propertiesCounts[1], equalTo((double) 3 + 4));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r3"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat((String) propertiesKeys[2], equalTo("r3"));
        assertThat((long) propertiesDocCounts[2], equalTo(numDocs - 4l));
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("r1", date(2, 15))
                        .addRange("r2", date(2, 15), date(3, 15))
                        .addUnboundedFrom("r3", date(3, 15))
                        .subAggregation(min("min")))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r1"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Min min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(1, 2).getMillis()));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r2"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));
        min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(2, 15).getMillis()));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r3"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
        min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(3, 15).getMillis()));
    }

    /*
        Jan 2,  Feb 3,      1
        Feb 2,  Mar 3,      2
        Feb 15, Mar 16,     3
        Mar 2,  Apr 3,      4
        Mar 15, Apr 16      5
        Mar 23, Apr 24      6
     */

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("dates")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 2l));
    }

    /*
        Feb 2,  Mar 3,      1
        Mar 2,  Apr 3,      2
        Mar 15, Apr 16,     3
        Apr 2,  May 3,      4
        Apr 15, May 16      5
        Apr 23, May 24      6
     */


    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("dates")
                        .script("new DateTime(_value.longValue(), DateTimeZone.UTC).plusMonths(1).getMillis()")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(1l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 1l));
    }

    /*
        Feb 2,  Mar 3,      1
        Mar 2,  Apr 3,      2
        Mar 15, Apr 16,     3
        Apr 2,  May 3,      4
        Apr 15, May 16      5
        Apr 23, May 24      6
     */

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("dates")
                        .script("new DateTime(_value.longValue(), DateTimeZone.UTC).plusMonths(1).getMillis()")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15))
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) date(3, 3).getMillis()));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) date(4, 3).getMillis()));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 1l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .script("doc['date'].value")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .script("doc['date'].value")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15))
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) date(2, 2).getMillis()));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) date(3, 2).getMillis()));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
    }

    /*
        Jan 2,  Feb 3,      1
        Feb 2,  Mar 3,      2
        Feb 15, Mar 16,     3
        Mar 2,  Apr 3,      4
        Mar 15, Apr 16      5
        Mar 23, Apr 24      6
     */

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .script("doc['dates'].values")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 2l));
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .script("doc['dates'].values")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15))
                        .subAggregation(min("min")))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Min min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(1, 2).getMillis()));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(3l));
        min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(2, 2).getMillis()));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 2l));
        min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(2, 15).getMillis()));
    }

    @Test
    public void unmapped() throws Exception {
        client().admin().cluster().prepareHealth("idx_unmapped").setWaitForYellowStatus().execute().actionGet();

        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(0l));
    }

    @Test
    public void unmapped_WithStringDates() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("2012-02-15")
                        .addRange("2012-02-15", "2012-03-15")
                        .addUnboundedFrom("2012-03-15"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(0l));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4l));
    }

    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0).subAggregation(dateRange("date_range").addRange("0-1", 0, 1)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Range dateRange = bucket.getAggregations().get("date_range");
        // TODO: use diamond once JI-9019884 is fixed
        List<Range.Bucket> buckets = new ArrayList<Range.Bucket>(dateRange.getBuckets());
        assertThat(dateRange, Matchers.notNullValue());
        assertThat(dateRange.getName(), equalTo("date_range"));
        assertThat(buckets.size(), is(1));
        assertThat((String) buckets.get(0).getKey(), equalTo("0-1"));
        assertThat(((DateTime) buckets.get(0).getFrom()).getMillis(), equalTo(0l));
        assertThat(((DateTime) buckets.get(0).getTo()).getMillis(), equalTo(1l));
        assertThat(buckets.get(0).getDocCount(), equalTo(0l));
        assertThat(buckets.get(0).getAggregations().asList().isEmpty(), is(true));

    }


}
