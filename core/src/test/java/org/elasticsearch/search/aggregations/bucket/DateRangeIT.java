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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.bucket.DateScriptMocks.DateScriptsMockPlugin;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.Range.Bucket;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
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
@ESIntegTestCase.SuiteScopeTestCase
public class DateRangeIT extends ESIntegTestCase {

    private static IndexRequestBuilder indexDoc(int month, int day, int value) throws Exception {
        return client().prepareIndex("idx", "type").setSource(jsonBuilder()
                .startObject()
                .field("value", value)
                .field("date", date(month, day))
                .startArray("dates").value(date(month, day)).value(date(month + 1, day + 1)).endArray()
                .endObject());
    }

    private static DateTime date(int month, int day) {
        return date(month, day, DateTimeZone.UTC);
    }

    private static DateTime date(int month, int day, DateTimeZone timezone) {
        return new DateTime(2012, month, day, 0, 0, timezone);
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                DateScriptsMockPlugin.class);
    }

    public void testDateMath() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("fieldname", "date");
        DateRangeAggregationBuilder rangeBuilder = dateRange("range");
        if (randomBoolean()) {
            rangeBuilder.field("date");
        } else {
            rangeBuilder.script(new Script(DateScriptMocks.ExtractFieldScript.NAME, ScriptType.INLINE, "native", params));
        }
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        rangeBuilder.addUnboundedTo("a long time ago", "now-50y").addRange("recently", "now-50y", "now-1y")
                                .addUnboundedFrom("last year", "now-1y").timeZone(DateTimeZone.forID("EST"))).execute().actionGet();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.getBuckets().size(), equalTo(3));

        // TODO: use diamond once JI-9019884 is fixed
        List<Range.Bucket> buckets = new ArrayList<>(range.getBuckets());

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

    public void testSingleValueField() throws Exception {
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
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
    }

    public void testSingleValueFieldWithStringDates() throws Exception {
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
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
    }

    public void testSingleValueFieldWithStringDatesWithCustomFormat() throws Exception {
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
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15-2012-03-15"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
    }

    public void testSingleValueFieldWithDateMath() throws Exception {
        DateTimeZone timezone = randomDateTimeZone();
        int timeZoneOffset = timezone.getOffset(date(2, 15));
        // if time zone is UTC (or equivalent), time zone suffix is "Z", else something like "+03:00", which we get with the "ZZ" format
        String feb15Suffix = timeZoneOffset == 0 ? "Z" : date(2,15, timezone).toString("ZZ");
        String mar15Suffix = timeZoneOffset == 0 ? "Z" : date(3,15, timezone).toString("ZZ");
        long expectedFirstBucketCount = timeZoneOffset < 0 ? 3L : 2L;

        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("2012-02-15")
                        .addRange("2012-02-15", "2012-02-15||+1M")
                        .addUnboundedFrom("2012-02-15||+1M")
                        .timeZone(timezone))
                .execute().actionGet();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000" + feb15Suffix));
        assertThat(((DateTime) bucket.getFrom()), nullValue());
        assertThat(((DateTime) bucket.getTo()), equalTo(date(2, 15, timezone).toDateTime(DateTimeZone.UTC)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000" + feb15Suffix));
        assertThat(bucket.getDocCount(), equalTo(expectedFirstBucketCount));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000" + feb15Suffix +
                "-2012-03-15T00:00:00.000" + mar15Suffix));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15, timezone).toDateTime(DateTimeZone.UTC)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15, timezone).toDateTime(DateTimeZone.UTC)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000" + feb15Suffix));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000" + mar15Suffix));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000" + mar15Suffix + "-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15, timezone).toDateTime(DateTimeZone.UTC)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000" + mar15Suffix));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 2L - expectedFirstBucketCount));
    }

    public void testSingleValueFieldWithCustomKey() throws Exception {
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
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r2"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r3"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
    }

    /*
        Jan 2,      1
        Feb 2,      2
        Feb 15,     3
        Mar 2,      4
        Mar 15,     5
        Mar 23,     6
     */

    public void testSingleValuedFieldWithSubAggregation() throws Exception {
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
        assertThat(range.getProperty("_bucket_count"), equalTo(3));
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
        assertThat(bucket.getDocCount(), equalTo(2L));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 1 + 2));
        assertThat((String) propertiesKeys[0], equalTo("r1"));
        assertThat((long) propertiesDocCounts[0], equalTo(2L));
        assertThat((double) propertiesCounts[0], equalTo((double) 1 + 2));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r2"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 3 + 4));
        assertThat((String) propertiesKeys[1], equalTo("r2"));
        assertThat((long) propertiesDocCounts[1], equalTo(2L));
        assertThat((double) propertiesCounts[1], equalTo((double) 3 + 4));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r3"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat((String) propertiesKeys[2], equalTo("r3"));
        assertThat((long) propertiesDocCounts[2], equalTo(numDocs - 4L));
    }



    /*
        Jan 2,  Feb 3,      1
        Feb 2,  Mar 3,      2
        Feb 15, Mar 16,     3
        Mar 2,  Apr 3,      4
        Mar 15, Apr 16      5
        Mar 23, Apr 24      6
     */

    public void testMultiValuedField() throws Exception {
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
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(3L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 2L));
    }

    /*
        Feb 2,  Mar 3,      1
        Mar 2,  Apr 3,      2
        Mar 15, Apr 16,     3
        Apr 2,  May 3,      4
        Apr 15, May 16      5
        Apr 23, May 24      6
     */


    public void testMultiValuedFieldWithValueScript() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("fieldname", "dates");
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("dates")
                                .script(new Script(DateScriptMocks.PlusOneMonthScript.NAME, ScriptType.INLINE, "native", params))
                                .addUnboundedTo(date(2, 15)).addRange(date(2, 15), date(3, 15)).addUnboundedFrom(date(3, 15))).execute()
                .actionGet();

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
        assertThat(bucket.getDocCount(), equalTo(1L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 1L));
    }



    /*
        Feb 2,  Mar 3,      1
        Mar 2,  Apr 3,      2
        Mar 15, Apr 16,     3
        Apr 2,  May 3,      4
        Apr 15, May 16      5
        Apr 23, May 24      6
     */

    public void testScriptSingleValue() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("fieldname", "date");
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .script(new Script(DateScriptMocks.ExtractFieldScript.NAME, ScriptType.INLINE, "native", params))
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
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
    }





    /*
        Jan 2,  Feb 3,      1
        Feb 2,  Mar 3,      2
        Feb 15, Mar 16,     3
        Mar 2,  Apr 3,      4
        Mar 15, Apr 16      5
        Mar 23, Apr 24      6
     */

    public void testScriptMultiValued() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("fieldname", "dates");
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        dateRange("range").script(new Script(DateScriptMocks.ExtractFieldScript.NAME, ScriptType.INLINE, "native", params))
                        .addUnboundedTo(date(2, 15)).addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15))).execute().actionGet();

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
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(3L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 2L));
    }

    public void testUnmapped() throws Exception {
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
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(0L));
    }

    public void testUnmappedWithStringDates() throws Exception {
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
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(0L));
    }

    public void testPartiallyUnmapped() throws Exception {
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
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((DateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((DateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((DateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1L).minDocCount(0)
                        .subAggregation(dateRange("date_range").field("value").addRange("0-1", 0, 1)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Range dateRange = bucket.getAggregations().get("date_range");
        // TODO: use diamond once JI-9019884 is fixed
        List<Range.Bucket> buckets = new ArrayList<>(dateRange.getBuckets());
        assertThat(dateRange, Matchers.notNullValue());
        assertThat(dateRange.getName(), equalTo("date_range"));
        assertThat(buckets.size(), is(1));
        assertThat((String) buckets.get(0).getKey(), equalTo("0-1"));
        assertThat(((DateTime) buckets.get(0).getFrom()).getMillis(), equalTo(0L));
        assertThat(((DateTime) buckets.get(0).getTo()).getMillis(), equalTo(1L));
        assertThat(buckets.get(0).getDocCount(), equalTo(0L));
        assertThat(buckets.get(0).getAggregations().asList().isEmpty(), is(true));
    }
}
