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
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.Range.Bucket;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class DateRangeIT extends ESIntegTestCase {

    private static IndexRequestBuilder indexDoc(int month, int day, int value) throws Exception {
        return client().prepareIndex("idx", "type").setSource(jsonBuilder()
                .startObject()
                .field("value", value)
                .timeField("date", date(month, day))
                .startArray("dates").timeValue(date(month, day)).timeValue(date(month + 1, day + 1)).endArray()
                .endObject());
    }

    private static ZonedDateTime date(int month, int day) {
        return date(month, day, ZoneOffset.UTC);
    }

    private static ZonedDateTime date(int month, int day, ZoneId timezone) {
        return ZonedDateTime.of(2012, month, day, 0, 0, 0, 0, timezone);
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
        return Collections.singleton(DateScriptMocksPlugin.class);
    }

    public void testDateMath() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("fieldname", "date");
        DateRangeAggregationBuilder rangeBuilder = dateRange("range");
        if (randomBoolean()) {
            rangeBuilder.field("date");
        } else {
            rangeBuilder.script(new Script(ScriptType.INLINE, "mockscript", DateScriptMocksPlugin.EXTRACT_FIELD, params));
        }
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        rangeBuilder.addUnboundedTo("a long time ago", "now-50y").addRange("recently", "now-50y", "now-1y")
                                .addUnboundedFrom("last year", "now-1y").timeZone(ZoneId.of("Etc/GMT+5"))).get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.getBuckets().size(), equalTo(3));

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
                .get();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
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
                .get();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
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
                .get();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15-2012-03-15"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15-*"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
    }

    public void testSingleValueFieldWithDateMath() throws Exception {
        ZoneId timezone = randomZone();
        int timeZoneOffset = timezone.getRules().getOffset(date(2, 15).toInstant()).getTotalSeconds();
        //there is a daylight saving time change on 11th March so suffix will be different
        String feb15Suffix = timeZoneOffset == 0 ? "Z" : date(2,15, timezone).format(DateTimeFormatter.ofPattern("xxx", Locale.ROOT));
        String mar15Suffix = timeZoneOffset == 0 ? "Z" : date(3,15, timezone).format(DateTimeFormatter.ofPattern("xxx", Locale.ROOT));
        long expectedFirstBucketCount = timeZoneOffset < 0 ? 3L : 2L;

        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("2012-02-15")
                        .addRange("2012-02-15", "2012-02-15||+1M")
                        .addUnboundedFrom("2012-02-15||+1M")
                        .timeZone(timezone))
                .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000" + feb15Suffix));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15, timezone).withZoneSameInstant(ZoneOffset.UTC)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000" + feb15Suffix));
        assertThat(bucket.getDocCount(), equalTo(expectedFirstBucketCount));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000" + feb15Suffix +
                "-2012-03-15T00:00:00.000" + mar15Suffix));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15, timezone).withZoneSameInstant(ZoneOffset.UTC)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15, timezone).withZoneSameInstant(ZoneOffset.UTC)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000" + feb15Suffix));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000" + mar15Suffix));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000" + mar15Suffix + "-*"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15, timezone).withZoneSameInstant(ZoneOffset.UTC)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
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
                .get();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r1"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r2"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r3"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
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
                .get();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        assertThat(((InternalAggregation)range).getProperty("_bucket_count"), equalTo(3));
        Object[] propertiesKeys = (Object[]) ((InternalAggregation)range).getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) ((InternalAggregation)range).getProperty("_count");
        Object[] propertiesCounts = (Object[]) ((InternalAggregation)range).getProperty("sum.value");

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r1"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
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
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
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
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
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
                .get();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(3L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
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
                                .script(new Script(ScriptType.INLINE, "mockscript", DateScriptMocksPlugin.DOUBLE_PLUS_ONE_MONTH, params))
                                .addUnboundedTo(date(2, 15)).addRange(date(2, 15), date(3, 15)).addUnboundedFrom(date(3, 15))).get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(1L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
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
                        .script(new Script(ScriptType.INLINE, "mockscript", DateScriptMocksPlugin.EXTRACT_FIELD, params))
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .get();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
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
                        dateRange("range").script(new Script(ScriptType.INLINE, "mockscript", DateScriptMocksPlugin.EXTRACT_FIELD, params))
                        .addUnboundedTo(date(2, 15)).addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15))).get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(3L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 2L));
    }

    public void testUnmapped() throws Exception {
        client().admin().cluster().prepareHealth("idx_unmapped").setWaitForYellowStatus().get();

        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .get();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
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
                .get();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
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
                .get();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), nullValue());
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(2, 15)));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(2, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), equalTo(date(3, 15)));
        assertThat(bucket.getFromAsString(), equalTo("2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(((ZonedDateTime) bucket.getFrom()), equalTo(date(3, 15)));
        assertThat(((ZonedDateTime) bucket.getTo()), nullValue());
        assertThat(bucket.getFromAsString(), equalTo("2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(numDocs - 4L));
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1L).minDocCount(0)
                        .subAggregation(dateRange("date_range").field("value").addRange("0-1", 0, 1)))
                .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Range dateRange = bucket.getAggregations().get("date_range");
        List<Range.Bucket> buckets = new ArrayList<>(dateRange.getBuckets());
        assertThat(dateRange, Matchers.notNullValue());
        assertThat(dateRange.getName(), equalTo("date_range"));
        assertThat(buckets.size(), is(1));
        assertThat((String) buckets.get(0).getKey(), equalTo("0-1"));
        assertThat(((ZonedDateTime) buckets.get(0).getFrom()).toInstant().toEpochMilli(), equalTo(0L));
        assertThat(((ZonedDateTime) buckets.get(0).getTo()).toInstant().toEpochMilli(), equalTo(1L));
        assertThat(buckets.get(0).getDocCount(), equalTo(0L));
        assertThat(buckets.get(0).getAggregations().asList().isEmpty(), is(true));
    }

    public void testNoRangesInQuery()  {
        try {
            client().prepareSearch("idx")
                .addAggregation(dateRange("my_date_range_agg").field("value"))
                .get();
            fail();
        } catch (SearchPhaseExecutionException spee){
            Throwable rootCause = spee.getCause().getCause();
            assertThat(rootCause, instanceOf(IllegalArgumentException.class));
            assertEquals(rootCause.getMessage(), "No [ranges] specified for the [my_date_range_agg] aggregation");
        }
    }

    /**
     * Make sure that a request using a script does not get cached and a request
     * not using a script does get cached.
     */
    public void testDontCacheScripts() throws Exception {
        assertAcked(prepareCreate("cache_test_idx").addMapping("type", "date", "type=date")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
                .get());
        indexRandom(true,
                client().prepareIndex("cache_test_idx", "type", "1")
                        .setSource(jsonBuilder().startObject().timeField("date", date(1, 1)).endObject()),
                client().prepareIndex("cache_test_idx", "type", "2")
                        .setSource(jsonBuilder().startObject().timeField("date", date(2, 1)).endObject()));

        // Make sure we are starting with a clear cache
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(0L));

        // Test that a request using a script does not get cached
        Map<String, Object> params = new HashMap<>();
        params.put("fieldname", "date");
        SearchResponse r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(dateRange("foo").field("date")
                .script(new Script(ScriptType.INLINE, "mockscript", DateScriptMocksPlugin.DOUBLE_PLUS_ONE_MONTH, params))
                .addRange(ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
                    ZonedDateTime.of(2013, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)))
                .get();
        assertSearchResponse(r);

        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(0L));

        // To make sure that the cache is working test that a request not using
        // a script is cached
        r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(dateRange("foo").field("date")
                .addRange(ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
                    ZonedDateTime.of(2013, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)))
                .get();
        assertSearchResponse(r);

        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getHitCount(), equalTo(0L));
        assertThat(client().admin().indices().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache()
                .getMissCount(), equalTo(1L));
    }

    /**
     * Test querying ranges on date mapping specifying a format with to/from
     * values specified as Strings
     */
    public void testRangeWithFormatStringValue() throws Exception {
        String indexName = "dateformat_test_idx";
        assertAcked(prepareCreate(indexName).addMapping("type", "date", "type=date,format=strict_hour_minute_second"));
        indexRandom(true,
                client().prepareIndex(indexName, "type", "1").setSource(jsonBuilder().startObject().field("date", "00:16:40").endObject()),
                client().prepareIndex(indexName, "type", "2").setSource(jsonBuilder().startObject().field("date", "00:33:20").endObject()),
                client().prepareIndex(indexName, "type", "3").setSource(jsonBuilder().startObject().field("date", "00:50:00").endObject()));

        // using no format should work when to/from is compatible with format in
        // mapping
        SearchResponse searchResponse = client().prepareSearch(indexName).setSize(0)
                .addAggregation(dateRange("date_range").field("date").addRange("00:16:40", "00:50:00").addRange("00:50:00", "01:06:40"))
                .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        List<Range.Bucket> buckets = checkBuckets(searchResponse.getAggregations().get("date_range"), "date_range", 2);
        assertBucket(buckets.get(0), 2L, "00:16:40-00:50:00", 1000000L, 3000000L);
        assertBucket(buckets.get(1), 1L, "00:50:00-01:06:40", 3000000L, 4000000L);

        // using different format should work when to/from is compatible with
        // format in aggregation
        searchResponse = client().prepareSearch(indexName).setSize(0).addAggregation(
                dateRange("date_range").field("date").addRange("00.16.40", "00.50.00").addRange("00.50.00", "01.06.40").format("HH.mm.ss"))
                .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        buckets = checkBuckets(searchResponse.getAggregations().get("date_range"), "date_range", 2);
        assertBucket(buckets.get(0), 2L, "00.16.40-00.50.00", 1000000L, 3000000L);
        assertBucket(buckets.get(1), 1L, "00.50.00-01.06.40", 3000000L, 4000000L);

        // providing numeric input with format should work, but bucket keys are
        // different now
        searchResponse = client().prepareSearch(indexName).setSize(0)
                .addAggregation(
                        dateRange("date_range").field("date").addRange(1000000, 3000000).addRange(3000000, 4000000).format("epoch_millis"))
                .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        buckets = checkBuckets(searchResponse.getAggregations().get("date_range"), "date_range", 2);
        assertBucket(buckets.get(0), 2L, "1000000-3000000", 1000000L, 3000000L);
        assertBucket(buckets.get(1), 1L, "3000000-4000000", 3000000L, 4000000L);

        // providing numeric input without format should throw an exception
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> client().prepareSearch(indexName).setSize(0)
                .addAggregation(dateRange("date_range").field("date").addRange(1000000, 3000000).addRange(3000000, 4000000)).get());
        assertThat(e.getDetailedMessage(),
            containsString("failed to parse date field [1000000] with format [strict_hour_minute_second]"));
    }

    /**
     * Test querying ranges on date mapping specifying a format with to/from
     * values specified as numeric value
     */
    public void testRangeWithFormatNumericValue() throws Exception {
        String indexName = "dateformat_numeric_test_idx";
        assertAcked(prepareCreate(indexName).addMapping("type", "date", "type=date,format=epoch_second"));
        indexRandom(true,
                client().prepareIndex(indexName, "type", "1").setSource(jsonBuilder().startObject().field("date", 1002).endObject()),
                client().prepareIndex(indexName, "type", "2").setSource(jsonBuilder().startObject().field("date", 2000).endObject()),
                client().prepareIndex(indexName, "type", "3").setSource(jsonBuilder().startObject().field("date", 3008).endObject()));

        // using no format should work when to/from is compatible with format in
        // mapping
        SearchResponse searchResponse = client().prepareSearch(indexName).setSize(0)
                .addAggregation(dateRange("date_range").field("date").addRange(1000, 3000).addRange(3000, 4000)).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        List<Bucket> buckets = checkBuckets(searchResponse.getAggregations().get("date_range"), "date_range", 2);
        assertBucket(buckets.get(0), 2L, "1000-3000", 1000000L, 3000000L);
        assertBucket(buckets.get(1), 1L, "3000-4000", 3000000L, 4000000L);

        // using no format should also work when and to/from are string values
        searchResponse = client().prepareSearch(indexName).setSize(0)
                .addAggregation(dateRange("date_range").field("date").addRange("1000", "3000").addRange("3000", "4000")).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        buckets = checkBuckets(searchResponse.getAggregations().get("date_range"), "date_range", 2);
        assertBucket(buckets.get(0), 2L, "1000-3000", 1000000L, 3000000L);
        assertBucket(buckets.get(1), 1L, "3000-4000", 3000000L, 4000000L);

        // also e-notation should work, fractional parts should be truncated
        searchResponse = client().prepareSearch(indexName).setSize(0)
                .addAggregation(dateRange("date_range").field("date").addRange(1.0e3, 3000.8123).addRange(3000.8123, 4.0e3)).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        buckets = checkBuckets(searchResponse.getAggregations().get("date_range"), "date_range", 2);
        assertBucket(buckets.get(0), 2L, "1000-3000", 1000000L, 3000000L);
        assertBucket(buckets.get(1), 1L, "3000-4000", 3000000L, 4000000L);

        // using different format should work when to/from is compatible with
        // format in aggregation
        searchResponse = client().prepareSearch(indexName).setSize(0).addAggregation(
                dateRange("date_range").field("date").addRange("00.16.40", "00.50.00").addRange("00.50.00", "01.06.40").format("HH.mm.ss"))
                .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        buckets = checkBuckets(searchResponse.getAggregations().get("date_range"), "date_range", 2);
        assertBucket(buckets.get(0), 2L, "00.16.40-00.50.00", 1000000L, 3000000L);
        assertBucket(buckets.get(1), 1L, "00.50.00-01.06.40", 3000000L, 4000000L);

        // providing different numeric input with format should work, but bucket
        // keys are different now
        searchResponse = client().prepareSearch(indexName).setSize(0)
                .addAggregation(
                        dateRange("date_range").field("date").addRange(1000000, 3000000).addRange(3000000, 4000000).format("epoch_millis"))
                .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        buckets = checkBuckets(searchResponse.getAggregations().get("date_range"), "date_range", 2);
        assertBucket(buckets.get(0), 2L, "1000000-3000000", 1000000L, 3000000L);
        assertBucket(buckets.get(1), 1L, "3000000-4000000", 3000000L, 4000000L);
    }

    private static List<Range.Bucket> checkBuckets(Range dateRange, String expectedAggName, long expectedBucketsSize) {
        assertThat(dateRange, Matchers.notNullValue());
        assertThat(dateRange.getName(), equalTo(expectedAggName));
        List<Range.Bucket> buckets = new ArrayList<>(dateRange.getBuckets());
        assertThat(buckets.size(), is(2));
        return buckets;
    }

    private static void assertBucket(Bucket bucket, long bucketSize, String expectedKey, long expectedFrom, long expectedTo) {
        assertThat(bucket.getDocCount(), equalTo(bucketSize));
        assertThat((String) bucket.getKey(), equalTo(expectedKey));
        assertThat(((ZonedDateTime) bucket.getFrom()).toInstant().toEpochMilli(), equalTo(expectedFrom));
        assertThat(((ZonedDateTime) bucket.getTo()).toInstant().toEpochMilli(), equalTo(expectedTo));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(true));
    }
}
