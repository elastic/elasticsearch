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
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.Range.Bucket;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.ipRange;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
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
public class IPv4RangeTests extends ElasticsearchIntegrationTest {

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        {
            assertAcked(prepareCreate("idx")
                    .addMapping("type", "ip", "type=ip", "ips", "type=ip"));
            IndexRequestBuilder[] builders = new IndexRequestBuilder[255]; // TODO randomize the size?
            // TODO randomize the values in the docs?
            for (int i = 0; i < builders.length; i++) {
                builders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                        .startObject()
                        .field("ip", "10.0.0." + (i))
                        .startArray("ips").value("10.0.0." + i).value("10.0.0." + (i + 1)).endArray()
                        .field("value", (i < 100 ? 1 : i < 200 ? 2 : 3))        // 100 1's, 100 2's, and 55 3's
                        .endObject());
            }
            indexRandom(true, builders);
            createIndex("idx_unmapped");
        }
        {
            assertAcked(prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer", "ip", "type=ip"));
            List<IndexRequestBuilder> builders = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + i).setSource(jsonBuilder()
                        .startObject()
                        .field("value", i * 2)
                        .field("ip", "10.0.0.5")
                        .endObject()));
            }
            indexRandom(true, builders.toArray(new IndexRequestBuilder[builders.size()]));
        }
        ensureSearchable();
    }

    @Test
    public void singleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) (String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) ((Number) bucket.getFrom())).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) ((Number) bucket.getTo())).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) (String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) ((Number) bucket.getFrom())).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) ((Number) bucket.getTo())).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) (String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
    }

    @Test
    public void singleValueField_WithMaskRange() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addMaskRange("10.0.0.0/25")
                        .addMaskRange("10.0.0.128/25"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(2));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.0/25"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.0")));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.0"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.128")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.128"));
        assertThat(bucket.getDocCount(), equalTo(128l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.128/25"));
        assertThat((long) ((Number) bucket.getFrom()).doubleValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.128")));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.128"));
        assertThat((long) ((Number) bucket.getTo()).doubleValue(), equalTo(IpFieldMapper.ipToLong("10.0.1.0"))); // range is exclusive on the to side
        assertThat(bucket.getToAsString(), equalTo("10.0.1.0"));
        assertThat(bucket.getDocCount(), equalTo(127l)); // include 10.0.0.128
    }

    @Test
    public void singleValueField_WithCustomKey() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("r1", "10.0.0.100")
                        .addRange("r2", "10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("r3", "10.0.0.200"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r1"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r2"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("r3"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200")
                        .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));
        Object[] propertiesKeys = (Object[]) range.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) range.getProperty("_count");
        Object[] propertiesCounts = (Object[]) range.getProperty("sum.value");

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 100));
        assertThat((String) propertiesKeys[0], equalTo("*-10.0.0.100"));
        assertThat((long) propertiesDocCounts[0], equalTo(100l));
        assertThat((double) propertiesCounts[0], equalTo((double) 100));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 200));
        assertThat((String) propertiesKeys[1], equalTo("10.0.0.100-10.0.0.200"));
        assertThat((long) propertiesDocCounts[1], equalTo(100l));
        assertThat((double) propertiesCounts[1], equalTo((double) 200));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 55*3));
        assertThat((String) propertiesKeys[2], equalTo("10.0.0.200-*"));
        assertThat((long) propertiesDocCounts[2], equalTo(55l));
        assertThat((double) propertiesCounts[2], equalTo((double) 55 * 3));
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200")
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.99")));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.199")));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.254")));
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .script("_value")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
    }

    /*
    [0, 1]
    [1, 2]
    [2, 3]
    ...
    [99, 100]
    [100, 101]
    [101, 102]
    ...
    [199, 200]
    [200, 201]
    [201, 202]
    ...
    [254, 255]
    [255, 256]
     */

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ips")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(101l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(56l));
    }

    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ips")
                        .script("_value")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(101l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(56l));
    }

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ips")
                        .script("_value")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200")
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.100")));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(101l));
        max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.200")));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(56l));
        max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.255")));
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .script("doc['ip'].value")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .script("doc['ip'].value")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200")
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.99")));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.199")));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.254")));
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .script("doc['ips'].values")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(101l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(56l));
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .script("doc['ips'].values")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200")
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.100")));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(101l));
        max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.200")));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(56l));
        max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.255")));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(0l));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertSearchResponse(response);


        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(range.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
    }

    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0)
                        .subAggregation(ipRange("ip_range").field("ip").addRange("r1", "10.0.0.1", "10.0.0.10")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Range range = bucket.getAggregations().get("ip_range");
        // TODO: use diamond once JI-9019884 is fixed
        List<Range.Bucket> buckets = new ArrayList<Range.Bucket>(range.getBuckets());
        assertThat(range, Matchers.notNullValue());
        assertThat(range.getName(), equalTo("ip_range"));
        assertThat(buckets.size(), is(1));
        assertThat((String) buckets.get(0).getKey(), equalTo("r1"));
        assertThat(buckets.get(0).getFromAsString(), equalTo("10.0.0.1"));
        assertThat(buckets.get(0).getToAsString(), equalTo("10.0.0.10"));
        assertThat(buckets.get(0).getDocCount(), equalTo(0l));
    }
}
