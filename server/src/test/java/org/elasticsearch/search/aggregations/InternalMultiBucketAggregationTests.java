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

package org.elasticsearch.search.aggregations;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.search.aggregations.InternalMultiBucketAggregation.resolvePropertyFromPath;
import static org.hamcrest.Matchers.equalTo;

public class InternalMultiBucketAggregationTests extends ESTestCase {

    public void testResolveToAgg() {
        AggregationPath path = AggregationPath.parse("the_avg");
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        InternalAggregation agg = new InternalAvg("the_avg", 2, 1, DocValueFormat.RAW, Collections.emptyMap());
        InternalAggregations internalAggregations = new InternalAggregations(Collections.singletonList(agg));

        LongTerms.Bucket bucket = new LongTerms.Bucket(1, 1, internalAggregations, false, 0, DocValueFormat.RAW);
        buckets.add(bucket);

        Object[] value = (Object[]) resolvePropertyFromPath(path.getPathElementsAsStringList(), buckets, "the_long_terms");
        assertThat(value[0], equalTo(agg));
    }

    public void testResolveToAggValue() {
        AggregationPath path = AggregationPath.parse("the_avg.value");
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        InternalAggregation agg = new InternalAvg("the_avg", 2, 1, DocValueFormat.RAW, Collections.emptyMap());
        InternalAggregations internalAggregations = new InternalAggregations(Collections.singletonList(agg));

        LongTerms.Bucket bucket = new LongTerms.Bucket(1, 1, internalAggregations, false, 0, DocValueFormat.RAW);
        buckets.add(bucket);

        Object[] value = (Object[]) resolvePropertyFromPath(path.getPathElementsAsStringList(), buckets, "the_long_terms");
        assertThat(value[0], equalTo(2.0));
    }

    public void testResolveToNothing() {
        AggregationPath path = AggregationPath.parse("foo.value");
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        InternalAggregation agg = new InternalAvg("the_avg", 2, 1, DocValueFormat.RAW, Collections.emptyMap());
        InternalAggregations internalAggregations = new InternalAggregations(Collections.singletonList(agg));

        LongTerms.Bucket bucket = new LongTerms.Bucket(1, 1, internalAggregations, false, 0, DocValueFormat.RAW);
        buckets.add(bucket);

        InvalidAggregationPathException e = expectThrows(InvalidAggregationPathException.class,
            () -> resolvePropertyFromPath(path.getPathElementsAsStringList(), buckets, "the_long_terms"));
        assertThat(e.getMessage(), equalTo("Cannot find an aggregation named [foo] in [the_long_terms]"));
    }

    public void testResolveToUnknown() {
        AggregationPath path = AggregationPath.parse("the_avg.unknown");
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        InternalAggregation agg = new InternalAvg("the_avg", 2, 1, DocValueFormat.RAW, Collections.emptyMap());
        InternalAggregations internalAggregations = new InternalAggregations(Collections.singletonList(agg));

        LongTerms.Bucket bucket = new LongTerms.Bucket(1, 1, internalAggregations, false, 0, DocValueFormat.RAW);
        buckets.add(bucket);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> resolvePropertyFromPath(path.getPathElementsAsStringList(), buckets, "the_long_terms"));
        assertThat(e.getMessage(), equalTo("path not supported for [the_avg]: [unknown]"));
    }

    public void testResolveToBucketCount() {
        AggregationPath path = AggregationPath.parse("_bucket_count");
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        InternalAggregation agg = new InternalAvg("the_avg", 2, 1, DocValueFormat.RAW, Collections.emptyMap());
        InternalAggregations internalAggregations = new InternalAggregations(Collections.singletonList(agg));

        LongTerms.Bucket bucket = new LongTerms.Bucket(1, 1, internalAggregations, false, 0, DocValueFormat.RAW);
        buckets.add(bucket);

        Object value = resolvePropertyFromPath(path.getPathElementsAsStringList(), buckets, "the_long_terms");
        assertThat(value, equalTo(1));
    }

    public void testResolveToCount() {
        AggregationPath path = AggregationPath.parse("_count");
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        InternalAggregation agg = new InternalAvg("the_avg", 2, 1, DocValueFormat.RAW, Collections.emptyMap());
        InternalAggregations internalAggregations = new InternalAggregations(Collections.singletonList(agg));

        LongTerms.Bucket bucket = new LongTerms.Bucket(1, 1, internalAggregations, false, 0, DocValueFormat.RAW);
        buckets.add(bucket);

        Object[] value = (Object[]) resolvePropertyFromPath(path.getPathElementsAsStringList(), buckets, "the_long_terms");
        assertThat(value[0], equalTo(1L));
    }

    public void testResolveToKey() {
        AggregationPath path = AggregationPath.parse("_key");
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        InternalAggregation agg = new InternalAvg("the_avg", 2, 1, DocValueFormat.RAW, Collections.emptyMap());
        InternalAggregations internalAggregations = new InternalAggregations(Collections.singletonList(agg));

        LongTerms.Bucket bucket = new LongTerms.Bucket(19, 1, internalAggregations, false, 0, DocValueFormat.RAW);
        buckets.add(bucket);

        Object[] value = (Object[]) resolvePropertyFromPath(path.getPathElementsAsStringList(), buckets, "the_long_terms");
        assertThat(value[0], equalTo(19L));
    }

    public void testResolveToSpecificBucket() {
        AggregationPath path = AggregationPath.parse("string_terms['foo']>the_avg.value");

        List<LongTerms.Bucket> buckets = new ArrayList<>();
        InternalAggregation agg = new InternalAvg("the_avg", 2, 1, DocValueFormat.RAW, Collections.emptyMap());
        InternalAggregations internalStringAggs = new InternalAggregations(Collections.singletonList(agg));
        List<StringTerms.Bucket> stringBuckets = Collections.singletonList(new StringTerms.Bucket(
            new BytesRef("foo".getBytes(StandardCharsets.UTF_8), 0, "foo".getBytes(StandardCharsets.UTF_8).length), 1,
            internalStringAggs, false, 0, DocValueFormat.RAW));

        InternalTerms termsAgg = new StringTerms("string_terms", BucketOrder.count(false), 1, 0,
            Collections.emptyMap(), DocValueFormat.RAW, 1, false, 0, stringBuckets, 0);
        InternalAggregations internalAggregations = new InternalAggregations(Collections.singletonList(termsAgg));
        LongTerms.Bucket bucket = new LongTerms.Bucket(19, 1, internalAggregations, false, 0, DocValueFormat.RAW);
        buckets.add(bucket);

        Object[] value = (Object[]) resolvePropertyFromPath(path.getPathElementsAsStringList(), buckets, "the_long_terms");
        assertThat(value[0], equalTo(2.0));
    }

    public void testResolveToMissingSpecificBucket() {
        AggregationPath path = AggregationPath.parse("string_terms['bar']>the_avg.value");

        List<LongTerms.Bucket> buckets = new ArrayList<>();
        InternalAggregation agg = new InternalAvg("the_avg", 2, 1, DocValueFormat.RAW, Collections.emptyMap());
        InternalAggregations internalStringAggs = new InternalAggregations(Collections.singletonList(agg));
        List<StringTerms.Bucket> stringBuckets = Collections.singletonList(new StringTerms.Bucket(
            new BytesRef("foo".getBytes(StandardCharsets.UTF_8), 0, "foo".getBytes(StandardCharsets.UTF_8).length), 1,
            internalStringAggs, false, 0, DocValueFormat.RAW));

        InternalTerms termsAgg = new StringTerms("string_terms", BucketOrder.count(false), 1, 0,
            Collections.emptyMap(), DocValueFormat.RAW, 1, false, 0, stringBuckets, 0);
        InternalAggregations internalAggregations = new InternalAggregations(Collections.singletonList(termsAgg));
        LongTerms.Bucket bucket = new LongTerms.Bucket(19, 1, internalAggregations, false, 0, DocValueFormat.RAW);
        buckets.add(bucket);

        InvalidAggregationPathException e = expectThrows(InvalidAggregationPathException.class,
            () -> resolvePropertyFromPath(path.getPathElementsAsStringList(), buckets, "the_long_terms"));
        assertThat(e.getMessage(), equalTo("Cannot find an key ['bar'] in [string_terms]"));
    }
}
