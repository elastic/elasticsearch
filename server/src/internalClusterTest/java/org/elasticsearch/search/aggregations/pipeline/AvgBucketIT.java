/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.avgBucket;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class AvgBucketIT extends ESIntegTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";

    static int numDocs;
    static int interval;
    static int minRandomValue;
    static int maxRandomValue;
    static int numValueBuckets;
    static long[] valueCounts;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("idx").addMapping("type", "tag", "type=keyword").get());
        createIndex("idx_unmapped");

        numDocs = randomIntBetween(6, 20);
        interval = randomIntBetween(2, 5);

        minRandomValue = 0;
        maxRandomValue = 20;

        numValueBuckets = ((maxRandomValue - minRandomValue) / interval) + 1;
        valueCounts = new long[numValueBuckets];

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            int fieldValue = randomIntBetween(minRandomValue, maxRandomValue);
            builders.add(
                client().prepareIndex("idx", "type")
                    .setSource(
                        jsonBuilder().startObject()
                            .field(SINGLE_VALUED_FIELD_NAME, fieldValue)
                            .field("tag", "tag" + (i % interval))
                            .endObject()
                    )
            );
            final int bucket = (fieldValue / interval); // + (fieldValue < 0 ? -1 : 0) - (minRandomValue / interval - 1);
            valueCounts[bucket]++;
        }

        assertAcked(prepareCreate("empty_bucket_idx").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=integer"));
        for (int i = 0; i < 2; i++) {
            builders.add(
                client().prepareIndex("empty_bucket_idx", "type", "" + i)
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i * 2).endObject())
            );
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testDocCountTopLevel() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).extendedBounds(minRandomValue, maxRandomValue)
            )
            .addAggregation(avgBucket("avg_bucket", "histo>_count"))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        double sum = 0;
        int count = 0;
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
            count++;
            sum += bucket.getDocCount();
        }

        double avgValue = count == 0 ? Double.NaN : (sum / count);
        InternalSimpleValue avgBucketValue = response.getAggregations().get("avg_bucket");
        assertThat(avgBucketValue, notNullValue());
        assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
        assertThat(avgBucketValue.value(), equalTo(avgValue));
    }

    public void testDocCountAsSubAgg() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).extendedBounds(minRandomValue, maxRandomValue)
                    )
                    .subAggregation(avgBucket("avg_bucket", "histo>_count"))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Bucket> buckets = histo.getBuckets();

            double sum = 0;
            int count = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                count++;
                sum += bucket.getDocCount();
            }

            double avgValue = count == 0 ? Double.NaN : (sum / count);
            InternalSimpleValue avgBucketValue = termsBucket.getAggregations().get("avg_bucket");
            assertThat(avgBucketValue, notNullValue());
            assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
            assertThat(avgBucketValue.value(), equalTo(avgValue));
        }
    }

    public void testMetricTopLevel() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(terms("terms").field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
            .addAggregation(avgBucket("avg_bucket", "terms>sum"))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(interval));

        double bucketSum = 0;
        int count = 0;
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((String) bucket.getKey(), equalTo("tag" + (i % interval)));
            assertThat(bucket.getDocCount(), greaterThan(0L));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            count++;
            bucketSum += sum.value();
        }

        double avgValue = count == 0 ? Double.NaN : (bucketSum / count);
        InternalSimpleValue avgBucketValue = response.getAggregations().get("avg_bucket");
        assertThat(avgBucketValue, notNullValue());
        assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
        assertThat(avgBucketValue.value(), equalTo(avgValue));
    }

    public void testMetricAsSubAgg() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                            .interval(interval)
                            .extendedBounds(minRandomValue, maxRandomValue)
                            .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
                    )
                    .subAggregation(avgBucket("avg_bucket", "histo>sum"))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Bucket> buckets = histo.getBuckets();

            double bucketSum = 0;
            int count = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() != 0) {
                    Sum sum = bucket.getAggregations().get("sum");
                    assertThat(sum, notNullValue());
                    count++;
                    bucketSum += sum.value();
                }
            }

            double avgValue = count == 0 ? Double.NaN : (bucketSum / count);
            InternalSimpleValue avgBucketValue = termsBucket.getAggregations().get("avg_bucket");
            assertThat(avgBucketValue, notNullValue());
            assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
            assertThat(avgBucketValue.value(), equalTo(avgValue));
        }
    }

    public void testMetricAsSubAggWithInsertZeros() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                            .interval(interval)
                            .extendedBounds(minRandomValue, maxRandomValue)
                            .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
                    )
                    .subAggregation(avgBucket("avg_bucket", "histo>sum").gapPolicy(GapPolicy.INSERT_ZEROS))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Bucket> buckets = histo.getBuckets();

            double bucketSum = 0;
            int count = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());

                count++;
                bucketSum += sum.value();
            }

            double avgValue = count == 0 ? Double.NaN : (bucketSum / count);
            InternalSimpleValue avgBucketValue = termsBucket.getAggregations().get("avg_bucket");
            assertThat(avgBucketValue, notNullValue());
            assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
            assertThat(avgBucketValue.value(), equalTo(avgValue));
        }
    }

    public void testNoBuckets() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .includeExclude(new IncludeExclude(null, "tag.*"))
                    .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
            )
            .addAggregation(avgBucket("avg_bucket", "terms>sum"))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(0));

        InternalSimpleValue avgBucketValue = response.getAggregations().get("avg_bucket");
        assertThat(avgBucketValue, notNullValue());
        assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
        assertThat(avgBucketValue.value(), equalTo(Double.NaN));
    }

    public void testNested() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).extendedBounds(minRandomValue, maxRandomValue)
                    )
                    .subAggregation(avgBucket("avg_histo_bucket", "histo>_count"))
            )
            .addAggregation(avgBucket("avg_terms_bucket", "terms>avg_histo_bucket"))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        double aggTermsSum = 0;
        int aggTermsCount = 0;
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Bucket> buckets = histo.getBuckets();

            double aggHistoSum = 0;
            int aggHistoCount = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));

                aggHistoCount++;
                aggHistoSum += bucket.getDocCount();
            }

            double avgHistoValue = aggHistoCount == 0 ? Double.NaN : (aggHistoSum / aggHistoCount);
            InternalSimpleValue avgBucketValue = termsBucket.getAggregations().get("avg_histo_bucket");
            assertThat(avgBucketValue, notNullValue());
            assertThat(avgBucketValue.getName(), equalTo("avg_histo_bucket"));
            assertThat(avgBucketValue.value(), equalTo(avgHistoValue));

            aggTermsCount++;
            aggTermsSum += avgHistoValue;
        }

        double avgTermsValue = aggTermsCount == 0 ? Double.NaN : (aggTermsSum / aggTermsCount);
        InternalSimpleValue avgBucketValue = response.getAggregations().get("avg_terms_bucket");
        assertThat(avgBucketValue, notNullValue());
        assertThat(avgBucketValue.getName(), equalTo("avg_terms_bucket"));
        assertThat(avgBucketValue.value(), equalTo(avgTermsValue));
    }
}
