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
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntToDoubleFunction;

import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
abstract class BucketMetricsPipeLineAggregationTestCase<T extends NumericMetricsAggregation> extends ESIntegTestCase {

    static final String SINGLE_VALUED_FIELD_NAME = "l_value";

    static int numDocs;
    static int interval;
    static int minRandomValue;
    static int maxRandomValue;
    static int numValueBuckets;
    static long[] valueCounts;

    static String histoName;
    static String termsName;

    /** Creates the pipeline aggregation to test */
    protected abstract BucketMetricsPipelineAggregationBuilder<?> BucketMetricsPipelineAgg(String name, String bucketsPath);

    /** Checks that the provided bucket values and keys agree with the result of the pipeline aggregation */
    protected abstract void assertResult(
        IntToDoubleFunction bucketValues,
        Function<Integer, String> bucketKeys,
        int numValues,
        T pipelineBucket
    );

    /** Nested metric from the pipeline aggregation to test. This metric is added to the end of the bucket path*/
    protected abstract String nestedMetric();

    /** Extract the value of the nested metric provided in {@link #nestedMetric()} */
    protected abstract double getNestedMetric(T bucket);

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("idx").setMapping("tag", "type=keyword").get());
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
                client().prepareIndex("idx")
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

        assertAcked(prepareCreate("empty_bucket_idx").setMapping(SINGLE_VALUED_FIELD_NAME, "type=integer"));
        for (int i = 0; i < 2; i++) {
            builders.add(
                client().prepareIndex("empty_bucket_idx")
                    .setId("" + i)
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i * 2).endObject())
            );
        }
        indexRandom(true, builders);
        ensureSearchable();
        histoName = randomName();
        termsName = randomName();
    }

    private String randomName() {
        return randomBoolean()
            ? randomAlphaOfLengthBetween(3, 12)
            : randomAlphaOfLengthBetween(3, 6) + "." + randomAlphaOfLengthBetween(3, 6);
    }

    public void testDocCountTopLevel() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram(histoName).field(SINGLE_VALUED_FIELD_NAME).interval(interval).extendedBounds(minRandomValue, maxRandomValue)
            )
            .addAggregation(BucketMetricsPipelineAgg("pipeline_agg", histoName + ">_count"))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get(histoName);
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo(histoName));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
        }

        T pipelineBucket = response.getAggregations().get("pipeline_agg");
        assertThat(pipelineBucket, notNullValue());
        assertThat(pipelineBucket.getName(), equalTo("pipeline_agg"));

        assertResult((i) -> buckets.get(i).getDocCount(), (i) -> buckets.get(i).getKeyAsString(), numValueBuckets, pipelineBucket);
    }

    public void testDocCountAsSubAgg() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms(termsName).field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram(histoName).field(SINGLE_VALUED_FIELD_NAME)
                            .interval(interval)
                            .extendedBounds(minRandomValue, maxRandomValue)
                    )
                    .subAggregation(BucketMetricsPipelineAgg("pipeline_agg", histoName + ">_count"))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get(termsName);
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo(termsName));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get(histoName);
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo(histoName));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
            }

            T pipelineBucket = termsBucket.getAggregations().get("pipeline_agg");
            assertThat(pipelineBucket, notNullValue());
            assertThat(pipelineBucket.getName(), equalTo("pipeline_agg"));

            assertResult((k) -> buckets.get(k).getDocCount(), (k) -> buckets.get(k).getKeyAsString(), numValueBuckets, pipelineBucket);
        }
    }

    public void testMetricTopLevel() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(terms(termsName).field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
            .addAggregation(BucketMetricsPipelineAgg("pipeline_agg", termsName + ">sum"))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get(termsName);
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo(termsName));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((String) bucket.getKey(), equalTo("tag" + (i % interval)));
            assertThat(bucket.getDocCount(), greaterThan(0L));
        }

        T pipelineBucket = response.getAggregations().get("pipeline_agg");
        assertThat(pipelineBucket, notNullValue());
        assertThat(pipelineBucket.getName(), equalTo("pipeline_agg"));

        IntToDoubleFunction function = (i) -> {
            Sum sum = buckets.get(i).getAggregations().get("sum");
            assertThat(sum, notNullValue());
            return sum.value();
        };
        assertResult(function, (i) -> buckets.get(i).getKeyAsString(), interval, pipelineBucket);
    }

    public void testMetricAsSubAgg() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms(termsName).field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram(histoName).field(SINGLE_VALUED_FIELD_NAME)
                            .interval(interval)
                            .extendedBounds(minRandomValue, maxRandomValue)
                            .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
                    )
                    .subAggregation(BucketMetricsPipelineAgg("pipeline_agg", histoName + ">sum"))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get(termsName);
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo(termsName));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get(histoName);
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo(histoName));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            List<Histogram.Bucket> notNullBuckets = new ArrayList<>();
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() != 0) {
                    notNullBuckets.add(bucket);
                }
            }

            T pipelineBucket = termsBucket.getAggregations().get("pipeline_agg");
            assertThat(pipelineBucket, notNullValue());
            assertThat(pipelineBucket.getName(), equalTo("pipeline_agg"));

            IntToDoubleFunction function = (k) -> {
                Sum sum = notNullBuckets.get(k).getAggregations().get("sum");
                assertThat(sum, notNullValue());
                return sum.value();
            };
            assertResult(function, (k) -> notNullBuckets.get(k).getKeyAsString(), notNullBuckets.size(), pipelineBucket);
        }
    }

    public void testMetricAsSubAggWithInsertZeros() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms(termsName).field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram(histoName).field(SINGLE_VALUED_FIELD_NAME)
                            .interval(interval)
                            .extendedBounds(minRandomValue, maxRandomValue)
                            .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
                    )
                    .subAggregation(
                        BucketMetricsPipelineAgg("pipeline_agg", histoName + ">sum").gapPolicy(BucketHelpers.GapPolicy.INSERT_ZEROS)
                    )
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get(termsName);
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo(termsName));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get(histoName);
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo(histoName));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
            }

            T pipelineBucket = termsBucket.getAggregations().get("pipeline_agg");
            assertThat(pipelineBucket, notNullValue());
            assertThat(pipelineBucket.getName(), equalTo("pipeline_agg"));

            IntToDoubleFunction function = (k) -> {
                Sum sum = buckets.get(k).getAggregations().get("sum");
                assertThat(sum, notNullValue());
                return sum.value();
            };
            assertResult(function, (k) -> buckets.get(k).getKeyAsString(), numValueBuckets, pipelineBucket);
        }
    }

    public void testNoBuckets() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms(termsName).field("tag")
                    .includeExclude(new IncludeExclude(null, "tag.*", null, null))
                    .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
            )
            .addAggregation(BucketMetricsPipelineAgg("pipeline_agg", termsName + ">sum"))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get(termsName);
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo(termsName));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(0));

        T pipelineBucket = response.getAggregations().get("pipeline_agg");
        assertThat(pipelineBucket, notNullValue());
        assertThat(pipelineBucket.getName(), equalTo("pipeline_agg"));

        assertResult((k) -> 0.0, (k) -> "", 0, pipelineBucket);
    }

    public void testNested() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms(termsName).field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram(histoName).field(SINGLE_VALUED_FIELD_NAME)
                            .interval(interval)
                            .extendedBounds(minRandomValue, maxRandomValue)
                    )
                    .subAggregation(BucketMetricsPipelineAgg("nested_histo_bucket", histoName + ">_count"))
            )
            .addAggregation(BucketMetricsPipelineAgg("nested_terms_bucket", termsName + ">nested_histo_bucket." + nestedMetric()))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get(termsName);
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo(termsName));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        List<T> allBuckets = new ArrayList<>();
        List<String> nestedTags = new ArrayList<>();
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get(histoName);
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo(histoName));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
            }

            T pipelineBucket = termsBucket.getAggregations().get("nested_histo_bucket");
            assertThat(pipelineBucket, notNullValue());
            assertThat(pipelineBucket.getName(), equalTo("nested_histo_bucket"));

            assertResult((k) -> buckets.get(k).getDocCount(), (k) -> buckets.get(k).getKeyAsString(), numValueBuckets, pipelineBucket);
            allBuckets.add(pipelineBucket);
            nestedTags.add(termsBucket.getKeyAsString());
        }

        T pipelineBucket = response.getAggregations().get("nested_terms_bucket");
        assertThat(pipelineBucket, notNullValue());
        assertThat(pipelineBucket.getName(), equalTo("nested_terms_bucket"));

        assertResult((k) -> getNestedMetric(allBuckets.get(k)), (k) -> nestedTags.get(k), allBuckets.size(), pipelineBucket);
    }

    /**
     * https://github.com/elastic/elasticsearch/issues/33514
     *
     * This bug manifests as the max_bucket agg ("peak") being added to the response twice, because
     * the pipeline agg is run twice.  This makes invalid JSON and breaks conversion to maps.
     * The bug was caused by an UnmappedTerms being the chosen as the first reduction target.  UnmappedTerms
     * delegated reduction to the first non-unmapped agg, which would reduce and run pipeline aggs.  But then
     * execution returns to the UnmappedTerms and _it_ runs pipelines as well, doubling up on the values.
     */
    public void testFieldIsntWrittenOutTwice() throws Exception {
        // you need to add an additional index with no fields in order to trigger this (or potentially a shard)
        // so that there is an UnmappedTerms in the list to reduce.
        createIndex("foo_1");
        // tag::noformat
        XContentBuilder builder = jsonBuilder().startObject()
            .startObject("properties")
              .startObject("@timestamp")
                .field("type", "date")
              .endObject()
              .startObject("license")
                .startObject("properties")
                  .startObject("count")
                    .field("type", "long")
                  .endObject()
                  .startObject("partnumber")
                    .field("type", "text")
                    .startObject("fields")
                      .startObject("keyword")
                        .field("type", "keyword")
                        .field("ignore_above", 256)
                      .endObject()
                    .endObject()
                  .endObject()
                .endObject()
              .endObject()
            .endObject()
          .endObject();
        // end::noformat
        assertAcked(client().admin().indices().prepareCreate("foo_2").setMapping(builder).get());
        // tag::noformat
        XContentBuilder docBuilder = jsonBuilder().startObject()
            .startObject("license")
              .field("partnumber", "foobar")
              .field("count", 2)
            .endObject()
            .field("@timestamp", "2018-07-08T08:07:00.599Z")
          .endObject();
        // end::noformat
        client().prepareIndex("foo_2").setSource(docBuilder).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        client().admin().indices().prepareRefresh();

        TermsAggregationBuilder groupByLicenseAgg = terms("group_by_license_partnumber").field("license.partnumber.keyword");

        SumAggregationBuilder sumAggBuilder = sum("total_licenses").field("license.count");
        DateHistogramAggregationBuilder licensePerDayBuilder = dateHistogram("licenses_per_day").field("@timestamp")
            .fixedInterval(DateHistogramInterval.DAY);
        licensePerDayBuilder.subAggregation(sumAggBuilder);
        groupByLicenseAgg.subAggregation(licensePerDayBuilder);
        groupByLicenseAgg.subAggregation(BucketMetricsPipelineAgg("peak", "licenses_per_day>total_licenses"));

        SearchResponse response = client().prepareSearch("foo_*").setSize(0).addAggregation(groupByLicenseAgg).get();
        BytesReference bytes = XContentHelper.toXContent(response, XContentType.JSON, false);
        XContentHelper.convertToMap(bytes, false, XContentType.JSON);
    }
}
