/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.pipeline.MovingFunctions;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.aggs.correlation.BucketCorrelationAggregationBuilder;
import org.elasticsearch.xpack.ml.aggs.correlation.CountCorrelationFunction;
import org.elasticsearch.xpack.ml.aggs.correlation.CountCorrelationIndicator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.closeTo;

public class BucketCorrelationAggregationIT extends MlSingleNodeTestCase {

    public void testCountCorrelation() {

        double[] xs = new double[10000];
        int[] isCat = new int[10000];
        int[] isDog = new int[10000];

        client().admin().indices().prepareCreate("data").setMapping("metric", "type=double", "term", "type=keyword").get();
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk("data");
        for (int i = 0; i < 5000; i++) {
            IndexRequest indexRequest = new IndexRequest("data");
            double x = randomDoubleBetween(100.0, 1000.0, true);
            xs[i] = x;
            isCat[i] = 1;
            isDog[i] = 0;
            indexRequest.source("metric", x, "term", "cat").opType(DocWriteRequest.OpType.CREATE);
            bulkRequestBuilder.add(indexRequest);
        }
        sendAndMaybeFail(bulkRequestBuilder);
        bulkRequestBuilder = client().prepareBulk("data");

        for (int i = 5000; i < 10000; i++) {
            IndexRequest indexRequest = new IndexRequest("data");
            double x = randomDoubleBetween(0.0, 100.0, true);
            xs[i] = x;
            isCat[i] = 0;
            isDog[i] = 1;
            indexRequest.source("metric", x, "term", "dog").opType(DocWriteRequest.OpType.CREATE);
            bulkRequestBuilder.add(indexRequest);
        }
        sendAndMaybeFail(bulkRequestBuilder);

        double catCorrelation = pearsonCorrelation(xs, isCat);
        double dogCorrelation = pearsonCorrelation(xs, isDog);

        AtomicLong counter = new AtomicLong();
        double[] steps = Stream.generate(() -> counter.getAndAdd(2L)).limit(50).mapToDouble(l -> (double) l).toArray();
        assertResponse(
            client().prepareSearch("data")
                .addAggregation(AggregationBuilders.percentiles("percentiles").field("metric").percentiles(steps))
                .setSize(0)
                .setTrackTotalHits(true),
            percentilesSearch -> {
                long totalHits = percentilesSearch.getHits().getTotalHits().value();
                Percentiles percentiles = percentilesSearch.getAggregations().get("percentiles");
                Tuple<RangeAggregationBuilder, BucketCorrelationAggregationBuilder> aggs = buildRangeAggAndSetExpectations(
                    percentiles,
                    steps,
                    totalHits,
                    "metric"
                );

                assertResponse(
                    client().prepareSearch("data")
                        .setSize(0)
                        .setTrackTotalHits(false)
                        .addAggregation(
                            AggregationBuilders.terms("buckets").field("term").subAggregation(aggs.v1()).subAggregation(aggs.v2())
                        ),
                    countCorrelations -> {

                        Terms terms = countCorrelations.getAggregations().get("buckets");
                        Terms.Bucket catBucket = terms.getBucketByKey("cat");
                        Terms.Bucket dogBucket = terms.getBucketByKey("dog");
                        NumericMetricsAggregation.SingleValue approxCatCorrelation = catBucket.getAggregations().get("correlates");
                        NumericMetricsAggregation.SingleValue approxDogCorrelation = dogBucket.getAggregations().get("correlates");

                        assertThat(approxCatCorrelation.value(), closeTo(catCorrelation, 0.1));
                        assertThat(approxDogCorrelation.value(), closeTo(dogCorrelation, 0.1));
                    }
                );
            }
        );
    }

    private static Tuple<RangeAggregationBuilder, BucketCorrelationAggregationBuilder> buildRangeAggAndSetExpectations(
        Percentiles raw_percentiles,
        double[] steps,
        long totalCount,
        String indicatorFieldName
    ) {
        List<Double> percentiles = new ArrayList<>();
        List<Double> fractions = new ArrayList<>();
        RangeAggregationBuilder builder = AggregationBuilders.range("correlation_range").field(indicatorFieldName);
        double percentile_0 = raw_percentiles.percentile(steps[0]);
        builder.addUnboundedTo(percentile_0);
        fractions.add(0.02);
        percentiles.add(percentile_0);
        int last_added = 0;
        for (int i = 1; i < steps.length; i++) {
            double percentile_l = raw_percentiles.percentile(steps[i - 1]);
            double percentile_r = raw_percentiles.percentile(steps[i]);
            if (Double.compare(percentile_l, percentile_r) == 0) {
                fractions.set(last_added, fractions.get(last_added) + 0.02);
            } else {
                last_added = i;
                fractions.add(0.02);
                percentiles.add(percentile_r);
            }
        }
        fractions.add(2.0 / 100);
        double[] expectations = new double[percentiles.size() + 1];
        expectations[0] = percentile_0;
        for (int i = 1; i < percentiles.size(); i++) {
            double percentile_l = percentiles.get(i - 1);
            double percentile_r = percentiles.get(i);
            double fractions_l = fractions.get(i - 1);
            double fractions_r = fractions.get(i);
            builder.addRange(percentile_l, percentile_r);
            expectations[i] = (fractions_l * percentile_l + fractions_r * percentile_r) / (fractions_l + fractions_r);
        }
        double percentile_n = percentiles.get(percentiles.size() - 1);
        builder.addUnboundedFrom(percentile_n);
        expectations[percentiles.size()] = percentile_n;
        return Tuple.tuple(
            builder,
            new BucketCorrelationAggregationBuilder(
                "correlates",
                "correlation_range>_count",
                new CountCorrelationFunction(
                    new CountCorrelationIndicator(expectations, fractions.stream().mapToDouble(Double::doubleValue).toArray(), totalCount)
                )
            )
        );
    }

    private double pearsonCorrelation(double[] xs, int[] ys) {
        double meanX = MovingFunctions.unweightedAvg(xs);
        double meanY = sum(ys) / (double) ys.length;
        double varX = Math.pow(MovingFunctions.stdDev(xs, meanX), 2.0);
        double varY = 0.0;
        for (int y : ys) {
            varY += Math.pow(y - meanY, 2);
        }
        varY /= ys.length;

        if (varY == 0 || varX == 0 || Double.isNaN(varX) || Double.isNaN(varY)) {
            fail("failed to calculate true correlation due to 0 variance in the data");
        }

        double corXY = 0.0;
        for (int i = 0; i < xs.length; i++) {
            corXY += (((xs[i] - meanX) * (ys[i] - meanY)) / Math.sqrt(varX * varY));
        }
        return corXY / xs.length;
    }

    private static int sum(int[] xs) {
        int s = 0;
        for (int x : xs) {
            s += x;
        }
        return s;
    }

    private void sendAndMaybeFail(BulkRequestBuilder bulkRequestBuilder) {
        BulkResponse bulkResponse = bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        if (bulkResponse.hasFailures()) {
            int failures = 0;
            for (BulkItemResponse itemResponse : bulkResponse) {
                if (itemResponse.isFailed()) {
                    failures++;
                }
            }
            logger.error("Item response failure [{}]", bulkResponse.buildFailureMessage());
            fail("Bulk response contained " + failures + " failures");
        }
    }

}
