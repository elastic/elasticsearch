/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence.overallbuckets;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.OverallBucket;
import org.elasticsearch.xpack.core.ml.job.results.Result;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class OverallBucketsProvider {

    private final long maxJobBucketSpanSeconds;
    private final int topN;
    private final double minOverallScore;

    public OverallBucketsProvider(TimeValue maxJobBucketSpan, int topN, double minOverallScore) {
        this.maxJobBucketSpanSeconds = maxJobBucketSpan.seconds();
        this.topN = topN;
        this.minOverallScore = minOverallScore;
    }

    public List<OverallBucket> computeOverallBuckets(Histogram histogram) {
        List<OverallBucket> overallBuckets = new ArrayList<>();
        for (Histogram.Bucket histogramBucket : histogram.getBuckets()) {
            Aggregations histogramBucketAggs = histogramBucket.getAggregations();
            Terms jobsAgg = histogramBucketAggs.get(Job.ID.getPreferredName());
            int jobsCount = jobsAgg.getBuckets().size();
            int bucketTopN = Math.min(topN, jobsCount);
            Set<OverallBucket.JobInfo> jobs = new TreeSet<>();
            TopNScores topNScores = new TopNScores(bucketTopN);
            for (Terms.Bucket jobsBucket : jobsAgg.getBuckets()) {
                Max maxScore = jobsBucket.getAggregations().get(OverallBucket.OVERALL_SCORE.getPreferredName());
                topNScores.insertWithOverflow(maxScore.getValue());
                jobs.add(new OverallBucket.JobInfo((String) jobsBucket.getKey(), maxScore.getValue()));
            }

            double overallScore = topNScores.overallScore();
            if (overallScore < minOverallScore) {
                continue;
            }

            Max interimAgg = histogramBucketAggs.get(Result.IS_INTERIM.getPreferredName());
            boolean isInterim = interimAgg.getValue() > 0;

            overallBuckets.add(new OverallBucket(getHistogramBucketTimestamp(histogramBucket),
                    maxJobBucketSpanSeconds, overallScore, new ArrayList<>(jobs), isInterim));
        }
        return overallBuckets;
    }

    private static Date getHistogramBucketTimestamp(Histogram.Bucket bucket) {
        ZonedDateTime bucketTimestamp = (ZonedDateTime) bucket.getKey();
        return new Date(bucketTimestamp.toInstant().toEpochMilli());
    }

    static class TopNScores extends PriorityQueue<Double> {

        TopNScores(int n) {
            super(n);
        }

        @Override
        protected boolean lessThan(Double a, Double b) {
            return a < b;
        }

        double overallScore() {
            double overallScore = 0.0;
            for (double score : this) {
                overallScore += score;
            }
            return size() > 0 ? overallScore / size() : 0.0;
        }
    }
}
