/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence.overallbuckets;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.job.results.OverallBucket;
import org.elasticsearch.xpack.core.ml.utils.Intervals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class OverallBucketsAggregator implements OverallBucketsProcessor {

    private final long bucketSpanSeconds;
    private final long bucketSpanMillis;
    private double maxOverallScore = 0.0;
    private Map<String, Double> maxScoreByJob = new TreeMap<>();
    private boolean isInterim = false;
    private Long startTime;
    private final List<OverallBucket> aggregated = new ArrayList<>();

    public OverallBucketsAggregator(TimeValue bucketSpan) {
        bucketSpanSeconds = bucketSpan.seconds();
        bucketSpanMillis = bucketSpan.millis();
    }

    @Override
    public synchronized void process(List<OverallBucket> buckets) {
        if (buckets.isEmpty()) {
            return;
        }
        if (startTime == null) {
            startTime = Intervals.alignToFloor(buckets.get(0).getTimestamp().getTime(), bucketSpanMillis);
        }
        long bucketTime;
        for (OverallBucket bucket : buckets) {
            bucketTime = bucket.getTimestamp().getTime();
            if (bucketTime >= startTime + bucketSpanMillis) {
                aggregated.add(outputBucket());
                startNextBucket(bucketTime);
            }
            processBucket(bucket);
        }
    }

    private OverallBucket outputBucket() {
        List<OverallBucket.JobInfo> jobs = new ArrayList<>(maxScoreByJob.size());
        maxScoreByJob.entrySet().stream().forEach(entry -> jobs.add(
                new OverallBucket.JobInfo(entry.getKey(), entry.getValue())));
        return new OverallBucket(new Date(startTime), bucketSpanSeconds, maxOverallScore, jobs, isInterim);
    }

    private void startNextBucket(long bucketTime) {
        maxOverallScore = 0.0;
        maxScoreByJob.clear();
        isInterim = false;
        startTime = Intervals.alignToFloor(bucketTime, bucketSpanMillis);
    }

    private void processBucket(OverallBucket bucket) {
        maxOverallScore = Math.max(maxOverallScore, bucket.getOverallScore());
        bucket.getJobs().stream().forEach(j -> {
            double currentMax = maxScoreByJob.computeIfAbsent(j.getJobId(), k -> 0.0);
            if (j.getMaxAnomalyScore() > currentMax) {
                maxScoreByJob.put(j.getJobId(), j.getMaxAnomalyScore());
            }
        });
        isInterim |= bucket.isInterim();
    }

    @Override
    public synchronized List<OverallBucket> finish() {
        if (startTime != null) {
            aggregated.add(outputBucket());
        }
        return aggregated;
    }

    @Override
    public synchronized int size() {
        return aggregated.size();
    }
}
