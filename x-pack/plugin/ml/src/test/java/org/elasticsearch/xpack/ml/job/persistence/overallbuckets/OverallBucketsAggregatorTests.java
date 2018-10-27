/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence.overallbuckets;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.results.OverallBucket;
import org.elasticsearch.xpack.core.ml.job.results.OverallBucket.JobInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class OverallBucketsAggregatorTests extends ESTestCase {

    public void testProcess_GivenEmpty() {
        OverallBucketsAggregator aggregator = new OverallBucketsAggregator(TimeValue.timeValueHours(1));
        aggregator.process(Collections.emptyList());
        List<OverallBucket> aggregated = aggregator.finish();
        assertThat(aggregated.isEmpty(), is(true));
    }

    public void testProcess_GivenAggSpanIsTwiceTheBucketSpan() {
        // Monday, October 16, 2017 12:00:00 AM UTC
        long startTime = 1508112000000L;

        List<OverallBucket> rawBuckets1 = new ArrayList<>();
        List<OverallBucket> rawBuckets2 = new ArrayList<>();
        rawBuckets1.add(new OverallBucket(new Date(startTime), 3600L, 10.0,
                Arrays.asList(new OverallBucket.JobInfo("job_1", 10.0),
                        new OverallBucket.JobInfo("job_2", 6.0)),
                false));
        rawBuckets1.add(new OverallBucket(new Date(startTime + TimeValue.timeValueHours(1).millis()), 3600L, 20.0,
                Arrays.asList(new JobInfo("job_1", 20.0), new JobInfo("job_2", 2.0)),
                false));
        rawBuckets1.add(new OverallBucket(new Date(startTime + TimeValue.timeValueHours(2).millis()), 3600L, 30.0,
                Arrays.asList(new JobInfo("job_1", 30.0), new JobInfo("job_2", 7.0)),
                false));
        rawBuckets1.add(new OverallBucket(new Date(startTime + TimeValue.timeValueHours(3).millis()), 3600L, 40.0,
                Arrays.asList(new JobInfo("job_1", 10.0), new JobInfo("job_2", 40.0)),
                false));
        rawBuckets1.add(new OverallBucket(new Date(startTime + TimeValue.timeValueHours(4).millis()), 3600L, 50.0,
                Collections.singletonList(new JobInfo("job_1", 50.0)), false));
        rawBuckets1.add(new OverallBucket(new Date(startTime + TimeValue.timeValueHours(5).millis()), 3600L, 60.0,
                Collections.singletonList(new JobInfo("job_1", 60.0)), true));
        rawBuckets1.add(new OverallBucket(new Date(startTime + TimeValue.timeValueHours(6).millis()), 3600L, 70.0,
                Arrays.asList(new JobInfo("job_1", 70.0), new JobInfo("job_2", 0.0)),
                true));


        TimeValue bucketSpan = TimeValue.timeValueHours(2);
        OverallBucketsAggregator aggregator = new OverallBucketsAggregator(bucketSpan);
        aggregator.process(rawBuckets1);
        aggregator.process(rawBuckets2);
        List<OverallBucket> aggregated = aggregator.finish();

        assertThat(aggregated.size(), equalTo(4));
        assertThat(aggregated.get(0).getTimestamp().getTime(), equalTo(startTime));
        assertThat(aggregated.get(0).getOverallScore(), equalTo(20.0));
        assertThat(aggregated.get(0).getJobs(), contains(new JobInfo("job_1", 20.0),
                new JobInfo("job_2", 6.0)));
        assertThat(aggregated.get(0).isInterim(), is(false));
        assertThat(aggregated.get(1).getTimestamp().getTime(), equalTo(startTime + bucketSpan.millis()));
        assertThat(aggregated.get(1).getOverallScore(), equalTo(40.0));
        assertThat(aggregated.get(1).getJobs(), contains(new JobInfo("job_1", 30.0),
                new JobInfo("job_2", 40.0)));
        assertThat(aggregated.get(1).isInterim(), is(false));
        assertThat(aggregated.get(2).getTimestamp().getTime(), equalTo(startTime + 2 * bucketSpan.millis()));
        assertThat(aggregated.get(2).getOverallScore(), equalTo(60.0));
        assertThat(aggregated.get(2).getJobs().size(), equalTo(1));
        assertThat(aggregated.get(2).getJobs(), contains(new JobInfo("job_1", 60.0)));
        assertThat(aggregated.get(2).isInterim(), is(true));
        assertThat(aggregated.get(3).getTimestamp().getTime(), equalTo(startTime + 3 * bucketSpan.millis()));
        assertThat(aggregated.get(3).getOverallScore(), equalTo(70.0));
        assertThat(aggregated.get(3).getJobs(), contains(new JobInfo("job_1", 70.0),
                new JobInfo("job_2", 0.0)));
        assertThat(aggregated.get(3).isInterim(), is(true));
    }
}
