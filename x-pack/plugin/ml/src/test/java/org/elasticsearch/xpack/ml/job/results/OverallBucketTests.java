/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.results.OverallBucket;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class OverallBucketTests extends AbstractWireSerializingTestCase<OverallBucket> {

    @Override
    protected OverallBucket createTestInstance() {
        int jobCount = randomIntBetween(0, 10);
        List<OverallBucket.JobInfo> jobs = new ArrayList<>(jobCount);
        for (int i = 0; i < jobCount; ++i) {
            jobs.add(new OverallBucket.JobInfo(JobTests.randomValidJobId(), randomDoubleBetween(0.0, 100.0, true)));
        }
        return new OverallBucket(
            new Date(randomLongBetween(0, 3000000000000L)),
            randomIntBetween(60, 24 * 3600),
            randomDoubleBetween(0.0, 100.0, true),
            jobs,
            randomBoolean()
        );
    }

    @Override
    protected Writeable.Reader<OverallBucket> instanceReader() {
        return OverallBucket::new;
    }

    public void testCompareTo() {
        OverallBucket.JobInfo jobInfo1 = new OverallBucket.JobInfo("aaa", 1.0);
        OverallBucket.JobInfo jobInfo2 = new OverallBucket.JobInfo("aaa", 3.0);
        OverallBucket.JobInfo jobInfo3 = new OverallBucket.JobInfo("bbb", 1.0);
        assertThat(jobInfo1.compareTo(jobInfo1), equalTo(0));
        assertThat(jobInfo1.compareTo(jobInfo2), lessThan(0));
        assertThat(jobInfo1.compareTo(jobInfo3), lessThan(0));
        assertThat(jobInfo2.compareTo(jobInfo3), lessThan(0));
    }
}
