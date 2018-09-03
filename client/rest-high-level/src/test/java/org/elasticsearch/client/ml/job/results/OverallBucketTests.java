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
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class OverallBucketTests extends AbstractXContentTestCase<OverallBucket> {

    @Override
    protected OverallBucket createTestInstance() {
        return createRandom();
    }

    public static OverallBucket createRandom() {
        int jobCount = randomIntBetween(0, 10);
        List<OverallBucket.JobInfo> jobs = new ArrayList<>(jobCount);
        for (int i = 0; i < jobCount; ++i) {
            jobs.add(new OverallBucket.JobInfo(randomAlphaOfLength(10), randomDoubleBetween(0.0, 100.0, true)));
        }
        OverallBucket overallBucket = new OverallBucket(new Date(randomNonNegativeLong()),
                randomIntBetween(60, 24 * 3600),
                randomDoubleBetween(0.0, 100.0, true),
                randomBoolean());
        overallBucket.setJobs(jobs);
        return overallBucket;
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

    @Override
    protected OverallBucket doParseInstance(XContentParser parser) {
        return OverallBucket.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
