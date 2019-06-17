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
package org.elasticsearch.client.ml.job.stats;

import org.elasticsearch.client.ml.NodeAttributes;
import org.elasticsearch.client.ml.NodeAttributesTests;
import org.elasticsearch.client.ml.job.process.DataCounts;
import org.elasticsearch.client.ml.job.process.DataCountsTests;
import org.elasticsearch.client.ml.job.process.ModelSizeStats;
import org.elasticsearch.client.ml.job.process.ModelSizeStatsTests;
import org.elasticsearch.client.ml.job.process.TimingStats;
import org.elasticsearch.client.ml.job.process.TimingStatsTests;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.client.ml.job.config.JobState;
import org.elasticsearch.client.ml.job.config.JobTests;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;


public class JobStatsTests extends AbstractXContentTestCase<JobStats> {

    public static JobStats createRandomInstance() {
        String jobId = JobTests.randomValidJobId();
        JobState state = randomFrom(JobState.CLOSING, JobState.CLOSED, JobState.OPENED, JobState.FAILED, JobState.OPENING);
        DataCounts dataCounts = DataCountsTests.createTestInstance(jobId);

        ModelSizeStats modelSizeStats = randomBoolean() ? ModelSizeStatsTests.createRandomized() : null;
        TimingStats timingStats = randomBoolean() ? TimingStatsTests.createTestInstance(jobId) : null;
        ForecastStats forecastStats = randomBoolean() ? ForecastStatsTests.createRandom(1, 22) : null;
        NodeAttributes nodeAttributes = randomBoolean() ? NodeAttributesTests.createRandom() : null;
        String assigmentExplanation = randomBoolean() ? randomAlphaOfLength(10) : null;
        TimeValue openTime = randomBoolean() ? TimeValue.timeValueMillis(randomIntBetween(1, 10000)) : null;

        return new JobStats(
            jobId, dataCounts, state, modelSizeStats, timingStats, forecastStats, nodeAttributes, assigmentExplanation, openTime);
    }

    @Override
    protected JobStats createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected JobStats doParseInstance(XContentParser parser) throws IOException {
        return JobStats.PARSER.parse(parser, null);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
