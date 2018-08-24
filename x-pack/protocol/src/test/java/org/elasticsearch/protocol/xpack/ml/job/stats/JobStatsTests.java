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
package org.elasticsearch.protocol.xpack.ml.job.stats;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.ml.NodeAttributes;
import org.elasticsearch.protocol.xpack.ml.NodeAttributesTests;
import org.elasticsearch.protocol.xpack.ml.job.config.JobState;
import org.elasticsearch.protocol.xpack.ml.job.config.JobTests;
import org.elasticsearch.protocol.xpack.ml.job.process.DataCounts;
import org.elasticsearch.protocol.xpack.ml.job.process.DataCountsTests;
import org.elasticsearch.protocol.xpack.ml.job.process.ModelSizeStats;
import org.elasticsearch.protocol.xpack.ml.job.process.ModelSizeStatsTests;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;


public class JobStatsTests extends AbstractXContentTestCase<JobStats> {

    @Override
    protected JobStats createTestInstance() {
        String jobId = JobTests.randomValidJobId();
        JobState state = randomFrom(JobState.CLOSING, JobState.CLOSED, JobState.OPENED, JobState.FAILED, JobState.OPENING);
        DataCounts dataCounts = DataCountsTests.createTestInstance(jobId);

        ModelSizeStats modelSizeStats = randomBoolean() ? ModelSizeStatsTests.createRandomized() : null;
        ForecastStats forecastStats = randomBoolean() ? ForecastStatsTests.createForecastStats(1, 22) : null;
        NodeAttributes nodeAttributes = randomBoolean() ? NodeAttributesTests.createRandom() : null;
        String assigmentExplanation = randomBoolean() ? randomAlphaOfLength(10) : null;
        TimeValue openTime = randomBoolean() ? TimeValue.timeValueMillis(randomIntBetween(1, 10000)) : null;

        return new JobStats(jobId, dataCounts, state, modelSizeStats, forecastStats, nodeAttributes, assigmentExplanation, openTime);
    }

    @Override
    protected JobStats doParseInstance(XContentParser parser) throws IOException {
        return JobStats.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
