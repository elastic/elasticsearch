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
package org.elasticsearch.client;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.protocol.xpack.ml.PutJobRequest;
import org.elasticsearch.protocol.xpack.ml.PutJobResponse;
import org.elasticsearch.protocol.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.protocol.xpack.ml.job.config.DataDescription;
import org.elasticsearch.protocol.xpack.ml.job.config.Detector;
import org.elasticsearch.protocol.xpack.ml.job.config.Job;
import org.elasticsearch.protocol.xpack.ml.job.config.ModelPlotConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class MachineLearningIT extends ESRestHighLevelClientTestCase {

    public void testPutJob() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildRandomJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        PutJobResponse putJobResponse = execute(new PutJobRequest(job), machineLearningClient::putJob, machineLearningClient::putJobAsync);
        Job createdJob = putJobResponse.getResponse();

        assertThat(createdJob.getId(), is(jobId));
        assertThat(createdJob.getJobType(), is(Job.ANOMALY_DETECTOR_JOB_TYPE));
    }

    public static String randomValidJobId() {
        CodepointSetGenerator generator = new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz0123456789".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }

    private static Job buildRandomJob(String jobId) {
        Job.Builder builder = new Job.Builder(jobId);
        if (randomBoolean()) {
            builder.setDescription(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            int groupsNum = randomIntBetween(0, 10);
            List<String> groups = new ArrayList<>(groupsNum);
            for (int i = 0; i < groupsNum; i++) {
                groups.add(randomValidJobId());
            }
            builder.setGroups(groups);
        }
        if (randomBoolean()) {
            builder.setEstablishedModelMemory(randomNonNegativeLong());
        }
        Detector detector = new Detector.Builder().setFieldName("some-field").setFunction("mean").build();
        builder.setAnalysisConfig(new AnalysisConfig.Builder(Arrays.asList(detector)));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(randomFrom(DataDescription.DataFormat.values()));
        builder.setDataDescription(dataDescription);

        if (randomBoolean()) {
            builder.setModelPlotConfig(new ModelPlotConfig(randomBoolean(), randomAlphaOfLength(10)));
        }
        if (randomBoolean()) {
            builder.setRenormalizationWindowDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setBackgroundPersistInterval(TimeValue.timeValueHours(randomIntBetween(1, 24)));
        }
        if (randomBoolean()) {
            builder.setResultsRetentionDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setCustomSettings(Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }
        if (randomBoolean()) {
            builder.setResultsIndexName(randomValidJobId());
        }
        return builder.build();
    }
}
