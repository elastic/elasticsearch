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
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

public class JobUpdateTests extends AbstractXContentTestCase<JobUpdate> {

    @Override
    protected JobUpdate createTestInstance() {
        return createRandom(randomAlphaOfLength(4));
    }

    /**
     * Creates a completely random update when the job is null
     * or a random update that is is valid for the given job
     */
    public static JobUpdate createRandom(String jobId) {
        JobUpdate.Builder update = new JobUpdate.Builder(jobId);
        if (randomBoolean()) {
            int groupsNum = randomIntBetween(0, 10);
            List<String> groups = new ArrayList<>(groupsNum);
            for (int i = 0; i < groupsNum; i++) {
                groups.add(JobTests.randomValidJobId());
            }
            update.setGroups(groups);
        }
        if (randomBoolean()) {
            update.setDescription(randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            update.setDetectorUpdates(createRandomDetectorUpdates());
        }
        if (randomBoolean()) {
            update.setModelPlotConfig(new ModelPlotConfig(randomBoolean(), randomAlphaOfLength(10)));
        }
        if (randomBoolean()) {
            update.setAnalysisLimits(AnalysisLimitsTests.createRandomized());
        }
        if (randomBoolean()) {
            update.setRenormalizationWindowDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            update.setBackgroundPersistInterval(TimeValue.timeValueHours(randomIntBetween(1, 24)));
        }
        if (randomBoolean()) {
            update.setModelSnapshotRetentionDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            update.setResultsRetentionDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            update.setCategorizationFilters(Arrays.asList(generateRandomStringArray(10, 10, false)));
        }
        if (randomBoolean()) {
            update.setCustomSettings(Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }
        if (randomBoolean()) {
            update.setAllowLazyOpen(randomBoolean());
        }

        return update.build();
    }


    private static List<JobUpdate.DetectorUpdate> createRandomDetectorUpdates() {
        int size = randomInt(10);
        List<JobUpdate.DetectorUpdate> detectorUpdates = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String detectorDescription = null;
            if (randomBoolean()) {
                detectorDescription = randomAlphaOfLength(12);
            }
            List<DetectionRule> detectionRules = null;
            if (randomBoolean()) {
                detectionRules = new ArrayList<>();
                detectionRules.add(new DetectionRule.Builder(
                        Collections.singletonList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 5))).build());
            }
            detectorUpdates.add(new JobUpdate.DetectorUpdate(i, detectorDescription, detectionRules));
        }
        return detectorUpdates;
    }

    @Override
    protected JobUpdate doParseInstance(XContentParser parser) {
        return JobUpdate.PARSER.apply(parser, null).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }
}
