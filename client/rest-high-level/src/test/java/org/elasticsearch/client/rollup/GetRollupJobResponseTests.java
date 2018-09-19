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

package org.elasticsearch.client.rollup;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.client.rollup.GetRollupJobResponse.IndexerState;
import org.elasticsearch.client.rollup.GetRollupJobResponse.JobWrapper;
import org.elasticsearch.client.rollup.GetRollupJobResponse.RollupIndexerJobStats;
import org.elasticsearch.client.rollup.GetRollupJobResponse.RollupJobStatus;
import org.elasticsearch.client.rollup.job.config.RollupJobConfigTests;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class GetRollupJobResponseTests extends AbstractXContentTestCase<GetRollupJobResponse> {
    @Override
    protected GetRollupJobResponse createTestInstance() {
        int jobCount = between(1, 5);
        List<JobWrapper> jobs = new ArrayList<>();
        for (int j = 0; j < jobCount; j++) {
            jobs.add(new JobWrapper(
                    RollupJobConfigTests.randomRollupJobConfig(randomAlphaOfLength(5)),
                    randomStats(),
                    randomStatus()));
        }
        return new GetRollupJobResponse(jobs);
    }

    private RollupIndexerJobStats randomStats() {
        return new RollupIndexerJobStats(randomLong(), randomLong(), randomLong(), randomLong());
    }

    private RollupJobStatus randomStatus() {
        Map<String, Object> currentPosition = new HashMap<>();
        int positions = between(0, 10);
        while (currentPosition.size() < positions) {
            currentPosition.put(randomAlphaOfLength(2), randomAlphaOfLength(2));
        }
        return new RollupJobStatus(
            randomFrom(IndexerState.values()),
            currentPosition,
            randomBoolean());
    }

    @Override
    protected GetRollupJobResponse doParseInstance(XContentParser parser) throws IOException {
        return GetRollupJobResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    /**
     * Returns a predicate that given the field name indicates whether the field has to be excluded from random fields insertion or not
     */
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.endsWith("status.current_position");
    }
}
