/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformStats;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformStatsTests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class GetDataFrameTransformStatsResponseTests extends ESTestCase {

    public void testXContentParser() throws IOException {
        xContentTester(this::createParser,
                GetDataFrameTransformStatsResponseTests::createTestInstance,
                GetDataFrameTransformStatsResponseTests::toXContent,
                GetDataFrameTransformStatsResponse::fromXContent)
                .assertEqualsConsumer(GetDataFrameTransformStatsResponseTests::assertEqualInstances)
                .assertToXContentEquivalence(false)
                .supportsUnknownFields(true)
                .randomFieldsExcludeFilter(path -> path.isEmpty() == false)
                .test();
    }

    private static GetDataFrameTransformStatsResponse createTestInstance() {
        int count = randomIntBetween(1, 3);
        List<DataFrameTransformStats> stats = new ArrayList<>();
        for (int i=0; i<count; i++) {
            stats.add(DataFrameTransformStatsTests.randomInstance());
        }

        List<TaskOperationFailure> taskFailures = null;
        if (randomBoolean()) {
            taskFailures = new ArrayList<>();
            int numTaskFailures = randomIntBetween(1, 4);
            for (int i=0; i<numTaskFailures; i++) {
                taskFailures.add(new TaskOperationFailure(randomAlphaOfLength(4), randomNonNegativeLong(), new IllegalStateException()));
            }
        }
        List<ElasticsearchException> nodeFailures = null;
        if (randomBoolean()) {
            nodeFailures = new ArrayList<>();
            int numNodeFailures = randomIntBetween(1, 4);
            for (int i=0; i<numNodeFailures; i++) {
                nodeFailures.add(new ElasticsearchException("GetDataFrameTransformStatsResponseTests"));
            }
        }

        return new GetDataFrameTransformStatsResponse(stats, taskFailures, nodeFailures);
    }

    private static void toXContent(GetDataFrameTransformStatsResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        {
            builder.startArray("transforms");
            for (DataFrameTransformStats stats : response.getTransformsStats()) {
                DataFrameTransformStatsTests.toXContent(stats, builder);
            }
            builder.endArray();

            AcknowledgedTasksResponseTests.taskFailuresToXContent(response.getTaskFailures(), builder);
            AcknowledgedTasksResponseTests.nodeFailuresToXContent(response.getNodeFailures(), builder);
        }
        builder.endObject();
    }

    // Serialisation of TaskOperationFailure and ElasticsearchException changes
    // the object so use a custom compare method rather than Object.equals
    private static void assertEqualInstances(GetDataFrameTransformStatsResponse expected,
                                             GetDataFrameTransformStatsResponse actual) {
        assertEquals(expected.getTransformsStats(), actual.getTransformsStats());
        AcknowledgedTasksResponseTests.assertTaskOperationFailuresEqual(expected.getTaskFailures(), actual.getTaskFailures());
        AcknowledgedTasksResponseTests.assertNodeFailuresEqual(expected.getNodeFailures(), actual.getNodeFailures());
    }
}
