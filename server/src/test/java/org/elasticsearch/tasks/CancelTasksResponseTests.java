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

package org.elasticsearch.tasks;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class CancelTasksResponseTests extends AbstractXContentTestCase<CancelTasksResponse> {

    @Override
    protected CancelTasksResponse createTestInstance() {
        List<TaskInfo> randomTasks = randomTasks();
        return new CancelTasksResponse(randomTasks, Collections.emptyList(), Collections.emptyList());
    }

    private static List<TaskInfo> randomTasks() {
        List<TaskInfo> randomTasks = new ArrayList<>();
        for (int i = 0; i < randomInt(10); i++) {
            randomTasks.add(TaskInfoTests.randomTaskInfo());
        }
        return randomTasks;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        //status and headers hold arbitrary content, we can't inject random fields in them
        return field -> field.endsWith("status") || field.endsWith("headers");
    }

    @Override
    protected void assertEqualInstances(CancelTasksResponse expectedInstance, CancelTasksResponse newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance.getTasks(), equalTo(expectedInstance.getTasks()));
        ListTasksResponseTests.assertOnNodeFailures(newInstance.getNodeFailures(), expectedInstance.getNodeFailures());
        ListTasksResponseTests.assertOnTaskFailures(newInstance.getTaskFailures(), expectedInstance.getTaskFailures());
    }

    @Override
    protected CancelTasksResponse doParseInstance(XContentParser parser) {
        return CancelTasksResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return true;
    }

    /**
     * Test parsing {@link ListTasksResponse} with inner failures as they don't support asserting on xcontent equivalence, given that
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<CancelTasksResponse> instanceSupplier = CancelTasksResponseTests::createTestInstanceWithFailures;
        //with random fields insertion in the inner exceptions, some random stuff may be parsed back as metadata,
        //but that does not bother our assertions, as we only want to test that we don't break.
        boolean supportsUnknownFields = true;
        //exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(NUMBER_OF_TEST_RUNS, instanceSupplier, supportsUnknownFields, Strings.EMPTY_ARRAY,
            getRandomFieldsExcludeFilter(), this::createParser, this::doParseInstance,
            this::assertEqualInstances, assertToXContentEquivalence, ToXContent.EMPTY_PARAMS);
    }

    private static CancelTasksResponse createTestInstanceWithFailures() {
        int numNodeFailures = randomIntBetween(0, 3);
        List<FailedNodeException> nodeFailures = new ArrayList<>(numNodeFailures);
        for (int i = 0; i < numNodeFailures; i++) {
            nodeFailures.add(new FailedNodeException(randomAlphaOfLength(5), "error message", new ConnectException()));
        }
        int numTaskFailures = randomIntBetween(0, 3);
        List<TaskOperationFailure> taskFailures = new ArrayList<>(numTaskFailures);
        for (int i = 0; i < numTaskFailures; i++) {
            taskFailures.add(new TaskOperationFailure(randomAlphaOfLength(5), randomLong(), new IllegalStateException()));
        }
        return new CancelTasksResponse(randomTasks(), taskFailures, nodeFailures);
    }

}
