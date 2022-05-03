/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

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
        // status and headers hold arbitrary content, we can't inject random fields in them
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
        // with random fields insertion in the inner exceptions, some random stuff may be parsed back as metadata,
        // but that does not bother our assertions, as we only want to test that we don't break.
        boolean supportsUnknownFields = true;
        // exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(
            NUMBER_OF_TEST_RUNS,
            instanceSupplier,
            supportsUnknownFields,
            Strings.EMPTY_ARRAY,
            getRandomFieldsExcludeFilter(),
            this::createParser,
            this::doParseInstance,
            this::assertEqualInstances,
            assertToXContentEquivalence,
            ToXContent.EMPTY_PARAMS
        );
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
