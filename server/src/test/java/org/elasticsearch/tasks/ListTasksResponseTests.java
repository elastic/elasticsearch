/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ListTasksResponseTests extends AbstractXContentTestCase<ListTasksResponseTests.ListTasksResponseWrapper> {

    // ListTasksResponse doesn't directly implement ToXContent because it has multiple XContent representations, so we must wrap here
    public record ListTasksResponseWrapper(ListTasksResponse in) implements ToXContentObject {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return ChunkedToXContent.wrapAsToXContent(in.groupedByNone()).toXContent(builder, params);
        }
    }

    public void testEmptyToString() {
        assertEquals("""
            {
              "tasks" : [ ]
            }""", new ListTasksResponse(null, null, null).toString());
    }

    public void testNonEmptyToString() {
        TaskInfo info = new TaskInfo(
            new TaskId("node1", 1),
            "dummy-type",
            "dummy-action",
            "dummy-description",
            null,
            0,
            1,
            true,
            false,
            new TaskId("node1", 0),
            Collections.singletonMap("foo", "bar")
        );
        ListTasksResponse tasksResponse = new ListTasksResponse(singletonList(info), emptyList(), emptyList());
        assertEquals("""
            {
              "tasks" : [
                {
                  "node" : "node1",
                  "id" : 1,
                  "type" : "dummy-type",
                  "action" : "dummy-action",
                  "description" : "dummy-description",
                  "start_time" : "1970-01-01T00:00:00.000Z",
                  "start_time_in_millis" : 0,
                  "running_time" : "1nanos",
                  "running_time_in_nanos" : 1,
                  "cancellable" : true,
                  "cancelled" : false,
                  "parent_task_id" : "node1:0",
                  "headers" : {
                    "foo" : "bar"
                  }
                }
              ]
            }""", tasksResponse.toString());
    }

    @Override
    protected ListTasksResponseWrapper createTestInstance() {
        // failures are tested separately, so we can test xcontent equivalence at least when we have no failures
        return new ListTasksResponseWrapper(new ListTasksResponse(randomTasks(), Collections.emptyList(), Collections.emptyList()));
    }

    private static List<TaskInfo> randomTasks() {
        List<TaskInfo> tasks = new ArrayList<>();
        for (int i = 0; i < randomInt(10); i++) {
            tasks.add(TaskInfoTests.randomTaskInfo());
        }
        return tasks;
    }

    @Override
    protected ListTasksResponseWrapper doParseInstance(XContentParser parser) {
        return new ListTasksResponseWrapper(ListTasksResponse.fromXContent(parser));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // status and headers hold arbitrary content, we can't inject random fields in them
        return field -> field.endsWith("status") || field.endsWith("headers");
    }

    @Override
    protected void assertEqualInstances(ListTasksResponseWrapper expectedInstanceWrapper, ListTasksResponseWrapper newInstanceWrapper) {
        final var expectedInstance = expectedInstanceWrapper.in();
        final var newInstance = newInstanceWrapper.in();
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance.getTasks(), equalTo(expectedInstance.getTasks()));
        assertOnNodeFailures(newInstance.getNodeFailures(), expectedInstance.getNodeFailures());
        assertOnTaskFailures(newInstance.getTaskFailures(), expectedInstance.getTaskFailures());
    }

    protected static void assertOnNodeFailures(List<ElasticsearchException> nodeFailures, List<ElasticsearchException> expectedFailures) {
        assertThat(nodeFailures.size(), equalTo(expectedFailures.size()));
        for (int i = 0; i < nodeFailures.size(); i++) {
            ElasticsearchException newException = nodeFailures.get(i);
            ElasticsearchException expectedException = expectedFailures.get(i);
            assertThat(newException.getMetadata("es.node_id").get(0), equalTo(((FailedNodeException) expectedException).nodeId()));
            assertThat(newException.getMessage(), equalTo("Elasticsearch exception [type=failed_node_exception, reason=error message]"));
            assertThat(newException.getCause(), instanceOf(ElasticsearchException.class));
            ElasticsearchException cause = (ElasticsearchException) newException.getCause();
            assertThat(cause.getMessage(), equalTo("Elasticsearch exception [type=connect_exception, reason=null]"));
        }
    }

    protected static void assertOnTaskFailures(List<TaskOperationFailure> taskFailures, List<TaskOperationFailure> expectedFailures) {
        assertThat(taskFailures.size(), equalTo(expectedFailures.size()));
        for (int i = 0; i < taskFailures.size(); i++) {
            TaskOperationFailure newFailure = taskFailures.get(i);
            TaskOperationFailure expectedFailure = expectedFailures.get(i);
            assertThat(newFailure.getNodeId(), equalTo(expectedFailure.getNodeId()));
            assertThat(newFailure.getTaskId(), equalTo(expectedFailure.getTaskId()));
            assertThat(newFailure.getStatus(), equalTo(expectedFailure.getStatus()));
            assertThat(newFailure.getCause(), instanceOf(ElasticsearchException.class));
            ElasticsearchException cause = (ElasticsearchException) newFailure.getCause();
            assertThat(cause.getMessage(), equalTo("Elasticsearch exception [type=illegal_state_exception, reason=null]"));
        }
    }

    /**
     * Test parsing {@link ListTasksResponse} with inner failures as they don't support asserting on xcontent equivalence, given that
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<ListTasksResponseWrapper> instanceSupplier = ListTasksResponseTests::createTestInstanceWithFailures;
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
            EMPTY_PARAMS
        );
    }

    public void testChunkedEncoding() {
        final var response = createTestInstanceWithFailures().in();
        AbstractChunkedSerializingTestCase.assertChunkCount(response.groupedByNone(), o -> response.getTasks().size() + 2);
        AbstractChunkedSerializingTestCase.assertChunkCount(response.groupedByParent(), o -> response.getTaskGroups().size() + 2);
        AbstractChunkedSerializingTestCase.assertChunkCount(
            response.groupedByNode(() -> DiscoveryNodes.EMPTY_NODES),
            o -> 2 + response.getPerNodeTasks().values().stream().mapToInt(entry -> 2 + entry.size()).sum()
        );
    }

    private static ListTasksResponseWrapper createTestInstanceWithFailures() {
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
        return new ListTasksResponseWrapper(new ListTasksResponse(randomTasks(), taskFailures, nodeFailures));
    }
}
