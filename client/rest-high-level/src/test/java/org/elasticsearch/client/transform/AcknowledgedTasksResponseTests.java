/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.hamcrest.Matchers.containsString;

public class AcknowledgedTasksResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
                this::createTestInstance,
                AcknowledgedTasksResponseTests::toXContent,
                AcknowledgedTasksResponseTests::fromXContent)
                .assertEqualsConsumer(AcknowledgedTasksResponseTests::assertEqualInstances)
                .assertToXContentEquivalence(false)
                .supportsUnknownFields(false)
                .test();
    }

    // Serialisation of TaskOperationFailure and ElasticsearchException changes
    // the object so use a custom compare method rather than Object.equals
    private static void assertEqualInstances(AcknowledgedTasksResponse expected, AcknowledgedTasksResponse actual) {
        assertNotSame(expected, actual);
        assertEquals(expected.isAcknowledged(), actual.isAcknowledged());

        assertTaskOperationFailuresEqual(expected.getTaskFailures(), actual.getTaskFailures());
        assertNodeFailuresEqual(expected.getNodeFailures(), actual.getNodeFailures());
    }

    private static <T> void assertListEquals(List<T> expected, List<T> actual, BiPredicate<T, T> comparator) {
        if (expected == null) {
            assertNull(actual);
            return;
        } else {
            assertNotNull(actual);
        }

        assertEquals(expected.size(), actual.size());
        for (int i=0; i<expected.size(); i++) {
            assertTrue(comparator.test(expected.get(i), actual.get(i)));
        }
    }

    public static void assertTaskOperationFailuresEqual(List<TaskOperationFailure> expected,
                                                        List<TaskOperationFailure> actual) {
        assertListEquals(expected, actual, (a, b) ->
                Objects.equals(a.getNodeId(), b.getNodeId())
                        && Objects.equals(a.getTaskId(), b.getTaskId())
                        && Objects.equals(a.getStatus(), b.getStatus())
        );
    }

    public static void assertNodeFailuresEqual(List<ElasticsearchException> expected,
                                               List<ElasticsearchException> actual) {
        // actualException is a wrapped copy of expectedException so the
        // error messages won't be the same but actualException should contain
        // the error message from expectedException
        assertListEquals(expected, actual, (expectedException, actualException) -> {
            assertThat(actualException.getDetailedMessage(), containsString(expectedException.getMessage()));
            return true;
        });
    }

    private static AcknowledgedTasksResponse fromXContent(XContentParser parser) {
        return AcknowledgedTasksResponse.generateParser("ack_tasks_response",
                AcknowledgedTasksResponse::new, "acknowleged")
                .apply(parser, null);
    }

    private AcknowledgedTasksResponse createTestInstance() {
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
                nodeFailures.add(new ElasticsearchException("AcknowledgedTasksResponseTest"));
            }
        }

        return new AcknowledgedTasksResponse(randomBoolean(), taskFailures, nodeFailures);
    }

    public static void toXContent(AcknowledgedTasksResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        {
            builder.field("acknowleged", response.isAcknowledged());
            taskFailuresToXContent(response.getTaskFailures(), builder);
            nodeFailuresToXContent(response.getNodeFailures(), builder);
        }
        builder.endObject();
    }

    public static void taskFailuresToXContent(List<TaskOperationFailure> taskFailures, XContentBuilder builder) throws IOException {
        if (taskFailures != null && taskFailures.isEmpty() == false) {
            builder.startArray(AcknowledgedTasksResponse.TASK_FAILURES.getPreferredName());
            for (TaskOperationFailure failure : taskFailures) {
                builder.startObject();
                failure.toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    public static void nodeFailuresToXContent(List<ElasticsearchException> nodeFailures, XContentBuilder builder) throws IOException {
        if (nodeFailures != null && nodeFailures.isEmpty() == false) {
            builder.startArray(AcknowledgedTasksResponse.NODE_FAILURES.getPreferredName());
            for (ElasticsearchException failure : nodeFailures) {
                builder.startObject();
                failure.toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
            }
            builder.endArray();
        }
    }
}


