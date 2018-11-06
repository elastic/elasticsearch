package org.elasticsearch.client.tasks;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class TaskSubmissionResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            this::createTestInstance,
            this::toXContent,
            TaskSubmissionResponse::fromXContent)
            .supportsUnknownFields(true)
            .test();
    }

    private void toXContent(TaskSubmissionResponse response, XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.startObject();
        xContentBuilder.field("task", response.getTask().toString());
        xContentBuilder.endObject();
    }

    private TaskSubmissionResponse createTestInstance() {
        TaskId taskId = new TaskId(randomAlphaOfLength(5), randomLong());
        return new TaskSubmissionResponse(taskId);
    }
}
