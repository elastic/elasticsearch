/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.tasks.TaskInfoTests.randomTaskInfo;

/**
 * Round trip tests for {@link TaskResult} and those classes that it includes like {@link TaskInfo} and {@link RawTaskStatus}.
 */
public class TaskResultTests extends ESTestCase {
    public void testBinaryRoundTrip() throws IOException {
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            Collections.singletonList(new NamedWriteableRegistry.Entry(Task.Status.class, RawTaskStatus.NAME, RawTaskStatus::new))
        );
        TaskResult result = randomTaskResult();
        TaskResult read;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            result.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                read = new TaskResult(in);
            }
        } catch (IOException e) {
            throw new IOException("Error processing [" + result + "]", e);
        }
        assertEquals(result, read);
    }

    public void testXContentRoundTrip() throws IOException {
        /*
         * Note that this round trip isn't 100% perfect - status will always be read as RawTaskStatus. Since this test uses RawTaskStatus
         * as the status we randomly generate then we can assert the round trip with .equals.
         */
        TaskResult result = randomTaskResult();
        TaskResult read;
        try (XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.values()).xContent())) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentBuilder shuffled = shuffleXContent(builder); XContentParser parser = createParser(shuffled)) {
                read = TaskResult.PARSER.apply(parser, null);
            }
        } catch (IOException e) {
            throw new IOException("Error processing [" + result + "]", e);
        }
        assertEquals(result, read);
    }

    public void testTaskInfoIsForwardCompatible() throws IOException {
        TaskInfo taskInfo = randomTaskInfo();
        TaskInfo read;
        try (XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.values()).xContent())) {
            builder.startObject();
            taskInfo.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            try (XContentBuilder withExtraFields = addRandomUnknownFields(builder)) {
                try (XContentBuilder shuffled = shuffleXContent(withExtraFields)) {
                    try (XContentParser parser = createParser(shuffled)) {
                        read = TaskInfo.PARSER.apply(parser, null);
                    }
                }
            }
        } catch (IOException e) {
            throw new IOException("Error processing [" + taskInfo + "]", e);
        }
        assertEquals(taskInfo, read);
    }

    private XContentBuilder addRandomUnknownFields(XContentBuilder builder) throws IOException {
        try (XContentParser parser = createParser(builder)) {
            Map<String, Object> map = parser.mapOrdered();
            int numberOfNewFields = randomIntBetween(2, 10);
            for (int i = 0; i < numberOfNewFields; i++) {
                if (randomBoolean()) {
                    map.put("unknown_field" + i, randomAlphaOfLength(20));
                } else {
                    map.put("unknown_field" + i, Collections.singletonMap("inner", randomAlphaOfLength(20)));
                }
            }
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(parser.contentType());
            return xContentBuilder.map(map);
        }
    }

    private static TaskResult randomTaskResult() throws IOException {
        return switch (between(0, 2)) {
            case 0 -> new TaskResult(randomBoolean(), randomTaskInfo());
            case 1 -> new TaskResult(randomTaskInfo(), new RuntimeException("error"));
            case 2 -> new TaskResult(randomTaskInfo(), randomTaskResponse());
            default -> throw new UnsupportedOperationException("Unsupported random TaskResult constructor");
        };
    }

    private static ToXContent randomTaskResponse() {
        Map<String, String> result = new TreeMap<>();
        int fields = between(0, 10);
        for (int f = 0; f < fields; f++) {
            result.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        return new ToXContent() {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                // Responses in Elasticsearch never output a leading startObject. There isn't really a good reason, they just don't.
                for (Map.Entry<String, String> entry : result.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                return builder;
            }
        };
    }
}
