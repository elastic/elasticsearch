/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class StackFrameTests extends ESTestCase {
    public void testCreateFromSource() {
        // tag::noformat
        StackFrame frame = StackFrame.fromSource(
            Map.of("Stackframe", Map.of(
                "file", Map.of("name", "Main.java"),
                "function", Map.of(
                        "name", "helloWorld",
                        "offset", 31733
                    ),
                "line", Map.of("number", 22),
                "source", Map.of("type", 3))
                )
        );
        // end::noformat
        assertEquals(Integer.valueOf(3), frame.sourceType);
        assertEquals("Main.java", frame.fileName);
        assertEquals("helloWorld", frame.functionName);
        assertEquals(Integer.valueOf(31733), frame.functionOffset);
        assertEquals(Integer.valueOf(22), frame.lineNumber);
    }

    public void testToXContent() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder expectedRequest = XContentFactory.contentBuilder(contentType)
            .startObject()
            .field("file_name", "Main.java")
            .field("function_name", "helloWorld")
            .field("function_offset", 31733)
            .field("line_number", 22)
            .field("source_type", 3)
            .endObject();

        XContentBuilder actualRequest = XContentFactory.contentBuilder(contentType);
        StackFrame stackTrace = new StackFrame("Main.java", "helloWorld", 31733, 22, 3);
        stackTrace.toXContent(actualRequest, ToXContent.EMPTY_PARAMS);

        assertToXContentEquivalent(BytesReference.bytes(expectedRequest), BytesReference.bytes(actualRequest), contentType);
    }

    public void testEquality() {
        StackFrame frame = new StackFrame("Main.java", "helloWorld", 31733, 22, 3);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            frame,
            (o -> new StackFrame(o.fileName, o.functionName, o.functionOffset, o.lineNumber, o.sourceType))
        );

    }
}
