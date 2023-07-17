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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class StackFrameTests extends ESTestCase {
    public void testCreateFromRegularSource() {
        // tag::noformat
        StackFrame frame = StackFrame.fromSource(
            Map.of(
                "Stackframe.file.name", "Main.java",
                "Stackframe.function.name", "helloWorld",
                "Stackframe.line.number", 22
            )
        );
        // end::noformat
        assertEquals(List.of("Main.java"), frame.fileName);
        assertEquals(List.of("helloWorld"), frame.functionName);
        assertEquals(Collections.emptyList(), frame.functionOffset);
        assertEquals(List.of(22), frame.lineNumber);
    }

    public void testToXContent() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder expectedRequest = XContentFactory.contentBuilder(contentType)
            .startObject()
            .array("file_name", "Main.java")
            .array("function_name", "helloWorld")
            .array("function_offset", 31733)
            .array("line_number", 22)
            .endObject();

        XContentBuilder actualRequest = XContentFactory.contentBuilder(contentType);
        StackFrame stackTrace = new StackFrame("Main.java", "helloWorld", 31733, 22);
        stackTrace.toXContent(actualRequest, ToXContent.EMPTY_PARAMS);

        assertToXContentEquivalent(BytesReference.bytes(expectedRequest), BytesReference.bytes(actualRequest), contentType);
    }

    public void testEquality() {
        StackFrame frame = new StackFrame("Main.java", "helloWorld", 31733, 22);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            frame,
            (o -> new StackFrame(o.fileName, o.functionName, o.functionOffset, o.lineNumber))
        );

    }
}
