/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ErrorCauseWrapperTests extends ESTestCase {

    ErrorCauseWrapper assertWraps(Error realError) {
        var e = ErrorCauseWrapper.maybeWrap(realError);
        assertThat(e.getCause(), nullValue());
        assertThat(e, instanceOf(ErrorCauseWrapper.class));
        var wrapper = (ErrorCauseWrapper) e;
        assertThat(wrapper.realCause, is(realError));
        return wrapper;
    }

    public void testOutOfMemoryError() {
        assertWraps(new OutOfMemoryError("oom"));
    }

    public void testStackOverflowError() {
        assertWraps(new StackOverflowError("soe"));
    }

    public void testLinkageError() {
        assertWraps(new LinkageError("le"));
    }

    public void testPainlessError() {
        assertWraps(new PainlessError("pe"));
    }

    public void testNotWrapped() {
        var realError = new AssertionError("not wrapped");
        var e = ErrorCauseWrapper.maybeWrap(realError);
        assertThat(e, is(realError));
    }

    public void testXContent() throws IOException {
        var e = assertWraps(new PainlessError("some error"));

        var output = new ByteArrayOutputStream();
        var builder = XContentFactory.jsonBuilder(output);
        builder.startObject();
        e.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.flush();

        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, output.toByteArray())) {
            Map<String, String> content = parser.mapStrings();
            assertThat(content, hasEntry("type", "painless_error"));
            assertThat(content, hasEntry("reason", "some error"));
        }

    }
}
