/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.support.filtering;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.AbstractFilteringTestCase;

import java.io.IOException;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractXContentFilteringTestCase extends AbstractFilteringTestCase {

    protected final void testFilter(Builder expected, Builder actual, Set<String> includes, Set<String> excludes) throws IOException {
        assertFilterResult(expected.apply(createBuilder()), actual.apply(createBuilder(includes, excludes)));
    }

    protected abstract void assertFilterResult(XContentBuilder expected, XContentBuilder actual);

    protected abstract XContentType getXContentType();

    private XContentBuilder createBuilder() throws IOException {
        return XContentBuilder.builder(getXContentType().xContent());
    }

    private XContentBuilder createBuilder(Set<String> includes, Set<String> excludes) throws IOException {
        return XContentBuilder.builder(getXContentType(), includes, excludes);
    }

    public void testSingleFieldObject() throws IOException {
        final Builder sample = builder -> builder.startObject().startObject("foo").field("bar", "test").endObject().endObject();

        Builder expected = builder -> builder.startObject().startObject("foo").field("bar", "test").endObject().endObject();
        testFilter(expected, sample, singleton("foo.bar"), emptySet());
        testFilter(expected, sample, emptySet(), singleton("foo.baz"));
        testFilter(expected, sample, singleton("foo"), singleton("foo.baz"));

        expected = builder -> builder.startObject().endObject();
        testFilter(expected, sample, emptySet(), singleton("foo.bar"));
        testFilter(expected, sample, singleton("foo"), singleton("foo.b*"));
    }

    static void assertXContentBuilderAsString(final XContentBuilder expected, final XContentBuilder actual) {
        assertThat(Strings.toString(actual), is(Strings.toString(expected)));
    }

    static void assertXContentBuilderAsBytes(final XContentBuilder expected, final XContentBuilder actual) {
        XContent xContent = XContentFactory.xContent(actual.contentType());
        try (
            XContentParser jsonParser =
                xContent.createParser(NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION, BytesReference.bytes(expected).streamInput());
            XContentParser testParser =
                xContent.createParser(NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION, BytesReference.bytes(actual).streamInput());
        ) {
            while (true) {
                XContentParser.Token token1 = jsonParser.nextToken();
                XContentParser.Token token2 = testParser.nextToken();
                if (token1 == null) {
                    assertThat(token2, nullValue());
                    return;
                }
                assertThat(token1, equalTo(token2));
                switch (token1) {
                    case FIELD_NAME:
                        assertThat(jsonParser.currentName(), equalTo(testParser.currentName()));
                        break;
                    case VALUE_STRING:
                        assertThat(jsonParser.text(), equalTo(testParser.text()));
                        break;
                    case VALUE_NUMBER:
                        assertThat(jsonParser.numberType(), equalTo(testParser.numberType()));
                        assertThat(jsonParser.numberValue(), equalTo(testParser.numberValue()));
                        break;
                }
            }
        } catch (Exception e) {
            fail("Fail to verify the result of the XContentBuilder: " + e.getMessage());
        }
    }
}
