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

package org.elasticsearch.common.xcontent.support.filtering;

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
        return XContentBuilder.builder(getXContentType().xContent(), includes, excludes);
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
        assertThat(actual.bytes().utf8ToString(), is(expected.bytes().utf8ToString()));
    }

    static void assertXContentBuilderAsBytes(final XContentBuilder expected, final XContentBuilder actual) {
        try {
            XContent xContent = XContentFactory.xContent(actual.contentType());
            XContentParser jsonParser = xContent.createParser(NamedXContentRegistry.EMPTY, expected.bytes());
            XContentParser testParser = xContent.createParser(NamedXContentRegistry.EMPTY, actual.bytes());

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
