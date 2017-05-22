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

package org.elasticsearch.test.hamcrest;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertVersionSerializable;

public class ElasticsearchAssertionsTests extends ESTestCase {
    public void testAssertVersionSerializableIsOkWithIllegalArgumentException() {
        Version version = randomVersion(random());
        NamedWriteableRegistry registry = new NamedWriteableRegistry(emptyList());
        Streamable testStreamable = new TestStreamable();

        // Should catch the exception and do nothing.
        assertVersionSerializable(version, testStreamable, registry);
    }

    public static class TestStreamable implements Streamable {
        @Override
        public void readFrom(StreamInput in) throws IOException {
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new IllegalArgumentException("Not supported.");
        }
    }

    public void testAssertXContentEquivalent() throws IOException {
        XContentBuilder original = JsonXContent.contentBuilder();
        original.startObject();
        addRandomNumberOfFields(original);
        {
            original.startObject(randomAlphaOfLength(10));
            addRandomNumberOfFields(original);
            original.endObject();
        }
        {
            original.startArray(randomAlphaOfLength(10));
            int elements = randomInt(5);
            for (int i = 0; i < elements; i++) {
                original.value(randomAlphaOfLength(10));
            }
            original.endArray();
        }
        original.endObject();

        XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, original.bytes(), original.contentType());
        XContentBuilder copy = JsonXContent.contentBuilder();
        parser.nextToken();
        XContentHelper.copyCurrentStructure(copy.generator(), parser);
        assertToXContentEquivalent(original.bytes(), copy.bytes(), original.contentType());
    }

    public void testAssertXContentEquivalentErrors() throws IOException {
        {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            {
                builder.startObject("foo");
                {
                    builder.field("field1", "value1");
                    builder.field("field2", "value2");
                }
                builder.endObject();
            }
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.contentBuilder();
            otherBuilder.startObject();
            {
                otherBuilder.startObject("foo");
                {
                    otherBuilder.field("field1", "value1");
                }
                otherBuilder.endObject();
            }
            otherBuilder.endObject();
            AssertionError error = expectThrows(AssertionError.class,
                    () -> assertToXContentEquivalent(builder.bytes(), otherBuilder.bytes(), builder.contentType()));
            assertEquals("different field names at [foo]: [field1, field2] vs. [field1] expected:<2> but was:<1>",
                    error.getMessage());
        }
        {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            {
                builder.startObject("foo");
                {
                    builder.field("field1", "value1");
                    builder.field("field2", "value2");
                }
                builder.endObject();
            }
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.contentBuilder();
            otherBuilder.startObject();
            {
                otherBuilder.startObject("foo");
                {
                    otherBuilder.field("field1", "value1");
                    otherBuilder.field("field2", "differentValue2");
                }
                otherBuilder.endObject();
            }
            otherBuilder.endObject();
            AssertionError error = expectThrows(AssertionError.class,
                    () -> assertToXContentEquivalent(builder.bytes(), otherBuilder.bytes(), builder.contentType()));
            assertEquals("different xContent at [foo/field2] expected:<[v]alue2> but was:<[differentV]alue2>", error.getMessage());
        }
        {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            {
                builder.startArray("foo");
                {
                    builder.value("one");
                    builder.value("two");
                    builder.value("three");
                }
                builder.endArray();
            }
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.contentBuilder();
            otherBuilder.startObject();
            {
                otherBuilder.startArray("foo");
                {
                    otherBuilder.value("one");
                    otherBuilder.value("two");
                    otherBuilder.value("four");
                }
                otherBuilder.endArray();
            }
            otherBuilder.endObject();
            AssertionError error = expectThrows(AssertionError.class,
                    () -> assertToXContentEquivalent(builder.bytes(), otherBuilder.bytes(), builder.contentType()));
            assertEquals("different xContent at [foo/2] expected:<[three]> but was:<[four]>", error.getMessage());
        }
        {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            {
                builder.startArray("foo");
                {
                    builder.value("one");
                    builder.value("two");
                    builder.value("three");
                }
                builder.endArray();
            }
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.contentBuilder();
            otherBuilder.startObject();
            {
                otherBuilder.startArray("foo");
                {
                    otherBuilder.value("one");
                    otherBuilder.value("two");
                }
                otherBuilder.endArray();
            }
            otherBuilder.endObject();
            AssertionError error = expectThrows(AssertionError.class,
                    () -> assertToXContentEquivalent(builder.bytes(), otherBuilder.bytes(), builder.contentType()));
            assertEquals("different list values at [foo]: [one, two, three] vs. [one, two] expected:<3> but was:<2>", error.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static void addRandomNumberOfFields(XContentBuilder builder) throws IOException {
        int fields = randomInt(5);
        for (int i = 0; i < fields; i++) {
            Supplier<Object> sup = randomFrom(new Supplier[] {
                () -> randomAlphaOfLength(10),
                () -> randomInt(),
                () -> randomBoolean()
            });
            builder.field(randomAlphaOfLength(10), sup.get());
        }
    }
}
