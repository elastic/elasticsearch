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

package org.elasticsearch.cluster.serialization;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.DiffableUtils.KeyedReader;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.StreamableReader;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class DiffableTests extends ESTestCase {

    @Test
    public void testImmutableMapDiff() throws IOException {
        ImmutableMap.Builder<String, TestDiffable> builder = ImmutableMap.builder();
        builder.put("foo", new TestDiffable("1"));
        builder.put("bar", new TestDiffable("2"));
        builder.put("baz", new TestDiffable("3"));
        ImmutableMap<String, TestDiffable> before = builder.build();
        Map<String, TestDiffable> map = new HashMap<>();
        map.putAll(before);
        map.remove("bar");
        map.put("baz", new TestDiffable("4"));
        map.put("new", new TestDiffable("5"));
        ImmutableMap<String, TestDiffable> after = ImmutableMap.copyOf(map);
        Diff diff = DiffableUtils.diff(before, after);
        BytesStreamOutput out = new BytesStreamOutput();
        diff.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes());
        ImmutableMap<String, TestDiffable> serialized = DiffableUtils.readImmutableMapDiff(in, TestDiffable.PROTO).apply(before);
        assertThat(serialized.size(), equalTo(3));
        assertThat(serialized.get("foo").value(), equalTo("1"));
        assertThat(serialized.get("baz").value(), equalTo("4"));
        assertThat(serialized.get("new").value(), equalTo("5"));
    }

    @Test
    public void testImmutableOpenMapDiff() throws IOException {
        ImmutableOpenMap.Builder<String, TestDiffable> builder = ImmutableOpenMap.builder();
        builder.put("foo", new TestDiffable("1"));
        builder.put("bar", new TestDiffable("2"));
        builder.put("baz", new TestDiffable("3"));
        ImmutableOpenMap<String, TestDiffable> before = builder.build();
        builder = ImmutableOpenMap.builder(before);
        builder.remove("bar");
        builder.put("baz", new TestDiffable("4"));
        builder.put("new", new TestDiffable("5"));
        ImmutableOpenMap<String, TestDiffable> after = builder.build();
        Diff diff = DiffableUtils.diff(before, after);
        BytesStreamOutput out = new BytesStreamOutput();
        diff.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes());
        ImmutableOpenMap<String, TestDiffable> serialized = DiffableUtils.readImmutableOpenMapDiff(in, new KeyedReader<TestDiffable>() {
            @Override
            public TestDiffable readFrom(StreamInput in, String key) throws IOException {
                return new TestDiffable(in.readString());
            }

            @Override
            public Diff<TestDiffable> readDiffFrom(StreamInput in, String key) throws IOException {
                return AbstractDiffable.readDiffFrom(new StreamableReader<TestDiffable>() {
                    @Override
                    public TestDiffable readFrom(StreamInput in) throws IOException {
                        return new TestDiffable(in.readString());
                    }
                }, in);
            }
        }).apply(before);
        assertThat(serialized.size(), equalTo(3));
        assertThat(serialized.get("foo").value(), equalTo("1"));
        assertThat(serialized.get("baz").value(), equalTo("4"));
        assertThat(serialized.get("new").value(), equalTo("5"));

    }
    public static class TestDiffable extends AbstractDiffable<TestDiffable> {

        public static final TestDiffable PROTO = new TestDiffable("");

        private final String value;

        public TestDiffable(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        @Override
        public TestDiffable readFrom(StreamInput in) throws IOException {
            return new TestDiffable(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }
    }

}
