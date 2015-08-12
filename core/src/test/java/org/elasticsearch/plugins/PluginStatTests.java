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

package org.elasticsearch.plugins;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;

public class PluginStatTests extends ESTestCase {
/*
    @Test
    public void testStringSerialization() throws IOException {
        PluginStat serialized = PluginStat.stringStat(randomAsciiOfLength(10), randomAsciiOfLengthBetween(10, 30));
        assertNotNull(serialized);

        PluginStat deserialized = deserialize(serialize(serialized));
        assertNotNull(deserialized);

        assertThat(deserialized.getType(), equalTo(serialized.getType()));
        assertThat(deserialized.getName(), equalTo(serialized.getName()));
        assertThat(deserialized.getValue(), equalTo(serialized.getValue()));
    }

    @Test
    public void testStringToXContent() throws IOException {
        PluginStat stat = PluginStat.stringStat("hello", "world");

        BytesReference result = build(XContentType.JSON, stat);
        assertThat(result.toUtf8(), is("{\"hello\":\"world\"}"));
    }

    @Test
    public void testLongSerialization() throws IOException {
        PluginStat serialized = PluginStat.longStat(randomAsciiOfLength(10), randomLong());
        assertNotNull(serialized);

        PluginStat deserialized = deserialize(serialize(serialized));
        assertNotNull(deserialized);

        assertThat(deserialized.getType(), equalTo(serialized.getType()));
        assertThat(deserialized.getName(), equalTo(serialized.getName()));
        assertThat(deserialized.getValue(), equalTo(serialized.getValue()));
    }

    @Test
    public void testLongToXContent() throws IOException {
        PluginStat stat = PluginStat.longStat("total_count", 123456L);

        BytesReference result = build(XContentType.JSON, stat);
        assertThat(result.toUtf8(), is("{\"total_count\":123456}"));
    }

    @Test
    public void testObjectSerialization() throws IOException {
        PluginStat inner;
        if (randomBoolean()) {
            inner = PluginStat.longStat(randomAsciiOfLength(10), randomLong());
        } else {
            inner = PluginStat.stringStat(randomAsciiOfLength(10), randomAsciiOfLengthBetween(10, 30));
        }

        PluginStat serialized = PluginStat.innerStat(randomAsciiOfLength(10), inner);
        assertNotNull(serialized);

        PluginStat deserialized = deserialize(serialize(serialized));
        assertNotNull(deserialized);

        assertThat(deserialized.getType(), equalTo(serialized.getType()));
        assertThat(deserialized.getName(), equalTo(serialized.getName()));
        assertThat(deserialized.getValue(), equalTo(serialized.getValue()));
    }

    @Test
    public void testObjectToXContent() throws IOException {
        PluginStat stat = PluginStat.innerStat("my_plugin",
                                PluginStat.innerStat("remote",
                                        PluginStat.stringStat("origin", "git@github.com:my/plugin.git")
                                )
        );

        BytesReference result = build(XContentType.JSON, stat);
        assertThat(result.toUtf8(), is("{\"my_plugin\":{\"remote\":{\"origin\":\"git@github.com:my/plugin.git\"}}}"));
    }

    private BytesReference serialize(PluginStat stat) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            PluginStat.writePluginStat(output, stat);
            return output.bytes();
        }
    }

    private PluginStat deserialize(BytesReference in) throws IOException {
        try (StreamInput input = StreamInput.wrap(in)) {
            return PluginStat.readPluginStat(input);
        }
    }

    private BytesReference build(XContentType xContentType, PluginStat stat) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (XContentBuilder builder = new XContentBuilder(xContentType.xContent(), output)) {
                builder.startObject();
                stat.toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
            }
            return output.bytes();
        }
    }
    */
}

