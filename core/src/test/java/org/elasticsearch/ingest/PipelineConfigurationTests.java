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

package org.elasticsearch.ingest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class PipelineConfigurationTests extends ESTestCase {

    public void testSerialization() throws IOException {
        PipelineConfiguration configuration = new PipelineConfiguration("1",
            new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
        assertEquals(XContentType.JSON, configuration.getXContentType());

        BytesStreamOutput out = new BytesStreamOutput();
        configuration.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        PipelineConfiguration serialized = PipelineConfiguration.readFrom(in);
        assertEquals(XContentType.JSON, serialized.getXContentType());
        assertEquals("{}", serialized.getConfig().utf8ToString());
    }

    public void testParser() throws IOException {
        ContextParser<Void, PipelineConfiguration> parser = PipelineConfiguration.getParser();
        XContentType xContentType = randomFrom(XContentType.values());
        final BytesReference bytes;
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            new PipelineConfiguration("1", new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), XContentType.JSON)
                .toXContent(builder, ToXContent.EMPTY_PARAMS);
            bytes = builder.bytes();
        }

        XContentParser xContentParser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY, bytes);
        PipelineConfiguration parsed = parser.parse(xContentParser, null);
        assertEquals(xContentType, parsed.getXContentType());
        assertEquals("{}", XContentHelper.convertToJson(parsed.getConfig(), false, parsed.getXContentType()));
        assertEquals("1", parsed.getId());
    }
}
