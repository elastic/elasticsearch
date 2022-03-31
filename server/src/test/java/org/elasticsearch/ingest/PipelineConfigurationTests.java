/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;

public class PipelineConfigurationTests extends AbstractXContentTestCase<PipelineConfiguration> {

    public void testSerialization() throws IOException {
        PipelineConfiguration configuration = new PipelineConfiguration(
            "1",
            new BytesArray("{}".getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        );
        assertEquals(XContentType.JSON, configuration.getXContentType());

        BytesStreamOutput out = new BytesStreamOutput();
        configuration.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        PipelineConfiguration serialized = PipelineConfiguration.readFrom(in);
        assertEquals(XContentType.JSON, serialized.getXContentType());
        assertEquals("{}", serialized.getConfig().utf8ToString());
    }

    public void testMetaSerialization() throws IOException {
        String configJson = """
            {"description": "blah", "_meta" : {"foo": "bar"}}""";
        PipelineConfiguration configuration = new PipelineConfiguration(
            "1",
            new BytesArray(configJson.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        );
        assertEquals(XContentType.JSON, configuration.getXContentType());
        BytesStreamOutput out = new BytesStreamOutput();
        configuration.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        PipelineConfiguration serialized = PipelineConfiguration.readFrom(in);
        assertEquals(XContentType.JSON, serialized.getXContentType());
        assertEquals(configJson, serialized.getConfig().utf8ToString());
    }

    public void testParser() throws IOException {
        ContextParser<Void, PipelineConfiguration> parser = PipelineConfiguration.getParser();
        XContentType xContentType = randomFrom(XContentType.values());
        final BytesReference bytes;
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            new PipelineConfiguration("1", new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), XContentType.JSON).toXContent(
                builder,
                ToXContent.EMPTY_PARAMS
            );
            bytes = BytesReference.bytes(builder);
        }

        XContentParser xContentParser = xContentType.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput());
        PipelineConfiguration parsed = parser.parse(xContentParser, null);
        assertEquals(xContentType.canonical(), parsed.getXContentType());
        assertEquals("{}", XContentHelper.convertToJson(parsed.getConfig(), false, parsed.getXContentType()));
        assertEquals("1", parsed.getId());
    }

    public void testGetVersion() {
        {
            // missing version
            String configJson = """
                {"description": "blah", "_meta" : {"foo": "bar"}}""";
            PipelineConfiguration configuration = new PipelineConfiguration(
                "1",
                new BytesArray(configJson.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON
            );
            assertNull(configuration.getVersion());
        }
        {
            // null version
            int version = randomInt();
            String configJson = String.format(Locale.ROOT, """
                {"version": %d, "description": "blah", "_meta" : {"foo": "bar"}}
                """, version);
            PipelineConfiguration configuration = new PipelineConfiguration(
                "1",
                new BytesArray(configJson.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON
            );
            assertThat(configuration.getVersion(), equalTo(version));
        }
    }

    @Override
    protected PipelineConfiguration createTestInstance() {
        BytesArray config;
        if (randomBoolean()) {
            config = new BytesArray("{}".getBytes(StandardCharsets.UTF_8));
        } else {
            config = new BytesArray("{\"foo\": \"bar\"}".getBytes(StandardCharsets.UTF_8));
        }
        return new PipelineConfiguration(randomAlphaOfLength(4), config, XContentType.JSON);
    }

    @Override
    protected PipelineConfiguration doParseInstance(XContentParser parser) throws IOException {
        return PipelineConfiguration.getParser().parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.equals("config");
    }
}
