/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class PipelineConfigurationTests extends AbstractXContentTestCase<PipelineConfiguration> {

    public void testConfigInvariants() {
        Map<String, Object> original = Map.of("a", 1);
        Map<String, Object> mutable = new HashMap<>(original);
        PipelineConfiguration configuration = new PipelineConfiguration("1", mutable);
        // the config is equal to the original & mutable map, regardless of how you get a reference to it
        assertThat(configuration.getConfig(), equalTo(original));
        assertThat(configuration.getConfig(), equalTo(mutable));
        assertThat(configuration.getConfig(), equalTo(configuration.getConfig(false)));
        assertThat(configuration.getConfig(), equalTo(configuration.getConfig(true)));
        // the config is the same instance as itself when unmodifiable is true
        assertThat(configuration.getConfig(), sameInstance(configuration.getConfig()));
        assertThat(configuration.getConfig(), sameInstance(configuration.getConfig(true)));
        // but it's not the same instance as the original mutable map, nor if unmodifiable is false
        assertThat(configuration.getConfig(), not(sameInstance(mutable)));
        assertThat(configuration.getConfig(), not(sameInstance(configuration.getConfig(false))));

        // changing the mutable map doesn't alter the pipeline's configuration
        mutable.put("b", 2);
        assertThat(configuration.getConfig(), equalTo(original));

        // the modifiable map can be modified
        Map<String, Object> modifiable = configuration.getConfig(false);
        modifiable.put("c", 3); // this doesn't throw an exception
        assertThat(modifiable.get("c"), equalTo(3));
        // but the next modifiable copy is a new fresh copy, and doesn't reflect those changes
        assertThat(configuration.getConfig(), equalTo(configuration.getConfig(false)));
    }

    public void testSerialization() throws IOException {
        PipelineConfiguration configuration = new PipelineConfiguration(
            "1",
            new BytesArray("{}".getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        );
        assertThat(configuration.getConfig(), anEmptyMap());
        BytesStreamOutput out = new BytesStreamOutput();
        configuration.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        PipelineConfiguration serialized = PipelineConfiguration.readFrom(in);
        assertThat(serialized.getConfig(), anEmptyMap());
    }

    public void testMetaSerialization() throws IOException {
        String configJson = """
            {"description": "blah", "_meta" : {"foo": "bar"}}""";
        PipelineConfiguration configuration = new PipelineConfiguration(
            "1",
            new BytesArray(configJson.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        );
        BytesStreamOutput out = new BytesStreamOutput();
        configuration.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        PipelineConfiguration serialized = PipelineConfiguration.readFrom(in);
        assertEquals(
            XContentHelper.convertToMap(new BytesArray(configJson.getBytes(StandardCharsets.UTF_8)), true, XContentType.JSON).v2(),
            serialized.getConfig()
        );
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
        assertThat(parsed.getId(), equalTo("1"));
        assertThat(parsed.getConfig(), anEmptyMap());
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
            String configJson = Strings.format("""
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
