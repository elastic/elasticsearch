/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class StoredScriptTests extends AbstractXContentSerializingTestCase<StoredScriptSource> {

    public void testBasicAddDelete() {
        StoredScriptSource source = new StoredScriptSource("lang", "code", emptyMap());
        ScriptMetadata smd = ScriptMetadata.putStoredScript(null, "test", source);
        assertThat(smd.getStoredScript("test"), equalTo(source));

        smd = ScriptMetadata.deleteStoredScript(smd, "test");
        assertThat(smd.getStoredScript("test"), nullValue());
    }

    public void testInvalidDelete() {
        ResourceNotFoundException rnfe = expectThrows(
            ResourceNotFoundException.class,
            () -> ScriptMetadata.deleteStoredScript(null, "test")
        );
        assertThat(rnfe.getMessage(), equalTo("stored script [test] does not exist and cannot be deleted"));
    }

    public void testSourceParsing() throws Exception {
        // simple script value string
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().startObject("script").field("lang", "lang").field("source", "code").endObject().endObject();

            StoredScriptSource parsed = StoredScriptSource.parse(BytesReference.bytes(builder), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource("lang", "code", Collections.emptyMap());

            assertThat(parsed, equalTo(source));
        }

        // complex template using script as the field name
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .startObject("script")
                .field("lang", "mustache")
                .startObject("source")
                .field("query", "code")
                .endObject()
                .endObject()
                .endObject();
            String code;

            try (XContentBuilder cb = XContentFactory.contentBuilder(builder.contentType())) {
                code = Strings.toString(cb.startObject().field("query", "code").endObject());
            }

            StoredScriptSource parsed = StoredScriptSource.parse(BytesReference.bytes(builder), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource(
                "mustache",
                code,
                Collections.singletonMap("content_type", "application/json;charset=utf-8")
            );

            assertThat(parsed, equalTo(source));
        }

        // complex script with script object
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("lang", "lang").field("source", "code").endObject().endObject();

            StoredScriptSource parsed = StoredScriptSource.parse(BytesReference.bytes(builder), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource("lang", "code", Collections.emptyMap());

            assertThat(parsed, equalTo(source));
        }

        // complex script with script object and empty options
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("script")
                .startObject()
                .field("lang", "lang")
                .field("source", "code")
                .field("options")
                .startObject()
                .endObject()
                .endObject()
                .endObject();

            StoredScriptSource parsed = StoredScriptSource.parse(BytesReference.bytes(builder), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource("lang", "code", Collections.emptyMap());

            assertThat(parsed, equalTo(source));
        }

        // complex script with embedded template
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            Strings.toString(
                builder.startObject()
                    .field("script")
                    .startObject()
                    .field("lang", "lang")
                    .startObject("source")
                    .field("query", "code")
                    .endObject()
                    .startObject("options")
                    .endObject()
                    .endObject()
                    .endObject()
            );
            String code;

            try (XContentBuilder cb = XContentFactory.contentBuilder(builder.contentType())) {
                code = Strings.toString(cb.startObject().field("query", "code").endObject());
            }

            StoredScriptSource parsed = StoredScriptSource.parse(BytesReference.bytes(builder), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource(
                "lang",
                code,
                Collections.singletonMap(Script.CONTENT_TYPE_OPTION, builder.contentType().mediaType())
            );

            assertThat(parsed, equalTo(source));
        }
    }

    public void testSourceParsingErrors() throws Exception {
        // check for missing lang parameter when parsing a script
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("source", "code").endObject().endObject();

            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> StoredScriptSource.parse(BytesReference.bytes(builder), XContentType.JSON)
            );
            assertThat(iae.getMessage(), equalTo("must specify lang for stored script"));
        }

        // check for missing source parameter when parsing a script
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("lang", "lang").endObject().endObject();

            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> StoredScriptSource.parse(BytesReference.bytes(builder), XContentType.JSON)
            );
            assertThat(iae.getMessage(), equalTo("must specify source for stored script"));
        }

        // check for illegal options parameter when parsing a script
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("script")
                .startObject()
                .field("lang", "lang")
                .field("source", "code")
                .startObject("options")
                .field("option", "option")
                .endObject()
                .endObject()
                .endObject();

            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> StoredScriptSource.parse(BytesReference.bytes(builder), XContentType.JSON)
            );
            assertThat(iae.getMessage(), equalTo("illegal compiler options [{option=option}] specified"));
        }

        // check for unsupported template context
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("template", "code").endObject();
            ParsingException pEx = expectThrows(
                ParsingException.class,
                () -> StoredScriptSource.parse(BytesReference.bytes(builder), XContentType.JSON)
            );
            assertThat(
                pEx.getMessage(),
                equalTo("unexpected field [template], expected [" + StoredScriptSource.SCRIPT_PARSE_FIELD.getPreferredName() + "]")
            );
        }
    }

    public void testEmptyTemplateDeprecations() throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("lang", "mustache").field("source", "").endObject().endObject();

            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> StoredScriptSource.parse(BytesReference.bytes(builder), XContentType.JSON)
            );
            assertEquals("source cannot be empty", iae.getMessage());
        }
    }

    @Override
    protected StoredScriptSource createTestInstance() {
        return new StoredScriptSource(
            randomAlphaOfLength(randomIntBetween(4, 32)),
            randomAlphaOfLength(randomIntBetween(4, 16383)),
            Collections.emptyMap()
        );
    }

    @Override
    protected Writeable.Reader<StoredScriptSource> instanceReader() {
        return StoredScriptSource::new;
    }

    @Override
    protected StoredScriptSource doParseInstance(XContentParser parser) {
        return StoredScriptSource.fromXContent(parser, false);
    }

    @Override
    protected StoredScriptSource mutateInstance(StoredScriptSource instance) {
        String source = instance.getSource();
        String lang = instance.getLang();
        Map<String, String> options = instance.getOptions();

        switch (between(0, 2)) {
            case 0 -> source = randomAlphaOfLength(randomIntBetween(4, 16383));
            case 1 -> lang = randomAlphaOfLengthBetween(1, 20);
            case 2 -> {
                options = new HashMap<>(options);
                options.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new StoredScriptSource(lang, source, options);
    }
}
