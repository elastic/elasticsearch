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

package org.elasticsearch.script;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class StoredScriptTests extends AbstractSerializingTestCase<StoredScriptSource> {

    public void testBasicAddDelete() {
        StoredScriptSource source = new StoredScriptSource("lang", "code", emptyMap());
        ScriptMetaData smd = ScriptMetaData.putStoredScript(null, "test", source);

        assertThat(smd.getStoredScript("test", null), equalTo(source));
        assertThat(smd.getStoredScript("test", "lang"), equalTo(source));

        smd = ScriptMetaData.deleteStoredScript(smd, "test", null);

        assertThat(smd.getStoredScript("test", null), nullValue());
        assertThat(smd.getStoredScript("test", "lang"), nullValue());
    }

    public void testDifferentMultiAddDelete() {
        StoredScriptSource source0 = new StoredScriptSource("lang0", "code0", emptyMap());
        StoredScriptSource source1 = new StoredScriptSource("lang0", "code1", emptyMap());
        StoredScriptSource source2 = new StoredScriptSource("lang1", "code2",
            singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType()));

        ScriptMetaData smd = ScriptMetaData.putStoredScript(null, "test0", source0);
        smd = ScriptMetaData.putStoredScript(smd, "test1", source1);
        smd = ScriptMetaData.putStoredScript(smd, "test2", source2);

        assertThat(smd.getStoredScript("test0", null), equalTo(source0));
        assertThat(smd.getStoredScript("test0", "lang0"), equalTo(source0));
        assertThat(smd.getStoredScript("test1", null), equalTo(source1));
        assertThat(smd.getStoredScript("test1", "lang0"), equalTo(source1));
        assertThat(smd.getStoredScript("test2", null), equalTo(source2));
        assertThat(smd.getStoredScript("test2", "lang1"), equalTo(source2));

        assertThat(smd.getStoredScript("test3", null), nullValue());
        assertThat(smd.getStoredScript("test0", "lang1"), nullValue());
        assertThat(smd.getStoredScript("test2", "lang0"), nullValue());

        smd = ScriptMetaData.deleteStoredScript(smd, "test0", null);

        assertThat(smd.getStoredScript("test0", null), nullValue());
        assertThat(smd.getStoredScript("test0", "lang0"), nullValue());
        assertThat(smd.getStoredScript("test1", null), equalTo(source1));
        assertThat(smd.getStoredScript("test1", "lang0"), equalTo(source1));
        assertThat(smd.getStoredScript("test2", null), equalTo(source2));
        assertThat(smd.getStoredScript("test2", "lang1"), equalTo(source2));

        assertThat(smd.getStoredScript("test3", null), nullValue());
        assertThat(smd.getStoredScript("test0", "lang1"), nullValue());
        assertThat(smd.getStoredScript("test2", "lang0"), nullValue());

        smd = ScriptMetaData.deleteStoredScript(smd, "test2", "lang1");

        assertThat(smd.getStoredScript("test0", null), nullValue());
        assertThat(smd.getStoredScript("test0", "lang0"), nullValue());
        assertThat(smd.getStoredScript("test1", null), equalTo(source1));
        assertThat(smd.getStoredScript("test1", "lang0"), equalTo(source1));
        assertThat(smd.getStoredScript("test2", null), nullValue());
        assertThat(smd.getStoredScript("test2", "lang1"), nullValue());

        assertThat(smd.getStoredScript("test3", null), nullValue());
        assertThat(smd.getStoredScript("test0", "lang1"), nullValue());
        assertThat(smd.getStoredScript("test2", "lang0"), nullValue());

        smd = ScriptMetaData.deleteStoredScript(smd, "test1", "lang0");

        assertThat(smd.getStoredScript("test0", null), nullValue());
        assertThat(smd.getStoredScript("test0", "lang0"), nullValue());
        assertThat(smd.getStoredScript("test1", null), nullValue());
        assertThat(smd.getStoredScript("test1", "lang0"), nullValue());
        assertThat(smd.getStoredScript("test2", null), nullValue());
        assertThat(smd.getStoredScript("test2", "lang1"), nullValue());

        assertThat(smd.getStoredScript("test3", null), nullValue());
        assertThat(smd.getStoredScript("test0", "lang1"), nullValue());
        assertThat(smd.getStoredScript("test2", "lang0"), nullValue());
    }

    public void testSameMultiAddDelete() {
        StoredScriptSource source0 = new StoredScriptSource("lang0", "code0", emptyMap());
        StoredScriptSource source1 = new StoredScriptSource("lang1", "code1", emptyMap());
        StoredScriptSource source2 = new StoredScriptSource("lang2", "code1", emptyMap());
        StoredScriptSource source3 = new StoredScriptSource("lang1", "code2",
            singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType()));

        ScriptMetaData smd = ScriptMetaData.putStoredScript(null, "test0", source0);
        smd = ScriptMetaData.putStoredScript(smd, "test0", source1);
        assertWarnings("stored script [test0] already exists using a different lang [lang0]," +
            " the new namespace for stored scripts will only use (id) instead of (lang, id)");
        smd = ScriptMetaData.putStoredScript(smd, "test3", source3);
        smd = ScriptMetaData.putStoredScript(smd, "test0", source2);
        assertWarnings("stored script [test0] already exists using a different lang [lang1]," +
            " the new namespace for stored scripts will only use (id) instead of (lang, id)");

        assertThat(smd.getStoredScript("test0", null), equalTo(source2));
        assertThat(smd.getStoredScript("test0", "lang0"), equalTo(source0));
        assertThat(smd.getStoredScript("test0", "lang1"), equalTo(source1));
        assertThat(smd.getStoredScript("test0", "lang2"), equalTo(source2));
        assertThat(smd.getStoredScript("test3", null), equalTo(source3));
        assertThat(smd.getStoredScript("test3", "lang1"), equalTo(source3));

        smd = ScriptMetaData.deleteStoredScript(smd, "test0", "lang1");

        assertThat(smd.getStoredScript("test0", null), equalTo(source2));
        assertThat(smd.getStoredScript("test0", "lang0"), equalTo(source0));
        assertThat(smd.getStoredScript("test0", "lang1"), nullValue());
        assertThat(smd.getStoredScript("test0", "lang2"), equalTo(source2));
        assertThat(smd.getStoredScript("test3", null), equalTo(source3));
        assertThat(smd.getStoredScript("test3", "lang1"), equalTo(source3));

        smd = ScriptMetaData.deleteStoredScript(smd, "test0", null);

        assertThat(smd.getStoredScript("test0", null), nullValue());
        assertThat(smd.getStoredScript("test0", "lang0"), equalTo(source0));
        assertThat(smd.getStoredScript("test0", "lang1"), nullValue());
        assertThat(smd.getStoredScript("test0", "lang2"), nullValue());
        assertThat(smd.getStoredScript("test3", null), equalTo(source3));
        assertThat(smd.getStoredScript("test3", "lang1"), equalTo(source3));

        smd = ScriptMetaData.deleteStoredScript(smd, "test3", "lang1");

        assertThat(smd.getStoredScript("test0", null), nullValue());
        assertThat(smd.getStoredScript("test0", "lang0"), equalTo(source0));
        assertThat(smd.getStoredScript("test0", "lang1"), nullValue());
        assertThat(smd.getStoredScript("test0", "lang2"), nullValue());
        assertThat(smd.getStoredScript("test3", null), nullValue());
        assertThat(smd.getStoredScript("test3", "lang1"), nullValue());

        smd = ScriptMetaData.deleteStoredScript(smd, "test0", "lang0");

        assertThat(smd.getStoredScript("test0", null), nullValue());
        assertThat(smd.getStoredScript("test0", "lang0"), nullValue());
        assertThat(smd.getStoredScript("test0", "lang1"), nullValue());
        assertThat(smd.getStoredScript("test0", "lang2"), nullValue());
        assertThat(smd.getStoredScript("test3", null), nullValue());
        assertThat(smd.getStoredScript("test3", "lang1"), nullValue());
    }

    public void testInvalidDelete() {
        ResourceNotFoundException rnfe =
            expectThrows(ResourceNotFoundException.class, () -> ScriptMetaData.deleteStoredScript(null, "test", "lang"));
        assertThat(rnfe.getMessage(), equalTo("stored script [test] using lang [lang] does not exist and cannot be deleted"));

        rnfe = expectThrows(ResourceNotFoundException.class, () -> ScriptMetaData.deleteStoredScript(null, "test", null));
        assertThat(rnfe.getMessage(), equalTo("stored script [test] does not exist and cannot be deleted"));
    }

    public void testSourceParsing() throws Exception {
        // simple script value string
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script", "code").endObject();

            StoredScriptSource parsed = StoredScriptSource.parse("lang", builder.bytes(), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource("lang", "code", Collections.emptyMap());

            assertThat(parsed, equalTo(source));
        }

        // simple template value string
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("template", "code").endObject();

            StoredScriptSource parsed = StoredScriptSource.parse("lang", builder.bytes(), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource("lang", "code",
                Collections.singletonMap(Script.CONTENT_TYPE_OPTION, builder.contentType().mediaType()));

            assertThat(parsed, equalTo(source));
        }

        // complex template with wrapper template object
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("template").startObject().field("query", "code").endObject().endObject();
            String code;

            try (XContentBuilder cb = XContentFactory.contentBuilder(builder.contentType())) {
                code = cb.startObject().field("query", "code").endObject().string();
            }

            StoredScriptSource parsed = StoredScriptSource.parse("lang", builder.bytes(), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource("lang", code,
                Collections.singletonMap(Script.CONTENT_TYPE_OPTION, builder.contentType().mediaType()));

            assertThat(parsed, equalTo(source));
        }

        // complex template with no wrapper object
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("query", "code").endObject();
            String code;

            try (XContentBuilder cb = XContentFactory.contentBuilder(builder.contentType())) {
                code = cb.startObject().field("query", "code").endObject().string();
            }

            StoredScriptSource parsed = StoredScriptSource.parse("lang", builder.bytes(), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource("lang", code,
                Collections.singletonMap(Script.CONTENT_TYPE_OPTION, builder.contentType().mediaType()));

            assertThat(parsed, equalTo(source));
        }

        // complex template using script as the field name
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("query", "code").endObject().endObject();
            String code;

            try (XContentBuilder cb = XContentFactory.contentBuilder(builder.contentType())) {
                code = cb.startObject().field("query", "code").endObject().string();
            }

            StoredScriptSource parsed = StoredScriptSource.parse("lang", builder.bytes(), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource("lang", code,
                Collections.singletonMap(Script.CONTENT_TYPE_OPTION, builder.contentType().mediaType()));

            assertThat(parsed, equalTo(source));
        }

        // complex script with script object
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("lang", "lang").field("code", "code").endObject().endObject();

            StoredScriptSource parsed = StoredScriptSource.parse(null, builder.bytes(), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource("lang", "code", Collections.emptyMap());

            assertThat(parsed, equalTo(source));
        }

        // complex script with script object and empty options
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("lang", "lang").field("code", "code")
                .field("options").startObject().endObject().endObject().endObject();

            StoredScriptSource parsed = StoredScriptSource.parse(null, builder.bytes(), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource("lang", "code", Collections.emptyMap());

            assertThat(parsed, equalTo(source));
        }

        // complex script with embedded template
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("lang", "lang").startObject("code").field("query", "code")
                .endObject().startObject("options").endObject().endObject().endObject().string();
            String code;

            try (XContentBuilder cb = XContentFactory.contentBuilder(builder.contentType())) {
                code = cb.startObject().field("query", "code").endObject().string();
            }

            StoredScriptSource parsed = StoredScriptSource.parse(null, builder.bytes(), XContentType.JSON);
            StoredScriptSource source = new StoredScriptSource("lang", code,
                Collections.singletonMap(Script.CONTENT_TYPE_OPTION, builder.contentType().mediaType()));

            assertThat(parsed, equalTo(source));
        }
    }

    public void testSourceParsingErrors() throws Exception {
        // check for missing lang parameter when parsing a template
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("template", "code").endObject();

            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
                StoredScriptSource.parse(null, builder.bytes(), XContentType.JSON));
            assertThat(iae.getMessage(), equalTo("unexpected stored script format"));
        }

        // check for missing lang parameter when parsing a script
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("code", "code").endObject().endObject();

            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
                StoredScriptSource.parse(null, builder.bytes(), XContentType.JSON));
            assertThat(iae.getMessage(), equalTo("must specify lang for stored script"));
        }

        // check for missing code parameter when parsing a script
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("lang", "lang").endObject().endObject();

            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
                StoredScriptSource.parse(null, builder.bytes(), XContentType.JSON));
            assertThat(iae.getMessage(), equalTo("must specify code for stored script"));
        }

        // check for illegal options parameter when parsing a script
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("lang", "lang").field("code", "code")
                .startObject("options").field("option", "option").endObject().endObject().endObject();

            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
                StoredScriptSource.parse(null, builder.bytes(), XContentType.JSON));
            assertThat(iae.getMessage(), equalTo("illegal compiler options [{option=option}] specified"));
        }

        // check for illegal use of content type option
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject().field("script").startObject().field("lang", "lang").field("code", "code")
                .startObject("options").field("content_type", "option").endObject().endObject().endObject();

            ParsingException pe = expectThrows(ParsingException.class, () ->
                StoredScriptSource.parse(null, builder.bytes(), XContentType.JSON));
            assertThat(pe.getRootCause().getMessage(), equalTo("content_type cannot be user-specified"));
        }
    }

    @Override
    protected StoredScriptSource createTestInstance() {
        return new StoredScriptSource(
            randomAsciiOfLength(randomIntBetween(4, 32)),
            randomAsciiOfLength(randomIntBetween(4, 16383)),
            Collections.emptyMap());
    }

    @Override
    protected Writeable.Reader<StoredScriptSource> instanceReader() {
        return StoredScriptSource::new;
    }

    @Override
    protected StoredScriptSource doParseInstance(XContentParser parser) {
        try {
            return StoredScriptSource.fromXContent(parser);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }
}
