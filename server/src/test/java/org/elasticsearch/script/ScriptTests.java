/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ScriptTests extends ESTestCase {

    public void testScriptParsing() throws IOException {
        Script expectedScript = createScript();
        try (XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()))) {
            expectedScript.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentParser parser = createParser(builder)) {
                Script actualScript = Script.parse(parser);
                assertThat(actualScript, equalTo(expectedScript));
            }
        }
    }

    public void testScriptSerialization() throws IOException {
        Script expectedScript = createScript();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            expectedScript.writeTo(new OutputStreamStreamOutput(out));
            try (ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray())) {
                Script actualScript = new Script(new InputStreamStreamInput(in));
                assertThat(actualScript, equalTo(expectedScript));
            }
        }
    }

    private Script createScript() throws IOException {
        final Map<String, Object> params = randomBoolean() ? Collections.emptyMap() : Collections.singletonMap("key", "value");
        ScriptType scriptType = randomFrom(ScriptType.values());
        String script;
        if (scriptType == ScriptType.INLINE) {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                builder.field("field", randomAlphaOfLengthBetween(1, 5));
                builder.endObject();
                script = Strings.toString(builder);
            }
        } else {
            script = randomAlphaOfLengthBetween(1, 5);
        }
        return new Script(
            scriptType,
            scriptType == ScriptType.STORED ? null : randomFrom("_lang1", "_lang2", "_lang3"),
            script,
            scriptType == ScriptType.INLINE ? Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType()) : null,
            params
        );
    }

    public void testParse() throws IOException {
        Script expectedScript = createScript();
        try (XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()))) {
            expectedScript.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentParser xParser = createParser(builder)) {
                Settings settings = Settings.fromXContent(xParser);
                Script actualScript = Script.parse(settings);
                assertThat(actualScript, equalTo(expectedScript));
            }
        }
    }

    public void testParseFromObjectShortSyntax() {
        Script script = Script.parse("doc['my_field']");
        assertEquals(Script.DEFAULT_SCRIPT_LANG, script.getLang());
        assertEquals("doc['my_field']", script.getIdOrCode());
        assertEquals(0, script.getParams().size());
        assertEquals(0, script.getOptions().size());
        assertEquals(ScriptType.INLINE, script.getType());
    }

    public void testParseFromObject() {
        Map<String, Object> map = new HashMap<>();
        map.put("source", "doc['my_field']");
        Map<String, Object> params = new HashMap<>();
        int numParams = randomIntBetween(0, 3);
        for (int i = 0; i < numParams; i++) {
            params.put("param" + i, i);
        }
        map.put("params", params);
        Map<String, String> options = new HashMap<>();
        int numOptions = randomIntBetween(0, 3);
        for (int i = 0; i < numOptions; i++) {
            options.put("option" + i, Integer.toString(i));
        }
        map.put("options", options);
        String lang = Script.DEFAULT_SCRIPT_LANG;
        ;
        if (randomBoolean()) {
            map.put("lang", lang);
        } else if (randomBoolean()) {
            lang = "expression";
            map.put("lang", lang);
        }

        Script script = Script.parse(map);
        assertEquals(lang, script.getLang());
        assertEquals("doc['my_field']", script.getIdOrCode());
        assertEquals(ScriptType.INLINE, script.getType());
        assertEquals(params, script.getParams());
        assertEquals(options, script.getOptions());
    }

    public void testParseFromObjectFromScript() {
        Map<String, Object> params = new HashMap<>();
        int numParams = randomIntBetween(0, 3);
        for (int i = 0; i < numParams; i++) {
            params.put("param" + i, i);
        }
        Map<String, String> options = new HashMap<>();
        int numOptions = randomIntBetween(0, 3);
        for (int i = 0; i < numOptions; i++) {
            options.put("option" + i, Integer.toString(i));
        }
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "doc['field']", options, params);
        Map<String, Object> scriptObject = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(script), false);
        Script parsedScript = Script.parse(scriptObject);
        assertEquals(script, parsedScript);
    }

    public void testParseFromObjectWrongFormat() {
        {
            NullPointerException exc = expectThrows(NullPointerException.class, () -> Script.parse((Object) null));
            assertEquals("Script must not be null", exc.getMessage());
        }
        {
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> Script.parse(3));
            assertEquals("Script value should be a String or a Map", exc.getMessage());
        }
        {
            ElasticsearchParseException exc = expectThrows(ElasticsearchParseException.class, () -> Script.parse(Collections.emptyMap()));
            assertEquals("Expected one of [source] or [id] fields, but found none", exc.getMessage());
        }
    }

    public void testParseFromObjectUnsupportedFields() {
        ElasticsearchParseException exc = expectThrows(
            ElasticsearchParseException.class,
            () -> Script.parse(Map.of("source", "script", "unsupported", "value"))
        );
        assertEquals("Unsupported field [unsupported]", exc.getMessage());
    }

    public void testParseFromObjectWrongOptionsFormat() {
        Map<String, Object> map = new HashMap<>();
        map.put("source", "doc['my_field']");
        map.put("options", 3);
        ElasticsearchParseException exc = expectThrows(ElasticsearchParseException.class, () -> Script.parse(map));
        assertEquals("Value must be of type Map: [options]", exc.getMessage());
    }

    public void testParseFromObjectWrongParamsFormat() {
        Map<String, Object> map = new HashMap<>();
        map.put("source", "doc['my_field']");
        map.put("params", 3);
        ElasticsearchParseException exc = expectThrows(ElasticsearchParseException.class, () -> Script.parse(map));
        assertEquals("Value must be of type Map: [params]", exc.getMessage());
    }

    public void testDynamicMapToString() {
        Map<String, Object> map = new HashMap<>();
        map.put("long", 1L);
        map.put("string", "value");
        DynamicMap dm = new DynamicMap(map, Collections.emptyMap());
        assertThat(dm.toString(), both(containsString("long=1")).and(containsString("string=value")));
    }
}
