/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class ScriptFieldMapperScriptParsingTests extends ESTestCase {

    private static Mapper.TypeParser.ParserContext parserContext() {
        return new Mapper.TypeParser.ParserContext(null, null, null, Version.CURRENT, null);
    }

    public void testParseScriptShortSyntax() {

        Script script = ScriptFieldMapper.Builder.parseScript("script", parserContext(), "doc['my_field']");
        assertEquals(PainlessScriptEngine.NAME, script.getLang());
        assertEquals("doc['my_field']", script.getIdOrCode());
        assertEquals(0, script.getParams().size());
        assertEquals(0, script.getOptions().size());
        assertEquals(ScriptType.INLINE, script.getType());
    }

    public void testParseScript() {
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
        if (randomBoolean()) {
            map.put("lang", PainlessScriptEngine.NAME);
        }
        Script script = ScriptFieldMapper.Builder.parseScript("script", parserContext(), map);
        assertEquals(PainlessScriptEngine.NAME, script.getLang());
        assertEquals("doc['my_field']", script.getIdOrCode());
        assertEquals(ScriptType.INLINE, script.getType());
        assertEquals(params, script.getParams());
        assertEquals(options, script.getOptions());
    }

    public void testParseScriptFromScript() {
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
        Script script = new Script(ScriptType.INLINE, PainlessScriptEngine.NAME, "doc['field']", options, params);
        Map<String, Object> scriptObject = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(script), false);
        Script parsedScript = ScriptFieldMapper.Builder.parseScript("my_field", parserContext(), scriptObject);
        assertEquals(script, parsedScript);
    }

    public void testParseScriptWrongFormat() {
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> ScriptFieldMapper.Builder.parseScript("my_field", parserContext(), 3)
        );
        assertEquals("unable to parse script for script field [my_field]", iae.getMessage());
    }

    public void testParseScriptWrongOptionsFormat() {
        Map<String, Object> map = new HashMap<>();
        map.put("source", "doc['my_field']");
        map.put("options", 3);
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> ScriptFieldMapper.Builder.parseScript("my_field", parserContext(), map)
        );
        assertEquals("unable to parse options for script field [my_field]", iae.getMessage());
    }

    public void testParseScriptWrongParamsFormat() {
        Map<String, Object> map = new HashMap<>();
        map.put("source", "doc['my_field']");
        map.put("params", 3);
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> ScriptFieldMapper.Builder.parseScript("my_field", parserContext(), map)
        );
        assertEquals("unable to parse params for script field [my_field]", iae.getMessage());
    }

    public void testParseScriptOnlyPainlessIsSupported() {
        Map<String, Object> map = new HashMap<>();
        map.put("source", "doc['my_field']");
        String lang;
        if (randomBoolean()) {
            lang = "mustache";
        } else if (randomBoolean()) {
            lang = "expression";
        } else {
            lang = "anything";
        }
        map.put("lang", lang);
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> ScriptFieldMapper.Builder.parseScript("my_field", parserContext(), map)
        );
        assertEquals("script lang [" + lang + "] not supported for script field [my_field]", iae.getMessage());
    }

    public void testParseScriptStoredScriptsAreNotSupported() {
        {
            Map<String, Object> map = new HashMap<>();
            map.put("id", "test");
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> ScriptFieldMapper.Builder.parseScript("my_field", parserContext(), map)
            );
            assertEquals("script source must be specified for script field [my_field]", iae.getMessage());
        }
        {
            Map<String, Object> map = new HashMap<>();
            map.put("source", "doc['my_field']");
            map.put("id", "test");
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> ScriptFieldMapper.Builder.parseScript("my_field", parserContext(), map)
            );
            assertEquals("unsupported parameters specified for script field [my_field]: [id]", iae.getMessage());
        }
    }
}
