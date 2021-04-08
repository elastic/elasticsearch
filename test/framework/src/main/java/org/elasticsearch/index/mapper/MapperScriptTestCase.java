/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;

public abstract class MapperScriptTestCase<FactoryType> extends MapperServiceTestCase {

    protected abstract String type();

    protected abstract FactoryType serializableScript();

    protected abstract FactoryType errorThrowingScript();

    protected FactoryType compileScript(String name) {
        throw new UnsupportedOperationException("Unknown script " + name);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final <T> T compileScript(Script script, ScriptContext<T> context) {
        if (script.getIdOrCode().equals("serializer_test")) {
            return (T) serializableScript();
        }
        if (script.getIdOrCode().equals("throws")) {
            return (T) errorThrowingScript();
        }
        return (T) compileScript(script.getIdOrCode());
    }

    public void testSerialization() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("scripted");
            b.field("type", type());
            b.field("script", "serializer_test");
            b.endObject();
        }));
        assertThat(
            Strings.toString(mapper.mapping()),
            containsString("\"script\":{\"source\":\"serializer_test\",\"lang\":\"painless\"}")
        );
    }

    public void testCannotIndexDirectlyIntoScriptMapper() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("scripted");
            b.field("type", type());
            b.field("script", "serializer_test");
            b.endObject();
        }));

        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            b.field("scripted", "foo");
        })));
        assertThat(e.getMessage(), containsString("failed to parse field [scripted]"));
        assertEquals("Cannot index data directly into a field with a [script] parameter", e.getCause().getMessage());
    }

    public void testStoredScriptsNotPermitted() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", type());
            b.startObject("script").field("id", "foo").endObject();
        })));
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: stored scripts are not supported on field [field]"));
    }

    public void testIndexAndDocValuesFalseNotPermitted() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", type());
            b.field("index", false);
            b.field("doc_values", false);
            b.field("script", "serializer_test");
        })));
        assertThat(e.getMessage(), containsString("Cannot define script on field with index:false and doc_values:false"));
    }

    public void testScriptErrorParameterRequiresScript() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", type());
            b.field("on_script_error", "ignore");
        })));
        assertThat(e.getMessage(),
            equalTo("Failed to parse mapping: Field [on_script_error] requires field [script] to be configured"));
    }

    public void testIgnoreScriptErrors() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("message").field("type", "keyword").endObject();
            b.startObject("message_error");
            {
                b.field("type", type());
                b.field("script", "throws");
                b.field("on_script_error", "ignore");
            }
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("message", "this is some text")));
        assertThat(doc.rootDoc().getFields("message_error"), arrayWithSize(0));
        assertThat(doc.rootDoc().getField("_ignored").stringValue(), equalTo("message_error"));
    }

    public void testRejectScriptErrors() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("message").field("type", "keyword").endObject();
            b.startObject("message_error");
            {
                b.field("type", type());
                b.field("script", "throws");
            }
            b.endObject();
        }));

        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("message", "foo"))));
        assertThat(e.getMessage(), equalTo("Error executing script on field [message_error]"));
    }
}
