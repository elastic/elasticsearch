/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
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

    protected abstract FactoryType singleValueScript();

    protected abstract FactoryType multipleValuesScript();

    protected FactoryType script(String id) {
        throw new UnsupportedOperationException("Unknown script " + id);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        if (script.getIdOrCode().equals("serializer_test")) {
            return (T) serializableScript();
        }
        if (script.getIdOrCode().equals("throws")) {
            return (T) errorThrowingScript();
        }
        if (script.getIdOrCode().equals("single-valued")) {
            return (T) singleValueScript();
        }
        if (script.getIdOrCode().equals("multi-valued")) {
            return (T) multipleValuesScript();
        }
        return (T) script(script.getIdOrCode());
    }

    public void testToXContent() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("scripted");
            b.field("type", type());
            b.field("script", "serializer_test");
            b.endObject();
        }));
        assertThat(Strings.toString(mapper.mapping()), containsString("""
            "script":{"source":"serializer_test","lang":"painless"}"""));
    }

    public void testCannotIndexDirectlyIntoScriptMapper() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("scripted");
            b.field("type", type());
            b.field("script", "serializer_test");
            b.endObject();
        }));

        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> { b.field("scripted", "foo"); })));
        assertThat(e.getMessage(), containsString("failed to parse field [scripted]"));
        assertEquals("Cannot index data directly into a field with a [script] parameter", e.getCause().getMessage());
    }

    public final void testStoredScriptsNotPermitted() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", type());
            b.startObject("script").field("id", "foo").endObject();
        })));
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: stored scripts are not supported on field [field]"));
    }

    public final void testIndexAndDocValuesFalseNotPermitted() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", type());
            b.field("index", false);
            b.field("doc_values", false);
            b.field("script", "serializer_test");
        })));
        assertThat(e.getMessage(), containsString("Cannot define script on field with index:false and doc_values:false"));
    }

    public final void testMultiFieldsNotPermitted() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", type());
            b.field("script", "serializer_test");
            b.startObject("fields");
            b.startObject("subfield").field("type", "keyword").endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Cannot define multifields on a field with a script"));
    }

    public final void testCopyToNotPermitted() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", type());
            b.field("script", "serializer_test");
            b.array("copy_to", "foo");
        })));
        assertThat(e.getMessage(), containsString("Cannot define copy_to parameter on a field with a script"));
    }

    public final void testOnScriptErrorParameterRequiresScript() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", type());
            b.field("on_script_error", "continue");
        })));
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: Field [on_script_error] requires field [script] to be configured"));
    }

    public final void testOnScriptErrorContinue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("message").field("type", "keyword").endObject();
            b.startObject("message_error");
            {
                b.field("type", type());
                b.field("script", "throws");
                b.field("on_script_error", "continue");
            }
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("message", "this is some text")));
        assertThat(doc.rootDoc().getFields("message_error"), arrayWithSize(0));
        assertThat(doc.rootDoc().getField("_ignored").stringValue(), equalTo("message_error"));
    }

    public final void testRejectScriptErrors() throws IOException {
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

    public final void testMultipleValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", type());
            b.field("script", "multi-valued");
        }));
        ParsedDocument doc = mapper.parse(source(b -> {}));
        assertMultipleValues(doc.rootDoc().getFields("field"));
    }

    protected abstract void assertMultipleValues(IndexableField[] fields);

    public final void testDocValuesDisabled() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", type());
            b.field("script", "single-valued");
            b.field("doc_values", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> {}));
        assertDocValuesDisabled(doc.rootDoc().getFields("field"));
    }

    protected abstract void assertDocValuesDisabled(IndexableField[] fields);

    public final void testIndexDisabled() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", type());
            b.field("script", "single-valued");
            b.field("index", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> {}));
        assertIndexDisabled(doc.rootDoc().getFields("field"));
    }

    protected abstract void assertIndexDisabled(IndexableField[] fields);
}
