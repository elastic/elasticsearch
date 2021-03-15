/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.runtimefields.mapper.DoubleFieldScript;
import org.elasticsearch.runtimefields.mapper.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;

public class CalculatedFieldTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Set.of(new TestScriptPlugin());
    }

    public void testCalculatedFieldLength() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("message").field("type", "text").endObject();
            b.startObject("message_length");
            b.field("type", "long");
            b.field("script", "message_length");
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("message", "this is some text")));
        IndexableField[] lengthFields = doc.rootDoc().getFields("message_length");
        assertEquals(2, lengthFields.length);
        assertEquals("LongPoint <message_length:17>", lengthFields[0].toString());
        assertEquals("docValuesType=SORTED_NUMERIC<message_length:17>", lengthFields[1].toString());
    }

    public void testDocAccess() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("long_field").field("type", "long").endObject();
            b.startObject("long_field_plus_two");
            b.field("type", "long");
            b.field("script", "long_field_plus_two");
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("long_field", 4)));
        assertEquals(doc.rootDoc().getField("long_field_plus_two").numericValue(), 6L);
    }

    public void testDoublesAccess() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("double_field").field("type", "double").endObject();
            b.startObject("double_field_plus_two");
            b.field("type", "double");
            b.field("script", "double_field_plus_two");
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("double_field", 4.5)));
        assertEquals(doc.rootDoc().getField("double_field_plus_two").numericValue(), 6.5);
    }

    public void testSerialization() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("message").field("type", "text").endObject();
            b.startObject("message_length");
            b.field("type", "long");
            b.field("script", "message_length");
            b.endObject();
        }));
        assertEquals(
            "{\"_doc\":{\"properties\":{\"message\":{\"type\":\"text\"}," +
                "\"message_length\":{\"type\":\"long\",\"script\":{\"source\":\"message_length\",\"lang\":\"painless\"}}}}}",
            Strings.toString(mapper.mapping()));
    }

    public void testCrossReferences() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("message").field("type", "text").endObject();
            b.startObject("message_length_plus_two");
            b.field("type", "long");
            b.field("script", "message_length_plus_two");
            b.endObject();
            b.startObject("message_length");
            b.field("type", "long");
            b.field("script", "message_length");
            b.endObject();
            b.startObject("message_length_plus_four");
            b.field("type", "double");
            b.field("script", "message_length_plus_two_plus_two");
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("message", "this is a message")));
        assertEquals(doc.rootDoc().getField("message_length_plus_two").numericValue(), 19L);
        assertEquals(doc.rootDoc().getField("message_length").numericValue(), 17L);
        assertEquals(doc.rootDoc().getField("message_length_plus_four").numericValue(), 21d);
    }

    public void testCannotIndexDirectlyIntoScriptMapper() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("message").field("type", "text").endObject();
            b.startObject("message_length");
            b.field("type", "long");
            b.field("script", "length");
            b.endObject();
        }));

        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            b.field("message", "foo");
            b.field("message_length", 3);
        })));
        assertEquals("failed to parse field [message_length] of type [long] in document with id '1'. Preview of field's value: '3'",
            e.getMessage());
        Throwable original = e.getCause();
        assertEquals("Cannot index data directly into scripted field", original.getMessage());
    }

    public void testLoopDetection() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field1").field("type", "long").field("script", "field2_plus_two").endObject();
            b.startObject("field2").field("type", "long").field("script", "field1_plus_two").endObject();
        }));

        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {})));
        assertEquals("failed to parse", e.getMessage());
        assertThat(e.getCause().getMessage(), containsString("Loop in field resolution detected"));
        // Can be either field1->field2->field1 or field2->field1->field2 because
        // post-phase executor order is not deterministic
        assertThat(e.getCause().getMessage(), containsString("field1->field2"));
    }

    @AwaitsFix(bugUrl = "TODO")
    public void testCannotReferToRuntimeFields() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("runtime-field").field("type", "long").endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("index-field").field("type", "long").field("script", "refer-to-runtime").endObject();
            b.endObject();
        }));

        Exception e = expectThrows(IllegalArgumentException.class, () -> mapper.parse(source(b -> {})));
        assertEquals("Cannot reference runtime field [runtime-field] in an index-time script", e.getMessage());
    }

    private static Consumer<TestLongFieldScript> getLongScript(String name) {
        if ("refer-to-runtime".equals(name)) {
            return s -> {
                s.emitValue((long) s.getDoc().get("runtime-field").get(0));
            };
        }
        if (name.endsWith("_length")) {
            String field = name.substring(0, name.lastIndexOf("_length"));
            return s -> {
                for (Object v : s.extractValuesFromSource(field)) {
                    s.emitValue(Objects.toString(v).length());
                }
            };
        }
        if (name.endsWith("_plus_two")) {
            String field = name.substring(0, name.lastIndexOf("_plus_two"));
            return s -> {
                long input = (long) s.getDoc().get(field).get(0);
                s.emitValue(input + 2);
            };
        }
        throw new UnsupportedOperationException("Unknown script [" + name + "]");
    }

    private static Consumer<TestDoubleFieldScript> getDoubleScript(String name) {
        if (name.endsWith("_plus_two")) {
            String field = name.substring(0, name.lastIndexOf("_plus_two"));
            return s -> {
                Number input = (Number) s.getDoc().get(field).get(0);
                s.emitValue(input.doubleValue() + 2);
            };
        }
        throw new UnsupportedOperationException("Unknown script [" + name + "]");
    }

    private static class TestLongFieldScript extends LongFieldScript {

        final Consumer<TestLongFieldScript> executor;

        TestLongFieldScript(
            String fieldName,
            Map<String, Object> params,
            SearchLookup searchLookup,
            LeafReaderContext ctx,
            Consumer<TestLongFieldScript> executor
        ) {
            super(fieldName, params, searchLookup, ctx);
            this.executor = executor;
        }

        @Override
        public void execute() {
            executor.accept(this);
        }

        public void emitValue(long v) {
            super.emit(v);
        }

        public List<Object> extractValuesFromSource(String path) {
            return super.extractFromSource(path);
        }
    }

    private static class TestDoubleFieldScript extends DoubleFieldScript {

        final Consumer<TestDoubleFieldScript> executor;

        TestDoubleFieldScript(
            String fieldName,
            Map<String, Object> params,
            SearchLookup searchLookup,
            LeafReaderContext ctx,
            Consumer<TestDoubleFieldScript> executor
        ) {
            super(fieldName, params, searchLookup, ctx);
            this.executor = executor;
        }

        @Override
        public void execute() {
            executor.accept(this);
        }

        public void emitValue(double v) {
            super.emit(v);
        }

        public List<Object> extractValuesFromSource(String path) {
            return super.extractFromSource(path);
        }
    }

    public static class TestScriptPlugin extends Plugin implements ScriptPlugin {

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "painless";
                }

                @Override
                @SuppressWarnings("unchecked")
                public <FactoryType> FactoryType compile(
                    String name,
                    String code,
                    ScriptContext<FactoryType> context,
                    Map<String, String> params
                ) {
                    if (context.factoryClazz == LongFieldScript.Factory.class) {
                        return (FactoryType) (LongFieldScript.Factory) (n, p, l) -> ctx -> new TestLongFieldScript(
                            n, p, l, ctx, getLongScript(name)
                        );
                    }
                    if (context.factoryClazz == DoubleFieldScript.Factory.class) {
                        return (FactoryType) (DoubleFieldScript.Factory) (n, p, l) -> ctx -> new TestDoubleFieldScript(
                            n, p, l, ctx, getDoubleScript(name)
                        );
                    }
                    throw new IllegalArgumentException("Unknown factory type " + context.factoryClazz + " for code " + code);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(LongFieldScript.CONTEXT, DoubleFieldScript.CONTEXT);
                }
            };
        }
    }

}
