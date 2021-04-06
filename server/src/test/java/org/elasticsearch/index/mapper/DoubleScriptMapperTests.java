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
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

public class DoubleScriptMapperTests extends MapperScriptTestCase<DoubleFieldScript.Factory> {

    private static DoubleFieldScript.Factory factory(Consumer<DoubleFieldScript> executor) {
        return new DoubleFieldScript.Factory() {
            @Override
            public DoubleFieldScript.LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup) {
                return new DoubleFieldScript.LeafFactory() {
                    @Override
                    public DoubleFieldScript newInstance(LeafReaderContext ctx) {
                        return new DoubleFieldScript(fieldName, params, searchLookup, ctx) {
                            @Override
                            public void execute() {
                                executor.accept(this);
                            }
                        };
                    }
                };
            }
        };
    }

    @Override
    protected String type() {
        return "double";
    }

    @Override
    protected DoubleFieldScript.Factory serializableScript() {
        return factory(s -> {});
    }

    @Override
    protected DoubleFieldScript.Factory errorThrowingScript() {
        return factory(s -> {
            throw new UnsupportedOperationException("Oops");
        });
    }

    @Override
    protected DoubleFieldScript.Factory compileScript(String name) {
        if ("single-valued".equals(name)) {
            return factory(s -> s.emit(3.14));
        }
        if ("multi-valued".equals(name)) {
            return factory(s -> {
                s.emit(3.14);
                s.emit(2.78);
            });
        }
        return super.compileScript(name);
    }

    public void testMultipleValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "double");
            b.field("script", "multi-valued");
        }));
        ParsedDocument doc = mapper.parse(source(b -> {}));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(4, fields.length);
        assertEquals("DoublePoint <field:3.14>", fields[0].toString());
        assertEquals("docValuesType=SORTED_NUMERIC<field:4614253070214989087>", fields[1].toString());
        assertEquals("DoublePoint <field:2.78>", fields[2].toString());
        assertEquals("docValuesType=SORTED_NUMERIC<field:4613442422282062397>", fields[3].toString());
    }

    public void testDocValuesDisabled() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "double");
            b.field("script", "single-valued");
            b.field("doc_values", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> {}));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals("DoublePoint <field:3.14>", fields[0].toString());
    }

    public void testIndexDisabled() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "double");
            b.field("script", "single-valued");
            b.field("index", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> {}));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals("docValuesType=SORTED_NUMERIC<field:4614253070214989087>", fields[0].toString());
    }
}
