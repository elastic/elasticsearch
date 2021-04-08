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
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

public class LongScriptMapperTests extends MapperScriptTestCase<LongFieldScript.Factory> {

    private static LongFieldScript.Factory factory(Consumer<LongFieldScript> executor) {
        return new LongFieldScript.Factory() {
            @Override
            public LongFieldScript.LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup) {
                return new LongFieldScript.LeafFactory() {
                    @Override
                    public LongFieldScript newInstance(LeafReaderContext ctx) {
                        return new LongFieldScript(fieldName, params, searchLookup, ctx) {
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
        return "long";
    }

    @Override
    protected LongFieldScript.Factory serializableScript() {
        return factory(s -> {});
    }

    @Override
    protected LongFieldScript.Factory errorThrowingScript() {
        return factory(s -> {
            throw new UnsupportedOperationException("Oops");
        });
    }

    @Override
    protected LongFieldScript.Factory compileScript(String name) {
        if ("single-valued".equals(name)) {
            return factory(s -> s.emit(4));
        }
        if ("multi-valued".equals(name)) {
            return factory(s -> {
                s.emit(1);
                s.emit(2);
            });
        }
        return super.compileScript(name);
    }

    public void testMultipleValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "long");
            b.field("script", "multi-valued");
        }));
        ParsedDocument doc = mapper.parse(source(b -> {}));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(4, fields.length);
        assertEquals("LongPoint <field:1>", fields[0].toString());
        assertEquals("docValuesType=SORTED_NUMERIC<field:1>", fields[1].toString());
        assertEquals("LongPoint <field:2>", fields[2].toString());
        assertEquals("docValuesType=SORTED_NUMERIC<field:2>", fields[3].toString());
    }

    public void testDocValuesDisabled() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "long");
            b.field("script", "single-valued");
            b.field("doc_values", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> {}));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals("LongPoint <field:4>", fields[0].toString());
    }

    public void testIndexDisabled() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "long");
            b.field("script", "single-valued");
            b.field("index", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> {}));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals("docValuesType=SORTED_NUMERIC<field:4>", fields[0].toString());
    }

}
