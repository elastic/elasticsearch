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
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class KeywordScriptMapperTests extends MapperScriptTestCase<StringFieldScript.Factory> {

    private static StringFieldScript.Factory factory(Consumer<StringFieldScript> executor) {
        return new StringFieldScript.Factory() {
            @Override
            public StringFieldScript.LeafFactory newFactory(
                String fieldName,
                Map<String, Object> params,
                SearchLookup searchLookup,
                OnScriptError onScriptError
            ) {
                return new StringFieldScript.LeafFactory() {
                    @Override
                    public StringFieldScript newInstance(LeafReaderContext ctx) {
                        return new StringFieldScript(fieldName, params, searchLookup, OnScriptError.FAIL, ctx) {
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
        return "keyword";
    }

    @Override
    protected StringFieldScript.Factory serializableScript() {
        return factory(s -> {});
    }

    @Override
    protected StringFieldScript.Factory errorThrowingScript() {
        return factory(s -> { throw new UnsupportedOperationException("Oops"); });
    }

    @Override
    protected StringFieldScript.Factory singleValueScript() {
        return factory(s -> s.emit("value"));
    }

    @Override
    protected StringFieldScript.Factory multipleValuesScript() {
        return factory(s -> {
            s.emit("value1");
            s.emit("value2");
        });
    }

    @Override
    protected void assertMultipleValues(List<IndexableField> fields) {
        assertEquals(2, fields.size());
        assertEquals("indexed,omitNorms,indexOptions=DOCS,docValuesType=SORTED_SET<field:[76 61 6c 75 65 31]>", fields.get(0).toString());
        assertEquals("indexed,omitNorms,indexOptions=DOCS,docValuesType=SORTED_SET<field:[76 61 6c 75 65 32]>", fields.get(1).toString());
    }

    @Override
    protected void assertDocValuesDisabled(List<IndexableField> fields) {
        assertEquals(1, fields.size());
        assertEquals("indexed,omitNorms,indexOptions=DOCS<field:[76 61 6c 75 65]>", fields.get(0).toString());
    }

    @Override
    protected void assertIndexDisabled(List<IndexableField> fields) {
        assertEquals(1, fields.size());
        assertEquals("docValuesType=SORTED_SET<field:[76 61 6c 75 65]>", fields.get(0).toString());
    }

    @Override
    protected StringFieldScript.Factory script(String id) {
        if ("day_of_week".equals(id)) {
            return factory(s -> s.emit("Thursday"));
        }
        if ("letters".equals(id)) {
            return factory(s -> {
                for (Object day : s.getDoc().get("day_of_week")) {
                    String dow = (String) day;
                    for (int i = 0; i < dow.length(); i++) {
                        s.emit(Character.toString(dow.charAt(i)));
                    }
                }
            });
        }
        return super.script(id);
    }

    public void testCrossFieldReferences() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("day_of_week").field("type", "keyword").field("script", "day_of_week").endObject();
            b.startObject("letters").field("type", "keyword").field("script", "letters").endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {}));
        List<IndexableField> letterFields = doc.rootDoc().getFields("letters");
        assertEquals(8, letterFields.size());
    }
}
