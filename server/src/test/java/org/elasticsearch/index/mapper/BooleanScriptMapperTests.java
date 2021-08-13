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
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.function.Consumer;

public class BooleanScriptMapperTests extends MapperScriptTestCase<BooleanFieldScript.Factory> {

    private static BooleanFieldScript.Factory factory(Consumer<BooleanFieldScript> executor) {
        return new BooleanFieldScript.Factory() {
            @Override
            public BooleanFieldScript.LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup) {
                return new BooleanFieldScript.LeafFactory() {
                    @Override
                    public BooleanFieldScript newInstance(LeafReaderContext ctx) {
                        return new BooleanFieldScript(fieldName, params, searchLookup, ctx) {
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
        return BooleanFieldMapper.CONTENT_TYPE;
    }

    @Override
    protected BooleanFieldScript.Factory serializableScript() {
        return factory(s -> {});
    }

    @Override
    protected BooleanFieldScript.Factory errorThrowingScript() {
        return factory(s -> {
            throw new UnsupportedOperationException("Oops");
        });
    }

    @Override
    protected BooleanFieldScript.Factory singleValueScript() {
        return factory(s -> s.emit(true));
    }

    @Override
    protected BooleanFieldScript.Factory multipleValuesScript() {
        return factory(s -> {
            s.emit(true);
            s.emit(false);
        });
    }

    @Override
    protected void assertMultipleValues(IndexableField[] fields) {
        assertEquals(4, fields.length);
        assertEquals("indexed,omitNorms,indexOptions=DOCS<field:F>", fields[0].toString());
        assertEquals("docValuesType=SORTED_NUMERIC<field:0>", fields[1].toString());
        assertEquals("indexed,omitNorms,indexOptions=DOCS<field:T>", fields[2].toString());
        assertEquals("docValuesType=SORTED_NUMERIC<field:1>", fields[3].toString());
    }

    @Override
    protected void assertDocValuesDisabled(IndexableField[] fields) {
        assertEquals(1, fields.length);
        assertEquals("indexed,omitNorms,indexOptions=DOCS<field:T>", fields[0].toString());
    }

    @Override
    protected void assertIndexDisabled(IndexableField[] fields) {
        assertEquals(1, fields.length);
        assertEquals("docValuesType=SORTED_NUMERIC<field:1>", fields[0].toString());
    }
}
