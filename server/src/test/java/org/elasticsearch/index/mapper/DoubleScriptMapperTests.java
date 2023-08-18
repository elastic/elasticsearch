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

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DoubleScriptMapperTests extends MapperScriptTestCase<DoubleFieldScript.Factory> {

    private static DoubleFieldScript.Factory factory(Consumer<DoubleFieldScript> executor) {
        return new DoubleFieldScript.Factory() {
            @Override
            public DoubleFieldScript.LeafFactory newFactory(
                String fieldName,
                Map<String, Object> params,
                SearchLookup searchLookup,
                OnScriptError onScriptError
            ) {
                return new DoubleFieldScript.LeafFactory() {
                    @Override
                    public DoubleFieldScript newInstance(LeafReaderContext ctx) {
                        return new DoubleFieldScript(fieldName, params, searchLookup, OnScriptError.FAIL, ctx) {
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
        return NumberFieldMapper.NumberType.DOUBLE.typeName();
    }

    @Override
    protected DoubleFieldScript.Factory serializableScript() {
        return factory(s -> {});
    }

    @Override
    protected DoubleFieldScript.Factory errorThrowingScript() {
        return factory(s -> { throw new UnsupportedOperationException("Oops"); });
    }

    @Override
    protected DoubleFieldScript.Factory singleValueScript() {
        return factory(s -> s.emit(3.14));
    }

    @Override
    protected DoubleFieldScript.Factory multipleValuesScript() {
        return factory(s -> {
            s.emit(3.14);
            s.emit(2.78);
        });
    }

    @Override
    protected void assertMultipleValues(List<IndexableField> fields) {
        assertEquals(2, fields.size());
        assertEquals("DoubleField <field:3.14>", fields.get(0).toString());
        assertEquals("DoubleField <field:2.78>", fields.get(1).toString());
    }

    @Override
    protected void assertDocValuesDisabled(List<IndexableField> fields) {
        assertEquals(1, fields.size());
        assertEquals("DoublePoint <field:3.14>", fields.get(0).toString());
    }

    @Override
    protected void assertIndexDisabled(List<IndexableField> fields) {
        assertEquals(1, fields.size());
        assertEquals("docValuesType=SORTED_NUMERIC<field:4614253070214989087>", fields.get(0).toString());
    }
}
