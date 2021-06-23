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
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.function.Consumer;

public class DateScriptMapperTests extends MapperScriptTestCase<DateFieldScript.Factory> {

    private static DateFieldScript.Factory factory(Consumer<DateFieldScript> executor) {
        return new DateFieldScript.Factory() {
            @Override
            public DateFieldScript.LeafFactory newFactory(
                String fieldName,
                Map<String, Object> params,
                SearchLookup searchLookup,
                DateFormatter formatter
            ) {
                return new DateFieldScript.LeafFactory() {
                    @Override
                    public DateFieldScript newInstance(LeafReaderContext ctx) {
                        return new DateFieldScript(fieldName, params, searchLookup, formatter, ctx) {
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
        return "date";
    }

    @Override
    protected DateFieldScript.Factory serializableScript() {
        return factory(s -> {});
    }

    @Override
    protected DateFieldScript.Factory errorThrowingScript() {
        return factory(s -> {
            throw new UnsupportedOperationException("Oops");
        });
    }

    @Override
    protected DateFieldScript.Factory singleValueScript() {
        return factory(s -> s.emit(1516729294000L));
    }

    @Override
    protected DateFieldScript.Factory multipleValuesScript() {
        return factory(s -> {
            s.emit(1516729294000L);
            s.emit(1516729295000L);
        });
    }

    @Override
    protected void assertMultipleValues(IndexableField[] fields) {
        assertEquals(4, fields.length);
        assertEquals("LongPoint <field:1516729294000>", fields[0].toString());
        assertEquals("docValuesType=SORTED_NUMERIC<field:1516729294000>", fields[1].toString());
        assertEquals("LongPoint <field:1516729295000>", fields[2].toString());
        assertEquals("docValuesType=SORTED_NUMERIC<field:1516729295000>", fields[3].toString());
    }

    @Override
    protected void assertDocValuesDisabled(IndexableField[] fields) {
        assertEquals(1, fields.length);
        assertEquals("LongPoint <field:1516729294000>", fields[0].toString());
    }

    @Override
    protected void assertIndexDisabled(IndexableField[] fields) {
        assertEquals(1, fields.length);
        assertEquals("docValuesType=SORTED_NUMERIC<field:1516729294000>", fields[0].toString());
    }
}
