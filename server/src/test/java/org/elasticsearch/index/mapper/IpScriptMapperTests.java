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
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class IpScriptMapperTests extends MapperScriptTestCase<IpFieldScript.Factory> {

    private static IpFieldScript.Factory factory(Consumer<IpFieldScript> executor) {
        return new IpFieldScript.Factory() {
            @Override
            public IpFieldScript.LeafFactory newFactory(
                String fieldName,
                Map<String, Object> params,
                SearchLookup searchLookup,
                OnScriptError onScriptError
            ) {
                return new IpFieldScript.LeafFactory() {
                    @Override
                    public IpFieldScript newInstance(LeafReaderContext ctx) {
                        return new IpFieldScript(fieldName, params, searchLookup, OnScriptError.FAIL, ctx) {
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
        return "ip";
    }

    @Override
    protected IpFieldScript.Factory serializableScript() {
        return factory(s -> {});
    }

    @Override
    protected IpFieldScript.Factory errorThrowingScript() {
        return factory(s -> { throw new UnsupportedOperationException("Oops"); });
    }

    @Override
    protected IpFieldScript.Factory singleValueScript() {
        return factory(s -> s.emit("::1"));
    }

    @Override
    protected IpFieldScript.Factory multipleValuesScript() {
        return factory(s -> {
            s.emit("::1");
            s.emit("::2");
        });
    }

    @Override
    protected void assertMultipleValues(List<IndexableField> fields) {
        assertEquals(4, fields.size());
        assertEquals("InetAddressPoint <field:[0:0:0:0:0:0:0:1]>", fields.get(0).toString());
        assertEquals("docValuesType=SORTED_SET<field:[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1]>", fields.get(1).toString());
        assertEquals("InetAddressPoint <field:[0:0:0:0:0:0:0:2]>", fields.get(2).toString());
        assertEquals("docValuesType=SORTED_SET<field:[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 2]>", fields.get(3).toString());
    }

    @Override
    protected void assertDocValuesDisabled(List<IndexableField> fields) {
        assertEquals(1, fields.size());
        assertEquals("InetAddressPoint <field:[0:0:0:0:0:0:0:1]>", fields.get(0).toString());
    }

    @Override
    protected void assertIndexDisabled(List<IndexableField> fields) {
        assertEquals(1, fields.size());
        assertEquals("docValuesType=SORTED_SET<field:[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1]>", fields.get(0).toString());
    }
}
