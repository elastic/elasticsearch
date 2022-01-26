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
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.function.Consumer;

public class GeoPointScriptMapperTests extends MapperScriptTestCase<GeoPointFieldScript.Factory> {

    private static GeoPointFieldScript.Factory factory(Consumer<GeoPointFieldScript> executor) {
        return new GeoPointFieldScript.Factory() {
            @Override
            public GeoPointFieldScript.LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup) {
                return new GeoPointFieldScript.LeafFactory() {
                    @Override
                    public GeoPointFieldScript newInstance(LeafReaderContext ctx) {
                        return new GeoPointFieldScript(fieldName, params, searchLookup, ctx) {
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
        return "geo_point";
    }

    @Override
    protected GeoPointFieldScript.Factory serializableScript() {
        return factory(s -> {});
    }

    @Override
    protected GeoPointFieldScript.Factory errorThrowingScript() {
        return factory(s -> { throw new UnsupportedOperationException("Oops"); });
    }

    @Override
    protected GeoPointFieldScript.Factory singleValueScript() {
        return factory(s -> s.emit(1));
    }

    @Override
    protected GeoPointFieldScript.Factory multipleValuesScript() {
        return factory(s -> {
            s.emit(1);
            s.emit(2);
        });
    }

    @Override
    protected void assertMultipleValues(IndexableField[] fields) {
        assertEquals(4, fields.length);
        assertEquals("LatLonPoint <field:0.0,8.381903171539307E-8>", fields[0].toString());
        assertEquals("LatLonDocValuesField <field:0.0,8.381903171539307E-8>", fields[1].toString());
        assertEquals("LatLonPoint <field:0.0,1.6763806343078613E-7>", fields[2].toString());
        assertEquals("LatLonDocValuesField <field:0.0,1.6763806343078613E-7>", fields[3].toString());
    }

    @Override
    protected void assertDocValuesDisabled(IndexableField[] fields) {
        assertEquals(1, fields.length);
        assertEquals("LatLonPoint <field:0.0,8.381903171539307E-8>", fields[0].toString());
    }

    @Override
    protected void assertIndexDisabled(IndexableField[] fields) {
        assertEquals(1, fields.length);
        assertEquals("LatLonDocValuesField <field:0.0,8.381903171539307E-8>", fields[0].toString());
    }
}
