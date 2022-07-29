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

    private static GeoPointFieldScript.Factory factory(Consumer<GeoPointFieldScript.Emit> executor) {
        return new GeoPointFieldScript.Factory() {
            @Override
            public GeoPointFieldScript.LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup) {
                return new GeoPointFieldScript.LeafFactory() {
                    @Override
                    public GeoPointFieldScript newInstance(LeafReaderContext ctx) {
                        return new GeoPointFieldScript(fieldName, params, searchLookup, ctx) {
                            @Override
                            public void execute() {
                                executor.accept(new Emit(this));
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
        return factory(s -> s.emit(-1, 1));
    }

    @Override
    protected GeoPointFieldScript.Factory multipleValuesScript() {
        return factory(s -> {
            s.emit(-1, 1);
            s.emit(-2, 2);
        });
    }

    @Override
    protected void assertMultipleValues(IndexableField[] fields) {
        assertEquals(4, fields.length);
        assertEquals("LatLonPoint <field:-1.000000024214387,0.9999999403953552>", fields[0].toString());
        assertEquals("LatLonDocValuesField <field:-1.000000024214387,0.9999999403953552>", fields[1].toString());
        assertEquals("LatLonPoint <field:-2.000000006519258,1.9999999646097422>", fields[2].toString());
        assertEquals("LatLonDocValuesField <field:-2.000000006519258,1.9999999646097422>", fields[3].toString());
    }

    @Override
    protected void assertDocValuesDisabled(IndexableField[] fields) {
        assertEquals(1, fields.length);
        assertEquals("LatLonPoint <field:-1.000000024214387,0.9999999403953552>", fields[0].toString());
    }

    @Override
    protected void assertIndexDisabled(IndexableField[] fields) {
        assertEquals(1, fields.length);
        assertEquals("LatLonDocValuesField <field:-1.000000024214387,0.9999999403953552>", fields[0].toString());
    }
}
