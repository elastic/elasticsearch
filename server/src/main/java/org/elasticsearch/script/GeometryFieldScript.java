/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Script producing geometries. It generates a unique {@link Geometry} for each document.
 */
public abstract class GeometryFieldScript extends AbstractFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("geometry_field", Factory.class);

    public static final Factory PARSE_FROM_SOURCE = new Factory() {
        @Override
        public LeafFactory newFactory(String field, Map<String, Object> params, SearchLookup lookup, OnScriptError onScriptError) {
            return ctx -> new GeometryFieldScript(field, params, lookup, OnScriptError.FAIL, ctx) {
                @Override
                public void execute() {
                    emitFromSource();
                }
            };
        }

        @Override
        public boolean isResultDeterministic() {
            return true;
        }
    };

    public static Factory leafAdapter(Function<SearchLookup, CompositeFieldScript.LeafFactory> parentFactory) {
        return (leafFieldName, params, searchLookup, onScriptError) -> {
            CompositeFieldScript.LeafFactory parentLeafFactory = parentFactory.apply(searchLookup);
            return (LeafFactory) ctx -> {
                CompositeFieldScript compositeFieldScript = parentLeafFactory.newInstance(ctx);
                return new GeometryFieldScript(leafFieldName, params, searchLookup, onScriptError, ctx) {
                    @Override
                    public void setDocument(int docId) {
                        compositeFieldScript.setDocument(docId);
                    }

                    @Override
                    public void execute() {
                        emitFromCompositeScript(compositeFieldScript);
                    }
                };
            };
        };
    }

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup, OnScriptError onScriptError);
    }

    public interface LeafFactory {
        GeometryFieldScript newInstance(LeafReaderContext ctx);
    }

    private final List<Geometry> geometries = new ArrayList<>();

    private final GeometryParser geometryParser;

    public GeometryFieldScript(
        String fieldName,
        Map<String, Object> params,
        SearchLookup searchLookup,
        OnScriptError onScriptError,
        LeafReaderContext ctx
    ) {
        super(fieldName, params, searchLookup, ctx, onScriptError);
        geometryParser = new GeometryParser(Orientation.CCW.getAsBoolean(), false, true);
    }

    @Override
    protected void prepareExecute() {
        geometries.clear();
    }

    /**
     * Execute the script for the provided {@code docId}, passing results to the {@code consumer}
     */
    public final void runForDoc(int docId, Consumer<Geometry> consumer) {
        runForDoc(docId);
        consumer.accept(geometry());
    }

    /**
     * {@link Geometry} from the last time {@link #runForDoc(int)} was called.
     */
    public final Geometry geometry() {
        if (geometries.isEmpty()) {
            return null;
        }
        return geometries.size() == 1 ? geometries.get(0) : new GeometryCollection<>(geometries);
    }

    /**
     * The number of results produced the last time {@link #runForDoc(int)} was called. It is 1 if
     * the document exists, otherwise 0.
     */
    public final int count() {
        // Note that emitting multiple geometries gets handled by a GeometryCollection
        return geometries.isEmpty() ? 0 : 1;
    }

    @Override
    protected void emitFromObject(Object value) {
        geometries.add(geometryParser.parseGeometry(value));
    }

    public static class Emit {
        private final GeometryFieldScript script;

        public Emit(GeometryFieldScript script) {
            this.script = script;
        }

        public void emit(Object object) {
            script.checkMaxSize(script.geometries.size());
            script.emitFromObject(object);
        }
    }
}
