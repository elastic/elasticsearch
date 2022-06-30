/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.script;

import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.AbstractPointFieldScript;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Script producing cartesian points.
 */
public abstract class CartesianPointFieldScript extends AbstractPointFieldScript<CartesianPoint> {
    public static final ScriptContext<Factory> CONTEXT = newContext("point_field", Factory.class);

    public static final Factory PARSE_FROM_SOURCE = new Factory() {
        @Override
        public LeafFactory newFactory(String field, Map<String, Object> params, SearchLookup lookup) {
            return ctx -> new CartesianPointFieldScript(field, params, lookup, ctx) {
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
        return (leafFieldName, params, searchLookup) -> {
            CompositeFieldScript.LeafFactory parentLeafFactory = parentFactory.apply(searchLookup);
            return (LeafFactory) ctx -> {
                CompositeFieldScript compositeFieldScript = parentLeafFactory.newInstance(ctx);
                return new CartesianPointFieldScript(leafFieldName, params, searchLookup, ctx) {
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

    public interface LeafFactory {
        CartesianPointFieldScript newInstance(LeafReaderContext ctx);
    }

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public CartesianPointFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, params, searchLookup, ctx, new CartesianPointFieldScriptEncoder());
    }

    public static class CartesianPointFieldScriptEncoder implements PointFieldScriptEncoder<CartesianPoint> {
        /**
         * Consumers must copy the emitted CartesianPoint(s) if stored.
         */
        public void consumeValues(long[] values, int count, Consumer<CartesianPoint> consumer) {
            CartesianPoint point = new CartesianPoint();
            for (int i = 0; i < count; i++) {
                final double x = XYEncodingUtils.decode((int) (values[i] >>> 32));
                final double y = XYEncodingUtils.decode((int) (values[i] & 0xFFFFFFFF));
                point.reset(x, y);
                consumer.accept(point);
            }
        }
    }

    protected void emitPoint(Object obj) {
        if (obj != null) {
            try {
                CartesianPoint point = CartesianPoint.parsePoint(obj, true);
                emit(point.getX(), point.getY());
            } catch (Exception e) {
                emit(0, 0);
            }
        }
    }

    protected final void emit(double x, double y) {
        final long xi = XYEncodingUtils.encode((float) x);
        final long yi = XYEncodingUtils.encode((float) y);
        long value = yi | xi << 32;
        emit(value);
    }

    // This cannot be generic and moved to parent class because it is used by painless
    public static class Emit {
        private final CartesianPointFieldScript script;

        public Emit(CartesianPointFieldScript script) {
            this.script = script;
        }

        public void emit(double a, double b) {
            script.emit(a, b);
        }
    }
}
