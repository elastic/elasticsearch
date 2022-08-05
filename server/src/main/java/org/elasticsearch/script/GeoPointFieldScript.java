/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Script producing geo points. Similarly to what {@link LatLonDocValuesField} does,
 * it encodes the points as a long value.
 */
public abstract class GeoPointFieldScript extends AbstractFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("geo_point_field", Factory.class);

    public static final Factory PARSE_FROM_SOURCE = new Factory() {
        @Override
        public LeafFactory newFactory(String field, Map<String, Object> params, SearchLookup lookup) {
            return ctx -> new GeoPointFieldScript(field, params, lookup, ctx) {
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
                return new GeoPointFieldScript(leafFieldName, params, searchLookup, ctx) {
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
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        GeoPointFieldScript newInstance(LeafReaderContext ctx);
    }

    private double[] lats = new double[1];
    private double[] lons = new double[1];
    private int count;

    public GeoPointFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, params, searchLookup, ctx);
    }

    /**
     * Execute the script for the provided {@code docId}.
     */
    public final void runForDoc(int docId) {
        count = 0;
        setDocument(docId);
        execute();
    }

    /**
     * Execute the script for the provided {@code docId}, passing results to the {@code consumer}
     */
    public final void runForDoc(int docId, Consumer<GeoPoint> consumer) {
        runForDoc(docId);
        GeoPoint point = new GeoPoint();
        for (int i = 0; i < count; i++) {
            point.reset(lats[i], lons[i]);
            consumer.accept(point);
        }
    }

    /**
     * Latitude values from the last time {@link #runForDoc(int)} was called. This
     * array is mutable and will change with the next call of {@link #runForDoc(int)}.
     * It is also oversized and will contain garbage at all indices at and
     * above {@link #count()}.
     */
    public final double[] lats() {
        return lats;
    }

    /**
     * Longitude values from the last time {@link #runForDoc(int)} was called. This
     * array is mutable and will change with the next call of {@link #runForDoc(int)}.
     * It is also oversized and will contain garbage at all indices at and
     * above {@link #count()}.
     */
    public final double[] lons() {
        return lons;
    }

    /**
     * The number of results produced the last time {@link #runForDoc(int)} was called.
     */
    public final int count() {
        return count;
    }

    @Override
    protected List<Object> extractFromSource(String path) {
        Object value = XContentMapValues.extractValue(path, sourceLookup.source());
        if (value instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            if (list.size() > 0 && list.get(0) instanceof Number) {
                // [2, 1]: two values but one single point, return it as a list or each value will be seen as a different geopoint.
                return Collections.singletonList(list);
            }
            // e.g. [ [2,1], {lat:2, lon:1} ]
            return list;
        }
        // e.g. {lat: 2, lon: 1}
        return Collections.singletonList(value);
    }

    @Override
    protected void emitFromObject(Object value) {
        if (value instanceof List<?> values) {
            if (values.size() > 0 && values.get(0) instanceof Number) {
                emitPoint(value);
            } else {
                for (Object point : values) {
                    emitPoint(point);
                }
            }
        } else {
            emitPoint(value);
        }
    }

    private void emitPoint(Object point) {
        if (point != null) {
            try {
                GeoPoint geoPoint = GeoUtils.parseGeoPoint(point, true);
                emit(geoPoint.lat(), geoPoint.lon());
            } catch (Exception e) {
                emit(0, 0);
            }
        }
    }

    protected final void emit(double lat, double lon) {
        checkMaxSize(count);
        if (lats.length < count + 1) {
            lats = ArrayUtil.grow(lats, count + 1);
            lons = ArrayUtil.growExact(lons, lats.length);
        }
        lats[count] = lat;
        lons[count++] = lon;
    }

    public static class Emit {
        private final GeoPointFieldScript script;

        public Emit(GeoPointFieldScript script) {
            this.script = script;
        }

        public void emit(double lat, double lon) {
            script.checkMaxSize(script.count());
            script.emit(lat, lon);
        }
    }
}
