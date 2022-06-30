/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/**
 * Script producing geo points. Similarly to what {@link LatLonDocValuesField} does,
 * it encodes the points as a long value.
 */
public abstract class GeoPointFieldScript extends AbstractPointFieldScript<GeoPoint> {
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

    public interface LeafFactory {
        GeoPointFieldScript newInstance(LeafReaderContext ctx);
    }

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public GeoPointFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, params, searchLookup, ctx, new GeoPointFieldScriptEncoder());
    }

    public static class GeoPointFieldScriptEncoder implements PointFieldScriptEncoder<GeoPoint> {
        /**
         * Consumers must copy the emitted GeoPoint(s) if stored.
         */
        public void consumeValues(long[] values, int count, Consumer<GeoPoint> consumer) {
            GeoPoint point = new GeoPoint();
            for (int i = 0; i < count; i++) {
                final int lat = (int) (values[i] >>> 32);
                final int lon = (int) (values[i] & 0xFFFFFFFF);
                point.reset(GeoEncodingUtils.decodeLatitude(lat), GeoEncodingUtils.decodeLongitude(lon));
                consumer.accept(point);
            }
        }
    }

    protected void emitPoint(Object point) {
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
        int latitudeEncoded = encodeLatitude(lat);
        int longitudeEncoded = encodeLongitude(lon);
        emit((((long) latitudeEncoded) << 32) | (longitudeEncoded & 0xFFFFFFFFL));
    }

    // This cannot be generic and moved to parent class because it is used by painless
    public static class Emit {
        private final GeoPointFieldScript script;

        public Emit(GeoPointFieldScript script) {
            this.script = script;
        }

        public void emit(double lat, double lon) {
            script.emit(lat, lon);
        }
    }
}
