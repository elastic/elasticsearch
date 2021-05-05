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
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.function.Consumer;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/**
 * Script producing geo points. Similarly to what {@link LatLonDocValuesField} does,
 * it encodes the points as a long value.
 */
public abstract class GeoPointFieldScript extends AbstractLongFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("geo_point_field", Factory.class);

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        GeoPointFieldScript newInstance(LeafReaderContext ctx);
    }

    public GeoPointFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, params, searchLookup, ctx);
    }

    /**
     * Consumers must copy the emitted GeoPoint(s) if stored.
     */
    public void runGeoPointForDoc(int doc, Consumer<GeoPoint> consumer) {
        runForDoc(doc);
        GeoPoint point = new GeoPoint();
        for (int i = 0; i < count(); i++) {
            final int lat = (int) (values()[i] >>> 32);
            final int lon = (int) (values()[i] & 0xFFFFFFFF);
            point.reset(GeoEncodingUtils.decodeLatitude(lat), GeoEncodingUtils.decodeLongitude(lon));
            consumer.accept(point);
        }
    }

    protected void emit(double lat, double lon) {
        int latitudeEncoded = encodeLatitude(lat);
        int longitudeEncoded = encodeLongitude(lon);
        emit(Long.valueOf((((long) latitudeEncoded) << 32) | (longitudeEncoded & 0xFFFFFFFFL)));
    }

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
