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
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Collections;
import java.util.List;
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

    public static final GeoPointFieldScript.Factory PARSE_FROM_SOURCE
        = (field, params, lookup) -> (GeoPointFieldScript.LeafFactory) ctx -> new GeoPointFieldScript
        (
            field,
            params,
            lookup,
            ctx
        ) {

        @Override
        public void execute() {
            emitFromSource();
        }
    };

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        GeoPointFieldScript newInstance(LeafReaderContext ctx);
    }

    private final GeoPoint scratch = new GeoPoint();

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

    @Override
    protected List<Object> extractFromSource(String path) {
        Object value = XContentMapValues.extractValue(path, leafSearchLookup.source().source());
        if (value instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            if (list.size() > 0 && list.get(0) instanceof Number) {
                //[2, 1]: two values but one single point, return it as a list or each value will be seen as a different geopoint.
                return Collections.singletonList(list);
            }
            //e.g. [ [2,1], {lat:2, lon:1} ]
            return list;
        }
        //e.g. {lat: 2, lon: 1}
        return Collections.singletonList(value);
    }

    @Override
    protected void emitFromObject(Object value) {
        if (value instanceof List<?>) {
            List<?> values = (List<?>) value;
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
                GeoUtils.parseGeoPoint(point, scratch, true);
            } catch(Exception e) {
                //ignore
            }
            emit(scratch.lat(), scratch.lon());
        }
    }

    protected final void emit(double lat, double lon) {
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
