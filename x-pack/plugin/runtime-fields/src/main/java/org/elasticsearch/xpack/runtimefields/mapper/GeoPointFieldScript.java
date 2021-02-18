/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.List;
import java.util.Map;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/**
 * Script producing geo points. Similarly to what {@link LatLonDocValuesField} does,
 * it encodes the points as a long value.
 */
public abstract class GeoPointFieldScript extends AbstractLongFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("geo_point_script_field", Factory.class);

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        GeoPointFieldScript newInstance(LeafReaderContext ctx);
    }

    public static final Factory PARSE_FROM_SOURCE = (field, params, lookup) -> (LeafFactory) ctx -> new GeoPointFieldScript(
        field,
        params,
        lookup,
        ctx
    ) {
        private final GeoPoint scratch = new GeoPoint();

        @Override
        public void execute() {
            try {
                Object value = XContentMapValues.extractValue(field, leafSearchLookup.source().source());
                if (value instanceof List<?>) {
                    List<?> values = (List<?>) value;
                    if (values.size() > 0 && values.get(0) instanceof Number) {
                        parsePoint(value);
                    } else {
                        for (Object point : values) {
                            parsePoint(point);
                        }
                    }
                } else {
                    parsePoint(value);
                }
            } catch (Exception e) {
                // ignore
            }
        }

        private void parsePoint(Object point) {
            if (point != null) {
                GeoUtils.parseGeoPoint(point, scratch, true);
                emit(scratch.lat(), scratch.lon());
            }
        }
    };

    public GeoPointFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, params, searchLookup, ctx);
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
