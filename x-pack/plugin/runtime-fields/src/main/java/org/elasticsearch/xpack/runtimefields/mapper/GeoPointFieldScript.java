/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;
import org.apache.lucene.document.LatLonDocValuesField;

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

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "geo_point_whitelist.txt"));
    }

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
                Object v = XContentMapValues.extractValue(field, leafSearchLookup.source().loadSourceIfNeeded());
                GeoUtils.parseGeoPoint(v, scratch, true);
                emit(scratch.lat(), scratch.lon());
            } catch (Exception e) {
                // ignore
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
