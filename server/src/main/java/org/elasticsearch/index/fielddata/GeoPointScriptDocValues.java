/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.script.GeoPointFieldScript;

import java.util.Arrays;

public final class GeoPointScriptDocValues extends MultiGeoPointValues {
    private final GeoPointFieldScript script;
    private final GeoPoint point;
    private int cursor;

    GeoPointScriptDocValues(GeoPointFieldScript script) {
        this.script = script;
        this.point = new GeoPoint();
    }

    @Override
    public boolean advanceExact(int docId) {
        script.runForDoc(docId);
        if (script.count() == 0) {
            return false;
        }
        Arrays.sort(script.values(), 0, script.count());
        cursor = 0;
        return true;
    }

    @Override
    public int docValueCount() {
        return script.count();
    }

    @Override
    public GeoPoint nextValue() {
        final long value = script.values()[cursor++];
        final int lat = (int) (value >>> 32);
        final int lon = (int) (value & 0xFFFFFFFF);
        return point.reset(GeoEncodingUtils.decodeLatitude(lat), GeoEncodingUtils.decodeLongitude(lon));
    }
}
