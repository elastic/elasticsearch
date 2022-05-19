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
    private final GeoPointScriptSortedNumericDocValues docValues;
    private final GeoPoint point;

    GeoPointScriptDocValues(GeoPointFieldScript script) {
        this.docValues = new GeoPointScriptSortedNumericDocValues(script);
        this.point = new GeoPoint();
    }

    @Override
    public boolean advanceExact(int docId) {
        return docValues.advanceExact(docId);
    }

    @Override
    public int docValueCount() {
        return docValues.docValueCount();
    }

    @Override
    public GeoPoint nextValue() {
        final long value = docValues.nextValue();
        final int lat = (int) (value >>> 32);
        final int lon = (int) (value & 0xFFFFFFFF);
        return point.reset(GeoEncodingUtils.decodeLatitude(lat), GeoEncodingUtils.decodeLongitude(lon));
    }

    static final class GeoPointScriptSortedNumericDocValues extends AbstractSortedNumericDocValues {

        private final GeoPointFieldScript script;
        private int cursor;

        GeoPointScriptSortedNumericDocValues(GeoPointFieldScript script) {
            this.script = script;
        }

        @Override
        public long nextValue() {
            return script.values()[cursor++];
        }

        @Override
        public int docValueCount() {
            return script.count();
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
    }
}
