/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.script.GeoPointFieldScript;

public final class GeoPointScriptDocValues extends AbstractSortedNumericDocValues {
    private final GeoPointFieldScript script;
    private int cursor;

    GeoPointScriptDocValues(GeoPointFieldScript script) {
        this.script = script;
    }

    @Override
    public boolean advanceExact(int docId) {
        script.runForDoc(docId);
        if (script.count() == 0) {
            return false;
        }
        new IntroSorter() {
            private int pivot;

            @Override
            protected void setPivot(int i) {
                this.pivot = i;
            }

            @Override
            protected void swap(int i, int j) {
                double tmp = script.lats()[i];
                script.lats()[i] = script.lats()[j];
                script.lats()[j] = tmp;
                tmp = script.lons()[i];
                script.lons()[i] = script.lons()[j];
                script.lons()[j] = tmp;
            }

            @Override
            protected int comparePivot(int j) {
                int cmp = Double.compare(script.lats()[pivot], script.lats()[j]);
                if (cmp != 0) {
                    return cmp;
                }
                return Double.compare(script.lons()[pivot], script.lons()[j]);
            }
        }.sort(0, script.count());
        cursor = 0;
        return true;
    }

    @Override
    public int docValueCount() {
        return script.count();
    }

    @Override
    public long nextValue() {
        int lat = GeoEncodingUtils.encodeLatitude(script.lats()[cursor]);
        int lon = GeoEncodingUtils.encodeLongitude(script.lons()[cursor++]);
        return Long.valueOf((((long) lat) << 32) | (lon & 0xFFFFFFFFL));
    }
}
