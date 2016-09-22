/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

final class LatLonPointDVAtomicFieldData extends AbstractAtomicGeoPointFieldData {
    private final SortedNumericDocValues values;

    LatLonPointDVAtomicFieldData(SortedNumericDocValues values) {
        super();
        this.values = values;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by lucene
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public void close() {
        // noop
    }

    @Override
    public MultiGeoPointValues getGeoPointValues() {
        return new MultiGeoPointValues() {
            GeoPoint[] points = new GeoPoint[0];
            private int count = 0;

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
                count = values.count();
                if (count > points.length) {
                    final int previousLength = points.length;
                    points = Arrays.copyOf(points, ArrayUtil.oversize(count, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
                    for (int i = previousLength; i < points.length; ++i) {
                        points[i] = new GeoPoint(Double.NaN, Double.NaN);
                    }
                }
                long encoded;
                for (int i=0; i<count; ++i) {
                    encoded = values.valueAt(i);
                    points[i].reset(GeoEncodingUtils.decodeLatitude((int)(encoded >>> 32)), GeoEncodingUtils.decodeLongitude((int)encoded));
                }
            }

            @Override
            public int count() {
                return count;
            }

            @Override
            public GeoPoint valueAt(int index) {
                return points[index];
            }
        };
    }
}
