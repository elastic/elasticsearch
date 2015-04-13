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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

final class GeoPointBinaryDVAtomicFieldData extends AbstractAtomicGeoPointFieldData {

    private static final int COORDINATE_SIZE = 8; // number of bytes per coordinate
    private static final int GEOPOINT_SIZE = COORDINATE_SIZE * 2; // lat + lon

    private final BinaryDocValues values;

    GeoPointBinaryDVAtomicFieldData(BinaryDocValues values) {
        super();
        this.values = values;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by Lucene
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public MultiGeoPointValues getGeoPointValues() {
        return new MultiGeoPointValues() {

            int count;
            GeoPoint[] points = new GeoPoint[0];

            @Override
            public void setDocument(int docId) {
                final BytesRef bytes = values.get(docId);
                assert bytes.length % GEOPOINT_SIZE == 0;
                count = (bytes.length >>> 4);
                if (count > points.length) {
                    final int previousLength = points.length;
                    points = Arrays.copyOf(points, ArrayUtil.oversize(count, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
                    for (int i = previousLength; i < points.length; ++i) {
                        points[i] = new GeoPoint();
                    }
                }
                for (int i = 0; i < count; ++i) {
                    final double lat = ByteUtils.readDoubleLE(bytes.bytes, bytes.offset + i * GEOPOINT_SIZE);
                    final double lon = ByteUtils.readDoubleLE(bytes.bytes, bytes.offset + i * GEOPOINT_SIZE + COORDINATE_SIZE);
                    points[i].reset(lat, lon);
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
