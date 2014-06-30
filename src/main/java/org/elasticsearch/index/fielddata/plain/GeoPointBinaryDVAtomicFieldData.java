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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.fielddata.AtomicGeoPointFieldData;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;

final class GeoPointBinaryDVAtomicFieldData extends AtomicGeoPointFieldData<ScriptDocValues> {

    private final BinaryDocValues values;

    GeoPointBinaryDVAtomicFieldData(BinaryDocValues values) {
        super();
        this.values = values;
    }

    @Override
    public long ramBytesUsed() {
        return -1; // not exposed by Lucene
    }

    @Override
    public ScriptDocValues getScriptValues() {
        return new ScriptDocValues.GeoPoints(getGeoPointValues());
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public GeoPointValues getGeoPointValues() {
        return new GeoPointValues(true) {

            BytesRef bytes;
            int i = Integer.MAX_VALUE;
            int valueCount = 0;
            final GeoPoint point = new GeoPoint();

            @Override
            public int setDocument(int docId) {
                bytes = values.get(docId);
                assert bytes.length % 16 == 0;
                i = 0;
                return valueCount = (bytes.length >>> 4);
            }

            @Override
            public GeoPoint nextValue() {
                assert i < 2 * valueCount;
                final double lat = ByteUtils.readDoubleLE(bytes.bytes, bytes.offset + i++ * 8);
                final double lon = ByteUtils.readDoubleLE(bytes.bytes, bytes.offset + i++ * 8);
                return point.reset(lat, lon);
            }

        };
    }

}
