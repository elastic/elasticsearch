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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.test.ESTestCase;

public class LatLonPointDVAtomicFieldDataTests extends ESTestCase {

    public void testSingleValued() {
        final NumericDocValues encodedValues = new NumericDocValues() {
            @Override
            public long get(int docID) {
                if (docID == 0) {
                    return 42;
                } else if (docID == 1) {
                    return 0;
                } else {
                    throw new IndexOutOfBoundsException();
                }
            }
        };
        Bits bits = new Bits.MatchAllBits(2);
        LatLonPointDVAtomicFieldData fd = new LatLonPointDVAtomicFieldData(DocValues.singleton(encodedValues, bits));
        MultiGeoPointValues values = fd.getGeoPointValues();
        values.setDocument(0);
        assertEquals(1, values.count());
        GeoPoint point = values.valueAt(0);
        assertEquals(GeoEncodingUtils.decodeLatitude((int)(42L >>> 32)), point.lat(), 0d);
        assertEquals(GeoEncodingUtils.decodeLongitude((int)(42L)), point.lon(), 0d);
        values.setDocument(1);
        assertEquals(1, values.count());
        point = values.valueAt(0);
        assertEquals(GeoEncodingUtils.decodeLatitude((int)(0L >>> 32)), point.lat(), 0d);
        assertEquals(GeoEncodingUtils.decodeLongitude((int)(0L)), point.lon(), 0d);

        FixedBitSet bits2 = new FixedBitSet(2);
        bits2.set(0);
        fd = new LatLonPointDVAtomicFieldData(DocValues.singleton(encodedValues, bits2));
        values = fd.getGeoPointValues();
        values.setDocument(0);
        assertEquals(1, values.count());
        point = values.valueAt(0);
        assertEquals(GeoEncodingUtils.decodeLatitude((int)(42L >>> 32)), point.lat(), 0d);
        assertEquals(GeoEncodingUtils.decodeLongitude((int)(42L)), point.lon(), 0d);
        values.setDocument(1);
        assertEquals(0, values.count());
    }

    public void testMultiValued() {
        final SortedNumericDocValues encodedValues = new SortedNumericDocValues() {
            int doc;

            @Override
            public void setDocument(int doc) {
                this.doc = doc;
            }

            @Override
            public int count() {
                if (doc == 0) {
                    return 2;
                } else {
                    return 0;
                }
            }

            @Override
            public long valueAt(int index) {
                if (doc == 0 && index == 0) {
                    return 42;
                } else if (doc == 0 && index == 1) {
                    return 62;
                } else {
                    throw new IndexOutOfBoundsException();
                }
            }
        };
        LatLonPointDVAtomicFieldData fd = new LatLonPointDVAtomicFieldData(encodedValues);
        MultiGeoPointValues values = fd.getGeoPointValues();
        values.setDocument(0);
        assertEquals(2, values.count());
        GeoPoint point = values.valueAt(0);
        assertEquals(GeoEncodingUtils.decodeLatitude((int)(42L >>> 32)), point.lat(), 0d);
        assertEquals(GeoEncodingUtils.decodeLongitude((int)(42L)), point.lon(), 0d);
        point = values.valueAt(1);
        assertEquals(GeoEncodingUtils.decodeLatitude((int)(62L >>> 32)), point.lat(), 0d);
        assertEquals(GeoEncodingUtils.decodeLongitude((int)(62L)), point.lon(), 0d);
        values.setDocument(1);
        assertEquals(0, values.count());
    }
}
