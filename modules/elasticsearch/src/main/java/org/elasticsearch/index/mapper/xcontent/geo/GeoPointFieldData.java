/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.xcontent.geo;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.support.FieldDataLoader;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author kimchy (shay.banon)
 */
public abstract class GeoPointFieldData extends FieldData<GeoPointDocFieldData> {

    public static final GeoPoint[] EMPTY_ARRAY = new GeoPoint[0];

    protected final GeoPoint[] values;

    protected GeoPointFieldData(String fieldName, GeoPoint[] values) {
        super(fieldName);
        this.values = values;
    }

    abstract public GeoPoint value(int docId);

    abstract public GeoPoint[] values(int docId);

    @Override public GeoPointDocFieldData docFieldData(int docId) {
        return super.docFieldData(docId);
    }

    @Override public String stringValue(int docId) {
        return value(docId).geohash();
    }

    @Override protected GeoPointDocFieldData createFieldData() {
        return new GeoPointDocFieldData(this);
    }

    @Override public FieldDataType type() {
        return GeoPointFieldDataType.TYPE;
    }

    @Override public void forEachValue(StringValueProc proc) {
        for (int i = 1; i < values.length; i++) {
            proc.onValue(values[i].geohash());
        }
    }

    public void forEachValue(ValueProc proc) {
        for (int i = 1; i < values.length; i++) {
            proc.onValue(values[i]);
        }
    }

    public static interface ValueProc {
        void onValue(GeoPoint value);
    }

    public static GeoPointFieldData load(IndexReader reader, String field) throws IOException {
        return FieldDataLoader.load(reader, field, new StringTypeLoader());
    }

    static class StringTypeLoader extends FieldDataLoader.FreqsTypeLoader<GeoPointFieldData> {

        private final ArrayList<GeoPoint> terms = new ArrayList<GeoPoint>();

        StringTypeLoader() {
            super();
            // the first one indicates null value
            terms.add(null);
        }

        @Override public void collectTerm(String term) {
            int comma = term.indexOf(',');
            double lat = Double.parseDouble(term.substring(0, comma));
            double lon = Double.parseDouble(term.substring(comma + 1));
            terms.add(new GeoPoint(lat, lon));
        }

        @Override public GeoPointFieldData buildSingleValue(String field, int[] order) {
            return new SingleValueGeoPointFieldData(field, order, terms.toArray(new GeoPoint[terms.size()]));
        }

        @Override public GeoPointFieldData buildMultiValue(String field, int[][] order) {
            return new MultiValueGeoPointFieldData(field, order, terms.toArray(new GeoPoint[terms.size()]));
        }
    }
}
