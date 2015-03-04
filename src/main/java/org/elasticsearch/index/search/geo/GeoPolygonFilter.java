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

package org.elasticsearch.index.search.geo;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocValuesDocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

/**
 *
 */
public class GeoPolygonFilter extends Filter {

    private final GeoPoint[] points;

    private final IndexGeoPointFieldData indexFieldData;

    public GeoPolygonFilter(IndexGeoPointFieldData indexFieldData, GeoPoint...points) {
        this.points = points;
        this.indexFieldData = indexFieldData;
    }

    public GeoPoint[] points() {
        return points;
    }

    public String fieldName() {
        return indexFieldData.getFieldNames().indexName();
    }

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptedDocs) throws IOException {
        final MultiGeoPointValues values = indexFieldData.load(context).getGeoPointValues();
        return new GeoPolygonDocIdSet(context.reader().maxDoc(), acceptedDocs, values, points);
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder("GeoPolygonFilter(");
        sb.append(indexFieldData.getFieldNames().indexName());
        sb.append(", ").append(Arrays.toString(points)).append(')');
        return sb.toString();
    }

    public static class GeoPolygonDocIdSet extends DocValuesDocIdSet {
        private final MultiGeoPointValues values;
        private final GeoPoint[] points;

        public GeoPolygonDocIdSet(int maxDoc, @Nullable Bits acceptDocs, MultiGeoPointValues values, GeoPoint[] points) {
            super(maxDoc, acceptDocs);
            this.values = values;
            this.points = points;
        }

        @Override
        protected boolean matchDoc(int doc) {
            values.setDocument(doc);
            final int length = values.count();
            for (int i = 0; i < length; i++) {
                GeoPoint point = values.valueAt(i);
                if (pointInPolygon(points, point.lat(), point.lon())) {
                    return true;
                }
            }
            return false;
        }

        private static boolean pointInPolygon(GeoPoint[] points, double lat, double lon) {
            boolean inPoly = false;

            for (int i = 1; i < points.length; i++) {
                if (points[i].lon() < lon && points[i-1].lon() >= lon
                        || points[i-1].lon() < lon && points[i].lon() >= lon) {
                    if (points[i].lat() + (lon - points[i].lon()) /
                            (points[i-1].lon() - points[i].lon()) * (points[i-1].lat() - points[i].lat()) < lat) {
                        inPoly = !inPoly;
                    }
                }
            }
            return inPoly;
        }
    }
}
