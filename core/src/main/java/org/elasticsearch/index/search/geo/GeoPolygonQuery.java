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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RandomAccessWeight;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class GeoPolygonQuery extends Query {

    private final GeoPoint[] points;

    private final IndexGeoPointFieldData indexFieldData;

    public GeoPolygonQuery(IndexGeoPointFieldData indexFieldData, GeoPoint...points) {
        this.points = points;
        this.indexFieldData = indexFieldData;
    }

    public GeoPoint[] points() {
        return points;
    }

    public String fieldName() {
        return indexFieldData.getFieldName();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new RandomAccessWeight(this) {
            @Override
            protected Bits getMatchingDocs(LeafReaderContext context) throws IOException {
                final int maxDoc = context.reader().maxDoc();
                final MultiGeoPointValues values = indexFieldData.load(context).getGeoPointValues();
                return new Bits() {

                    private boolean pointInPolygon(GeoPoint[] points, double lat, double lon) {
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

                    @Override
                    public boolean get(int doc) {
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

                    @Override
                    public int length() {
                        return maxDoc;
                    }

                };
            }
        };
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder("GeoPolygonQuery(");
        sb.append(indexFieldData.getFieldName());
        sb.append(", ").append(Arrays.toString(points)).append(')');
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        GeoPolygonQuery that = (GeoPolygonQuery) obj;
        return indexFieldData.getFieldName().equals(that.indexFieldData.getFieldName())
                && Arrays.equals(points, that.points);
    }

    @Override
    public int hashCode() {
        int h = super.hashCode();
        h = 31 * h + indexFieldData.getFieldName().hashCode();
        h = 31 * h + Arrays.hashCode(points);
        return h;
    }
}
