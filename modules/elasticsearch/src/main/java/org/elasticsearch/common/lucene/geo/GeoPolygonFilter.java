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

package org.elasticsearch.common.lucene.geo;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.lucene.docset.GetDocSet;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class GeoPolygonFilter extends Filter {

    private final Point[] points;

    private final String latFieldName;

    private final String lonFieldName;

    private final FieldDataType fieldDataType;

    private final FieldDataCache fieldDataCache;

    public GeoPolygonFilter(Point[] points, String latFieldName, String lonFieldName, FieldDataType fieldDataType, FieldDataCache fieldDataCache) {
        this.points = points;
        this.latFieldName = latFieldName;
        this.lonFieldName = lonFieldName;
        this.fieldDataType = fieldDataType;
        this.fieldDataCache = fieldDataCache;
    }

    public Point[] points() {
        return points;
    }

    public String latFieldName() {
        return latFieldName;
    }

    public String lonFieldName() {
        return lonFieldName;
    }

    @Override public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final NumericFieldData latFieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, latFieldName);
        final NumericFieldData lonFieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, lonFieldName);

        return new GetDocSet(reader.maxDoc()) {
            @Override public boolean get(int doc) throws IOException {
                if (!latFieldData.hasValue(doc) || !lonFieldData.hasValue(doc)) {
                    return false;
                }

                if (latFieldData.multiValued()) {
                    double[] lats = latFieldData.doubleValues(doc);
                    double[] lons = latFieldData.doubleValues(doc);
                    for (int i = 0; i < lats.length; i++) {
                        if (pointInPolygon(points, lats[i], lons[i])) {
                            return true;
                        }
                    }
                } else {
                    double lat = latFieldData.doubleValue(doc);
                    double lon = lonFieldData.doubleValue(doc);
                    return pointInPolygon(points, lat, lon);
                }
                return false;
            }
        };
    }

    private static boolean pointInPolygon(Point[] points, double lat, double lon) {
        int i;
        int j = points.length - 1;
        boolean inPoly = false;

        for (i = 0; i < points.length; i++) {
            if (points[i].lon < lon && points[j].lon >= lon
                    || points[j].lon < lon && points[i].lon >= lon) {
                if (points[i].lat + (lon - points[i].lon) /
                        (points[j].lon - points[i].lon) * (points[j].lat - points[i].lat) < lat) {
                    inPoly = !inPoly;
                }
            }
            j = i;
        }
        return inPoly;
    }

    public static class Point {
        public double lat;
        public double lon;

        public Point() {
        }

        public Point(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
        }
    }
}
