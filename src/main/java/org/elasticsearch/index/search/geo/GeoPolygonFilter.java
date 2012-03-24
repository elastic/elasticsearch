/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.search.geo;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.lucene.docset.GetDocSet;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.mapper.geo.GeoPointFieldData;
import org.elasticsearch.index.mapper.geo.GeoPointFieldDataType;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class GeoPolygonFilter extends Filter {

    private final Point[] points;

    private final String fieldName;

    private final FieldDataCache fieldDataCache;

    public GeoPolygonFilter(Point[] points, String fieldName, FieldDataCache fieldDataCache) {
        this.points = points;
        this.fieldName = fieldName;
        this.fieldDataCache = fieldDataCache;
    }

    public Point[] points() {
        return points;
    }

    public String fieldName() {
        return this.fieldName;
    }

    @Override
    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final GeoPointFieldData fieldData = (GeoPointFieldData) fieldDataCache.cache(GeoPointFieldDataType.TYPE, reader, fieldName);
        return new GeoPolygonDocSet(reader.maxDoc(), fieldData, points);
    }

    @Override
    public String toString() {
        return "GeoPolygonFilter(" + fieldName + ", "  + Arrays.toString(points) + ")";
    }

    public static class GeoPolygonDocSet extends GetDocSet {
        private final GeoPointFieldData fieldData;
        private final Point[] points;

        public GeoPolygonDocSet(int maxDoc, GeoPointFieldData fieldData, Point[] points) {
            super(maxDoc);
            this.fieldData = fieldData;
            this.points = points;
        }

        @Override
        public boolean isCacheable() {
            // not cacheable for several reasons:
            // 1. It is only relevant when _cache is set to true, and then, we really want to create in mem bitset
            // 2. Its already fast without in mem bitset, since it works with field data
            return false;
        }

        @Override
        public boolean get(int doc) {
            if (!fieldData.hasValue(doc)) {
                return false;
            }

            if (fieldData.multiValued()) {
                double[] lats = fieldData.latValues(doc);
                double[] lons = fieldData.lonValues(doc);
                for (int i = 0; i < lats.length; i++) {
                    if (pointInPolygon(points, lats[i], lons[i])) {
                        return true;
                    }
                }
            } else {
                double lat = fieldData.latValue(doc);
                double lon = fieldData.lonValue(doc);
                return pointInPolygon(points, lat, lon);
            }
            return false;
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
    }
}
