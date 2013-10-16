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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class GeoPolygonFilter extends Filter {

    private final GeoPoint[] points;

    private final IndexGeoPointFieldData indexFieldData;

    public GeoPolygonFilter(GeoPoint[] points, IndexGeoPointFieldData indexFieldData) {
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
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptedDocs) throws IOException {
        final GeoPointValues values = indexFieldData.load(context).getGeoPointValues();
        return new GeoPolygonDocIdSet(context.reader().maxDoc(), acceptedDocs, values, points);
    }

    @Override
    public String toString() {
        return "GeoPolygonFilter(" + indexFieldData.getFieldNames().indexName() + ", " + Arrays.toString(points) + ")";
    }

    public static class GeoPolygonDocIdSet extends MatchDocIdSet {
        private final GeoPointValues values;
        private final GeoPoint[] points;

        public GeoPolygonDocIdSet(int maxDoc, @Nullable Bits acceptDocs, GeoPointValues values, GeoPoint[] points) {
            super(maxDoc, acceptDocs);
            this.values = values;
            this.points = points;
        }

        @Override
        public boolean isCacheable() {
            return true;
        }

        @Override
        protected boolean matchDoc(int doc) {
            final int length = values.setDocument(doc);
            for (int i = 0; i < length; i++) {
                GeoPoint point = values.nextValue();
                if (pointInPolygon(points, point.lat(), point.lon())) {
                    return true;
                }
            }
            return false;
        }

        private static boolean pointInPolygon(GeoPoint[] points, double lat, double lon) {
            int i;
            int j = points.length - 1;
            boolean inPoly = false;

            for (i = 0; i < points.length; i++) {
                if (points[i].lon() < lon && points[j].lon() >= lon
                        || points[j].lon() < lon && points[i].lon() >= lon) {
                    if (points[i].lat() + (lon - points[i].lon()) /
                            (points[j].lon() - points[i].lon()) * (points[j].lat() - points[i].lat()) < lat) {
                        inPoly = !inPoly;
                    }
                }
                j = i;
            }
            return inPoly;
        }
    }
}
