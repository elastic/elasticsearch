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

import org.apache.commons.lang3.Validate;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocValuesDocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.docset.AndDocIdSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class GeoPolygonFilter extends Filter {

    private final GeoPoint[] points;
    private final Filter boundingBoxFilter;

    private final IndexGeoPointFieldData indexFieldData;

    public GeoPolygonFilter(IndexGeoPointFieldData indexFieldData, GeoPointFieldMapper mapper,
                            GeoPoint[] points, String optimizeBbox) {
        this.indexFieldData = indexFieldData;
        this.points = points;

        if (optimizeBbox != null && !"none".equals(optimizeBbox)) {
            BoundingBox boundingBox = getBoundingBox(points);
            if ("memory".equals(optimizeBbox)) {
                boundingBoxFilter = new InMemoryGeoBoundingBoxFilter(boundingBox.topLeft(), boundingBox.bottomRight(), indexFieldData);
            } else if ("indexed".equals(optimizeBbox)) {
                boundingBoxFilter = IndexedGeoBoundingBoxFilter.create(boundingBox.topLeft(), boundingBox.bottomRight(), mapper);
            } else {
                throw new ElasticsearchIllegalArgumentException("type [" + optimizeBbox + "] for bounding box optimization not supported");
            }
        } else {
            boundingBoxFilter = null;
        }
    }

    public GeoPoint[] points() {
        return points;
    }

    public String fieldName() {
        return indexFieldData.getFieldNames().indexName();
    }

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptedDocs) throws IOException {
        MultiGeoPointValues values = indexFieldData.load(context).getGeoPointValues();
        GeoPolygonDocIdSet polygonDocSet = new GeoPolygonDocIdSet(context.reader().maxDoc(), acceptedDocs, values, points);

        if (boundingBoxFilter == null) {
            return polygonDocSet;
        } else {
            DocIdSet boundingBoxDocSet = boundingBoxFilter.getDocIdSet(context, acceptedDocs);
            if (DocIdSets.isEmpty(boundingBoxDocSet)) { // Necessary to avoid a NullPointerException.
                return polygonDocSet;
            }
            return new AndDocIdSet(new DocIdSet[]{boundingBoxDocSet, polygonDocSet});
        }
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

    private BoundingBox getBoundingBox(GeoPoint[] points) {
        Validate.isTrue(points.length > 0);
        double maxLat = points[0].lat();
        double minLat = points[0].lat();
        double maxLon = points[0].lon();
        double minLon = points[0].lon();

        for (int i = 1; i < points.length; i++) {
            maxLat = Math.max(maxLat, points[i].lat());
            minLat = Math.min(minLat, points[i].lat());
            maxLon = Math.max(maxLon, points[i].lon());
            minLon = Math.min(minLon, points[i].lon());
        }

        GeoPoint topLeft = new GeoPoint(maxLat, minLon);
        GeoPoint bottomRight = new GeoPoint(minLat, maxLon);
        return new BoundingBox(topLeft, bottomRight);
    }

    private static class BoundingBox {
        private final GeoPoint topLeft;
        private final GeoPoint bottomRight;

        public BoundingBox(GeoPoint topLeft, GeoPoint bottomRight) {
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        public GeoPoint topLeft() {
            return topLeft;
        }

        public GeoPoint bottomRight() {
            return bottomRight;
        }
    }
}
