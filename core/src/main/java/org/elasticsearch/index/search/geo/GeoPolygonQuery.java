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

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.Validate;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper.GeoPointFieldType;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class GeoPolygonQuery extends Query {

    private final GeoPoint[] points;
    private final IndexGeoPointFieldData indexFieldData;
    private final Query boundingBoxQuery;

    public GeoPolygonQuery(GeoPoint[] points, GeoPointFieldType fieldType,
            IndexGeoPointFieldData indexFieldData, String optimizeBbox) {
        this.points = points;
        this.indexFieldData = indexFieldData;
        this.boundingBoxQuery = getBoundingBoxQuery(fieldType, indexFieldData, optimizeBbox);
    }

    private Query getBoundingBoxQuery(GeoPointFieldType fieldType, IndexGeoPointFieldData indexFieldData, String optimizeBbox) {
        if (optimizeBbox != null && !"none".equals(optimizeBbox)) {
            BoundingBox boundingBox = determineBoundingBox(points);
            if ("memory".equals(optimizeBbox)) {
                return new InMemoryGeoBoundingBoxQuery(boundingBox.topLeft(), boundingBox.bottomRight(), indexFieldData);
            } else if ("indexed".equals(optimizeBbox)) {
                return IndexedGeoBoundingBoxQuery.create(boundingBox.topLeft(), boundingBox.bottomRight(), fieldType);
            } else {
                throw new IllegalArgumentException("type [" + optimizeBbox + "] for bounding box optimization not supported");
            }
        } else {
            return null;
        }
    }

    public GeoPoint[] points() {
        return points;
    }

    public String fieldName() {
        return indexFieldData.getFieldNames().indexName();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        final Weight boundingBoxWeight = boundingBoxQuery != null ?
                searcher.createNormalizedWeight(boundingBoxQuery, false) :
                null;

        return new ConstantScoreWeight(this) {
            @Override
            public Scorer scorer(LeafReaderContext context, final Bits acceptDocs) throws IOException {
                DocIdSetIterator approximation = boundingBoxWeight != null ?
                        boundingBoxWeight.scorer(context, null) :
                        DocIdSetIterator.all(context.reader().maxDoc());

                if (approximation == null) {
                    // If the approximation is empty, no documents can match.
                    return null;
                }

                final MultiGeoPointValues values = indexFieldData.load(context).getGeoPointValues();
                final TwoPhaseIterator twoPhaseIterator = new TwoPhaseIterator(approximation) {
                    @Override
                    public boolean matches() throws IOException {
                        final int doc = approximation.docID();
                        if (acceptDocs != null && acceptDocs.get(doc) == false) {
                            return false;
                        }

                        values.setDocument(doc);
                        for (int i = 0; i < values.count(); i++) {
                            GeoPoint point = values.valueAt(i);
                            if (containedInPolygon(point.getLat(), point.getLon())) {
                                return true;
                            }
                        }
                        return false;
                    }
                };
                return new ConstantScoreScorer(this, score(), twoPhaseIterator);
            }
        };
    }

    @VisibleForTesting
    protected boolean containedInPolygon(double lat, double lon) {
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
    public String toString(String field) {
        StringBuilder sb = new StringBuilder("GeoPolygonFilter(");
        sb.append(indexFieldData.getFieldNames().indexName());
        sb.append(", ").append(Arrays.toString(points)).append(')');
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        GeoPolygonQuery that = (GeoPolygonQuery) obj;
        return indexFieldData.getFieldNames().indexName().equals(that.indexFieldData.getFieldNames().indexName())
                && Arrays.equals(points, that.points);
    }

    @Override
    public int hashCode() {
        int h = super.hashCode();
        h = 31 * h + indexFieldData.getFieldNames().indexName().hashCode();
        h = 31 * h + Arrays.hashCode(points);
        return h;
    }

    public BoundingBox determineBoundingBox(GeoPoint[] points) {
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
