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

import org.apache.lucene.index.IndexReader;
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
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;

import java.io.IOException;

/**
 *
 */
public class GeoDistanceRangeQuery extends Query {

    private final double lat;
    private final double lon;

    private final double inclusiveLowerPoint; // in meters
    private final double inclusiveUpperPoint; // in meters

    private final GeoDistance geoDistance;
    private final GeoDistance.FixedSourceDistance fixedSourceDistance;
    private GeoDistance.DistanceBoundingCheck distanceBoundingCheck;
    private final Query boundingBoxFilter;

    private final IndexGeoPointFieldData indexFieldData;

    public GeoDistanceRangeQuery(GeoPoint point, Double lowerVal, Double upperVal, boolean includeLower, boolean includeUpper, GeoDistance geoDistance, GeoPointFieldMapper.GeoPointFieldType fieldType, IndexGeoPointFieldData indexFieldData,
                                  String optimizeBbox) {
        this.lat = point.lat();
        this.lon = point.lon();
        this.geoDistance = geoDistance;
        this.indexFieldData = indexFieldData;

        this.fixedSourceDistance = geoDistance.fixedSourceDistance(lat, lon, DistanceUnit.DEFAULT);

        if (lowerVal != null) {
            double f = lowerVal.doubleValue();
            long i = NumericUtils.doubleToSortableLong(f);
            inclusiveLowerPoint = NumericUtils.sortableLongToDouble(includeLower ? i : (i + 1L));
        } else {
            inclusiveLowerPoint = Double.NEGATIVE_INFINITY;
        }
        if (upperVal != null) {
            double f = upperVal.doubleValue();
            long i = NumericUtils.doubleToSortableLong(f);
            inclusiveUpperPoint = NumericUtils.sortableLongToDouble(includeUpper ? i : (i - 1L));
        } else {
            inclusiveUpperPoint = Double.POSITIVE_INFINITY;
            // we disable bounding box in this case, since the upper point is all and we create bounding box up to the
            // upper point it will effectively include all
            // TODO we can create a bounding box up to from and "not" it
            optimizeBbox = null;
        }

        if (optimizeBbox != null && !"none".equals(optimizeBbox)) {
            distanceBoundingCheck = GeoDistance.distanceBoundingCheck(lat, lon, inclusiveUpperPoint, DistanceUnit.DEFAULT);
            if ("memory".equals(optimizeBbox)) {
                boundingBoxFilter = null;
            } else if ("indexed".equals(optimizeBbox)) {
                boundingBoxFilter = IndexedGeoBoundingBoxQuery.create(distanceBoundingCheck.topLeft(), distanceBoundingCheck.bottomRight(), fieldType);
                distanceBoundingCheck = GeoDistance.ALWAYS_INSTANCE; // fine, we do the bounding box check using the filter
            } else {
                throw new IllegalArgumentException("type [" + optimizeBbox + "] for bounding box optimization not supported");
            }
        } else {
            distanceBoundingCheck = GeoDistance.ALWAYS_INSTANCE;
            boundingBoxFilter = null;
        }
    }

    public double lat() {
        return lat;
    }

    public double lon() {
        return lon;
    }

    public GeoDistance geoDistance() {
        return geoDistance;
    }

    public double minInclusiveDistance() {
        return inclusiveLowerPoint;
    }

    public double maxInclusiveDistance() {
        return inclusiveUpperPoint;
    }

    public String fieldName() {
        return indexFieldData.getFieldNames().indexName();
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        return super.rewrite(reader);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        final Weight boundingBoxWeight;
        if (boundingBoxFilter != null) {
            boundingBoxWeight = searcher.createNormalizedWeight(boundingBoxFilter, false);
        } else {
            boundingBoxWeight = null;
        }
        return new ConstantScoreWeight(this) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final DocIdSetIterator approximation;
                if (boundingBoxWeight != null) {
                    approximation = boundingBoxWeight.scorer(context);
                } else {
                    approximation = DocIdSetIterator.all(context.reader().maxDoc());
                }
                if (approximation == null) {
                    // if the approximation does not match anything, we're done
                    return null;
                }
                final MultiGeoPointValues values = indexFieldData.load(context).getGeoPointValues();
                final TwoPhaseIterator twoPhaseIterator = new TwoPhaseIterator(approximation) {
                    @Override
                    public boolean matches() throws IOException {
                        final int doc = approximation.docID();
                        values.setDocument(doc);
                        final int length = values.count();
                        for (int i = 0; i < length; i++) {
                            GeoPoint point = values.valueAt(i);
                            if (distanceBoundingCheck.isWithin(point.lat(), point.lon())) {
                                double d = fixedSourceDistance.calculate(point.lat(), point.lon());
                                if (d >= inclusiveLowerPoint && d <= inclusiveUpperPoint) {
                                    return true;
                                }
                            }
                        }
                        return false;
                    }
                };
                return new ConstantScoreScorer(this, score(), twoPhaseIterator);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;

        GeoDistanceRangeQuery filter = (GeoDistanceRangeQuery) o;

        if (Double.compare(filter.inclusiveLowerPoint, inclusiveLowerPoint) != 0) return false;
        if (Double.compare(filter.inclusiveUpperPoint, inclusiveUpperPoint) != 0) return false;
        if (Double.compare(filter.lat, lat) != 0) return false;
        if (Double.compare(filter.lon, lon) != 0) return false;
        if (!indexFieldData.getFieldNames().indexName().equals(filter.indexFieldData.getFieldNames().indexName()))
            return false;
        if (geoDistance != filter.geoDistance) return false;

        return true;
    }

    @Override
    public String toString(String field) {
        return "GeoDistanceRangeFilter(" + indexFieldData.getFieldNames().indexName() + ", " + geoDistance + ", [" + inclusiveLowerPoint + " - " + inclusiveUpperPoint + "], " + lat + ", " + lon + ")";
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        temp = lat != +0.0d ? Double.doubleToLongBits(lat) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = lon != +0.0d ? Double.doubleToLongBits(lon) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = inclusiveLowerPoint != +0.0d ? Double.doubleToLongBits(inclusiveLowerPoint) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = inclusiveUpperPoint != +0.0d ? Double.doubleToLongBits(inclusiveUpperPoint) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (geoDistance != null ? geoDistance.hashCode() : 0);
        result = 31 * result + indexFieldData.getFieldNames().indexName().hashCode();
        return result;
    }

}
