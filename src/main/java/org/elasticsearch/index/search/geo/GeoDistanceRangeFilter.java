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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocValuesDocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.docset.AndDocIdSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;

/**
 *
 */
public class GeoDistanceRangeFilter extends Filter {

    private final double lat;
    private final double lon;

    private final double inclusiveLowerPoint; // in meters
    private final double inclusiveUpperPoint; // in meters

    private final GeoDistance geoDistance;
    private final GeoDistance.FixedSourceDistance fixedSourceDistance;
    private GeoDistance.DistanceBoundingCheck distanceBoundingCheck;
    private final Filter boundingBoxFilter;

    private final IndexGeoPointFieldData indexFieldData;

    public GeoDistanceRangeFilter(GeoPoint point, Double lowerVal, Double upperVal, boolean includeLower, boolean includeUpper, GeoDistance geoDistance, GeoPointFieldMapper mapper, IndexGeoPointFieldData indexFieldData,
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
                boundingBoxFilter = IndexedGeoBoundingBoxFilter.create(distanceBoundingCheck.topLeft(), distanceBoundingCheck.bottomRight(), mapper);
                distanceBoundingCheck = GeoDistance.ALWAYS_INSTANCE; // fine, we do the bounding box check using the filter
            } else {
                throw new ElasticsearchIllegalArgumentException("type [" + optimizeBbox + "] for bounding box optimization not supported");
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

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptedDocs) throws IOException {
        DocIdSet boundingBoxDocSet = null;
        if (boundingBoxFilter != null) {
            boundingBoxDocSet = boundingBoxFilter.getDocIdSet(context, null);
            if (DocIdSets.isEmpty(boundingBoxDocSet)) {
                return null;
            }
        }
        MultiGeoPointValues values = indexFieldData.load(context).getGeoPointValues();
        GeoDistanceRangeDocSet distDocSet = new GeoDistanceRangeDocSet(context.reader().maxDoc(), acceptedDocs, values, fixedSourceDistance, distanceBoundingCheck, inclusiveLowerPoint, inclusiveUpperPoint);
        if (boundingBoxDocSet == null) {
            return distDocSet;
        } else {
            return new AndDocIdSet(new DocIdSet[]{boundingBoxDocSet, distDocSet});
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;

        GeoDistanceRangeFilter filter = (GeoDistanceRangeFilter) o;

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

    public static class GeoDistanceRangeDocSet extends DocValuesDocIdSet {

        private final MultiGeoPointValues values;
        private final GeoDistance.FixedSourceDistance fixedSourceDistance;
        private final GeoDistance.DistanceBoundingCheck distanceBoundingCheck;
        private final double inclusiveLowerPoint; // in miles
        private final double inclusiveUpperPoint; // in miles

        public GeoDistanceRangeDocSet(int maxDoc, @Nullable Bits acceptDocs, MultiGeoPointValues values, GeoDistance.FixedSourceDistance fixedSourceDistance, GeoDistance.DistanceBoundingCheck distanceBoundingCheck,
                                      double inclusiveLowerPoint, double inclusiveUpperPoint) {
            super(maxDoc, acceptDocs);
            this.values = values;
            this.fixedSourceDistance = fixedSourceDistance;
            this.distanceBoundingCheck = distanceBoundingCheck;
            this.inclusiveLowerPoint = inclusiveLowerPoint;
            this.inclusiveUpperPoint = inclusiveUpperPoint;
        }

        @Override
        protected boolean matchDoc(int doc) {
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
    }
}
