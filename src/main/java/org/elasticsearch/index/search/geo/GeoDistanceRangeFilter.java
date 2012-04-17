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

import com.google.common.collect.ImmutableList;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.lucene.docset.AndDocSet;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.DocSets;
import org.elasticsearch.common.lucene.docset.GetDocSet;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.mapper.geo.GeoPointFieldData;
import org.elasticsearch.index.mapper.geo.GeoPointFieldDataType;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;

import java.io.IOException;

/**
 *
 */
public class GeoDistanceRangeFilter extends Filter {

    private final double lat;
    private final double lon;

    private final double inclusiveLowerPoint; // in miles
    private final double inclusiveUpperPoint; // in miles

    private final GeoDistance geoDistance;
    private final GeoDistance.FixedSourceDistance fixedSourceDistance;
    private GeoDistance.DistanceBoundingCheck distanceBoundingCheck;
    private final Filter boundingBoxFilter;

    private final String fieldName;

    private final FieldDataCache fieldDataCache;

    public GeoDistanceRangeFilter(double lat, double lon, Double lowerVal, Double upperVal, boolean includeLower, boolean includeUpper, GeoDistance geoDistance, String fieldName, GeoPointFieldMapper mapper, FieldDataCache fieldDataCache,
                                  String optimizeBbox) {
        this.lat = lat;
        this.lon = lon;
        this.geoDistance = geoDistance;
        this.fieldName = fieldName;
        this.fieldDataCache = fieldDataCache;

        this.fixedSourceDistance = geoDistance.fixedSourceDistance(lat, lon, DistanceUnit.MILES);

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
            distanceBoundingCheck = GeoDistance.distanceBoundingCheck(lat, lon, inclusiveUpperPoint, DistanceUnit.MILES);
            if ("memory".equals(optimizeBbox)) {
                boundingBoxFilter = null;
            } else if ("indexed".equals(optimizeBbox)) {
                boundingBoxFilter = IndexedGeoBoundingBoxFilter.create(distanceBoundingCheck.topLeft(), distanceBoundingCheck.bottomRight(), mapper);
                distanceBoundingCheck = GeoDistance.ALWAYS_INSTANCE; // fine, we do the bounding box check using the filter
            } else {
                throw new ElasticSearchIllegalArgumentException("type [" + optimizeBbox + "] for bounding box optimization not supported");
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

    public String fieldName() {
        return fieldName;
    }

    @Override
    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        DocSet boundingBoxDocSet = null;
        if (boundingBoxFilter != null) {
            DocIdSet docIdSet = boundingBoxFilter.getDocIdSet(reader);
            if (docIdSet == null) {
                return null;
            }
            boundingBoxDocSet = DocSets.convert(reader, docIdSet);
        }
        final GeoPointFieldData fieldData = (GeoPointFieldData) fieldDataCache.cache(GeoPointFieldDataType.TYPE, reader, fieldName);
        GeoDistanceRangeDocSet distDocSet = new GeoDistanceRangeDocSet(reader.maxDoc(), fieldData, fixedSourceDistance, distanceBoundingCheck, inclusiveLowerPoint, inclusiveUpperPoint);
        if (boundingBoxDocSet == null) {
            return distDocSet;
        } else {
            return new AndDocSet(ImmutableList.of(boundingBoxDocSet, distDocSet));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeoDistanceRangeFilter filter = (GeoDistanceRangeFilter) o;

        if (Double.compare(filter.inclusiveLowerPoint, inclusiveLowerPoint) != 0) return false;
        if (Double.compare(filter.inclusiveUpperPoint, inclusiveUpperPoint) != 0) return false;
        if (Double.compare(filter.lat, lat) != 0) return false;
        if (Double.compare(filter.lon, lon) != 0) return false;
        if (fieldName != null ? !fieldName.equals(filter.fieldName) : filter.fieldName != null) return false;
        if (geoDistance != filter.geoDistance) return false;

        return true;
    }

    @Override
    public String toString() {
        return "GeoDistanceRangeFilter(" + fieldName + ", " + geoDistance + ", [" + inclusiveLowerPoint + " - " + inclusiveUpperPoint + "], " + lat + ", " + lon + ")";
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = lat != +0.0d ? Double.doubleToLongBits(lat) : 0L;
        result = (int) (temp ^ (temp >>> 32));
        temp = lon != +0.0d ? Double.doubleToLongBits(lon) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = inclusiveLowerPoint != +0.0d ? Double.doubleToLongBits(inclusiveLowerPoint) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = inclusiveUpperPoint != +0.0d ? Double.doubleToLongBits(inclusiveUpperPoint) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (geoDistance != null ? geoDistance.hashCode() : 0);
        result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
        return result;
    }

    public static class GeoDistanceRangeDocSet extends GetDocSet {

        private final GeoPointFieldData fieldData;
        private final GeoDistance.FixedSourceDistance fixedSourceDistance;
        private final GeoDistance.DistanceBoundingCheck distanceBoundingCheck;
        private final double inclusiveLowerPoint; // in miles
        private final double inclusiveUpperPoint; // in miles

        public GeoDistanceRangeDocSet(int maxDoc, GeoPointFieldData fieldData, GeoDistance.FixedSourceDistance fixedSourceDistance, GeoDistance.DistanceBoundingCheck distanceBoundingCheck,
                                      double inclusiveLowerPoint, double inclusiveUpperPoint) {
            super(maxDoc);
            this.fieldData = fieldData;
            this.fixedSourceDistance = fixedSourceDistance;
            this.distanceBoundingCheck = distanceBoundingCheck;
            this.inclusiveLowerPoint = inclusiveLowerPoint;
            this.inclusiveUpperPoint = inclusiveUpperPoint;
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
                    double lat = lats[i];
                    double lon = lons[i];
                    if (distanceBoundingCheck.isWithin(lat, lon)) {
                        double d = fixedSourceDistance.calculate(lat, lon);
                        if (d >= inclusiveLowerPoint && d <= inclusiveUpperPoint) {
                            return true;
                        }
                    }
                }
                return false;
            } else {
                double lat = fieldData.latValue(doc);
                double lon = fieldData.lonValue(doc);
                if (distanceBoundingCheck.isWithin(lat, lon)) {
                    double d = fixedSourceDistance.calculate(lat, lon);
                    if (d >= inclusiveLowerPoint && d <= inclusiveUpperPoint) {
                        return true;
                    }
                }
                return false;
            }
        }
    }
}
