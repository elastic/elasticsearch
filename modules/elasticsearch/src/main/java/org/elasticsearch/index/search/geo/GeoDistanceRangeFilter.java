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

package org.elasticsearch.index.search.geo;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.docset.GetDocSet;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.mapper.xcontent.geo.GeoPointFieldData;
import org.elasticsearch.index.mapper.xcontent.geo.GeoPointFieldDataType;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class GeoDistanceRangeFilter extends Filter {

    private final double lat;

    private final double lon;

    private final double inclusiveLowerPoint; // in miles
    private final double inclusiveUpperPoint; // in miles

    private final GeoDistance geoDistance;

    private final String fieldName;

    private final FieldDataCache fieldDataCache;

    public GeoDistanceRangeFilter(double lat, double lon, Double lowerVal, Double upperVal, boolean includeLower, boolean includeUpper, GeoDistance geoDistance, String fieldName, FieldDataCache fieldDataCache) {
        this.lat = lat;
        this.lon = lon;
        this.geoDistance = geoDistance;
        this.fieldName = fieldName;
        this.fieldDataCache = fieldDataCache;

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

    @Override public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final GeoPointFieldData fieldData = (GeoPointFieldData) fieldDataCache.cache(GeoPointFieldDataType.TYPE, reader, fieldName);
        return new GetDocSet(reader.maxDoc()) {

            @Override public boolean isCacheable() {
                // not cacheable for several reasons:
                // 1. It is only relevant when _cache is set to true, and then, we really want to create in mem bitset
                // 2. Its already fast without in mem bitset, since it works with field data
                return false;
            }

            @Override public boolean get(int doc) throws IOException {
                if (!fieldData.hasValue(doc)) {
                    return false;
                }

                if (fieldData.multiValued()) {
                    double[] lats = fieldData.latValues(doc);
                    double[] lons = fieldData.lonValues(doc);
                    for (int i = 0; i < lats.length; i++) {
                        double d = geoDistance.calculate(lat, lon, lats[i], lons[i], DistanceUnit.MILES);
                        if (d >= inclusiveLowerPoint && d <= inclusiveUpperPoint) {
                            return true;
                        }
                    }
                    return false;
                } else {
                    double d = geoDistance.calculate(lat, lon, fieldData.latValue(doc), fieldData.lonValue(doc), DistanceUnit.MILES);
                    if (d >= inclusiveLowerPoint && d <= inclusiveUpperPoint) {
                        return true;
                    }
                    return false;
                }
            }
        };
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
}
