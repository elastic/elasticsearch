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
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.NumericFieldData;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class GeoDistanceFilter extends Filter {

    private final double lat;

    private final double lon;

    private final double distance; // in miles

    private final GeoDistance geoDistance;

    private final String latFieldName;

    private final String lonFieldName;

    private final FieldData.Type fieldDataType;

    private final FieldDataCache fieldDataCache;

    public GeoDistanceFilter(double lat, double lon, double distance, GeoDistance geoDistance, String latFieldName, String lonFieldName,
                             FieldData.Type fieldDataType, FieldDataCache fieldDataCache) {
        this.lat = lat;
        this.lon = lon;
        this.distance = distance;
        this.geoDistance = geoDistance;
        this.latFieldName = latFieldName;
        this.lonFieldName = lonFieldName;
        this.fieldDataType = fieldDataType;
        this.fieldDataCache = fieldDataCache;
    }

    public double lat() {
        return lat;
    }

    public double lon() {
        return lon;
    }

    public double distance() {
        return distance;
    }

    public GeoDistance geoDistance() {
        return geoDistance;
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
            @Override public boolean isCacheable() {
                return false;
            }

            @Override public boolean get(int doc) throws IOException {
                if (!latFieldData.hasValue(doc) || !lonFieldData.hasValue(doc)) {
                    return false;
                }

                if (latFieldData.multiValued()) {
                    double[] lats = latFieldData.doubleValues(doc);
                    double[] lons = latFieldData.doubleValues(doc);
                    for (int i = 0; i < lats.length; i++) {
                        double d = geoDistance.calculate(lat, lon, lats[i], lons[i], DistanceUnit.MILES);
                        if (d < distance) {
                            return true;
                        }
                    }
                    return false;
                } else {
                    double d = geoDistance.calculate(lat, lon, latFieldData.doubleValue(doc), lonFieldData.doubleValue(doc), DistanceUnit.MILES);
                    return d < distance;
                }
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeoDistanceFilter that = (GeoDistanceFilter) o;

        if (Double.compare(that.distance, distance) != 0) return false;
        if (Double.compare(that.lat, lat) != 0) return false;
        if (Double.compare(that.lon, lon) != 0) return false;
        if (geoDistance != that.geoDistance) return false;
        if (latFieldName != null ? !latFieldName.equals(that.latFieldName) : that.latFieldName != null)
            return false;
        if (lonFieldName != null ? !lonFieldName.equals(that.lonFieldName) : that.lonFieldName != null)
            return false;

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
        temp = distance != +0.0d ? Double.doubleToLongBits(distance) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (geoDistance != null ? geoDistance.hashCode() : 0);
        result = 31 * result + (latFieldName != null ? latFieldName.hashCode() : 0);
        result = 31 * result + (lonFieldName != null ? lonFieldName.hashCode() : 0);
        return result;
    }
}
