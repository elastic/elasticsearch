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

package org.elasticsearch.search.sort;

import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.search.geo.GeoDistance;

import java.io.IOException;

/**
 * A geo distance based sorting on a geo point like field.
 *
 *
 */
public class GeoDistanceSortBuilder extends SortBuilder {

    final String fieldName;

    private double lat;
    private double lon;
    private String geohash;

    private GeoDistance geoDistance;
    private DistanceUnit unit;
    private SortOrder order;

    /**
     * Constructs a new distance based sort on a geo point like field.
     *
     * @param fieldName The geo point like field name.
     */
    public GeoDistanceSortBuilder(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * The point to create the range distance facets from.
     *
     * @param lat latitude.
     * @param lon longitude.
     */
    public GeoDistanceSortBuilder point(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
        return this;
    }

    /**
     * The geohash of the geo point to create the range distance facets from.
     */
    public GeoDistanceSortBuilder geohash(String geohash) {
        this.geohash = geohash;
        return this;
    }

    /**
     * The geo distance type used to compute the distance.
     */
    public GeoDistanceSortBuilder geoDistance(GeoDistance geoDistance) {
        this.geoDistance = geoDistance;
        return this;
    }

    /**
     * The distance unit to use. Defaults to {@link org.elasticsearch.common.unit.DistanceUnit#KILOMETERS}
     */
    public GeoDistanceSortBuilder unit(DistanceUnit unit) {
        this.unit = unit;
        return this;
    }

    /**
     * The order of sorting. Defaults to {@link SortOrder#ASC}.
     */
    @Override
    public GeoDistanceSortBuilder order(SortOrder order) {
        this.order = order;
        return this;
    }

    /**
     * Not relevant.
     */
    @Override
    public SortBuilder missing(Object missing) {
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("_geo_distance");

        if (geohash != null) {
            builder.field(fieldName, geohash);
        } else {
            builder.startArray(fieldName).value(lon).value(lat).endArray();
        }

        if (unit != null) {
            builder.field("unit", unit);
        }
        if (geoDistance != null) {
            builder.field("distance_type", geoDistance.name().toLowerCase());
        }
        if (order == SortOrder.DESC) {
            builder.field("reverse", true);
        }

        builder.endObject();
        return builder;
    }
}
