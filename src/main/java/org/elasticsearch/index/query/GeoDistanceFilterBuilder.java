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

package org.elasticsearch.index.query;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 *
 */
public class GeoDistanceFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private String distance;

    private double lat;

    private double lon;

    private String geohash;

    private GeoDistance geoDistance;

    private String optimizeBbox;

    private Boolean cache;
    private String cacheKey;

    private String filterName;

    public GeoDistanceFilterBuilder(String name) {
        this.name = name;
    }

    public GeoDistanceFilterBuilder point(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
        return this;
    }

    public GeoDistanceFilterBuilder lat(double lat) {
        this.lat = lat;
        return this;
    }

    public GeoDistanceFilterBuilder lon(double lon) {
        this.lon = lon;
        return this;
    }

    public GeoDistanceFilterBuilder distance(String distance) {
        this.distance = distance;
        return this;
    }

    public GeoDistanceFilterBuilder distance(double distance, DistanceUnit unit) {
        this.distance = unit.toString(distance);
        return this;
    }

    public GeoDistanceFilterBuilder geohash(String geohash) {
        this.geohash = geohash;
        return this;
    }

    public GeoDistanceFilterBuilder geoDistance(GeoDistance geoDistance) {
        this.geoDistance = geoDistance;
        return this;
    }

    public GeoDistanceFilterBuilder optimizeBbox(String optimizeBbox) {
        this.optimizeBbox = optimizeBbox;
        return this;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public GeoDistanceFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>false</tt>.
     */
    public GeoDistanceFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public GeoDistanceFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoDistanceFilterParser.NAME);
        if (geohash != null) {
            builder.field(name, geohash);
        } else {
            builder.startArray(name).value(lon).value(lat).endArray();
        }
        builder.field("distance", distance);
        if (geoDistance != null) {
            builder.field("distance_type", geoDistance.name().toLowerCase(Locale.ROOT));
        }
        if (optimizeBbox != null) {
            builder.field("optimize_bbox", optimizeBbox);
        }
        if (filterName != null) {
            builder.field("_name", filterName);
        }
        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }
        builder.endObject();
    }
}
