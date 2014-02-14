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
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 *
 */
public class GeoDistanceRangeFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private Object from;
    private Object to;
    private boolean includeLower = true;
    private boolean includeUpper = true;

    private double lat;

    private double lon;

    private String geohash;

    private GeoDistance geoDistance;

    private Boolean cache;
    private String cacheKey;

    private String filterName;

    private String optimizeBbox;

    public GeoDistanceRangeFilterBuilder(String name) {
        this.name = name;
    }

    public GeoDistanceRangeFilterBuilder point(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
        return this;
    }

    public GeoDistanceRangeFilterBuilder lat(double lat) {
        this.lat = lat;
        return this;
    }

    public GeoDistanceRangeFilterBuilder lon(double lon) {
        this.lon = lon;
        return this;
    }

    public GeoDistanceRangeFilterBuilder from(Object from) {
        this.from = from;
        return this;
    }

    public GeoDistanceRangeFilterBuilder to(Object to) {
        this.to = to;
        return this;
    }

    public GeoDistanceRangeFilterBuilder gt(Object from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    public GeoDistanceRangeFilterBuilder gte(Object from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    public GeoDistanceRangeFilterBuilder lt(Object to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    public GeoDistanceRangeFilterBuilder lte(Object to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    public GeoDistanceRangeFilterBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    public GeoDistanceRangeFilterBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    public GeoDistanceRangeFilterBuilder geohash(String geohash) {
        this.geohash = geohash;
        return this;
    }

    public GeoDistanceRangeFilterBuilder geoDistance(GeoDistance geoDistance) {
        this.geoDistance = geoDistance;
        return this;
    }

    public GeoDistanceRangeFilterBuilder optimizeBbox(String optimizeBbox) {
        this.optimizeBbox = optimizeBbox;
        return this;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public GeoDistanceRangeFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>false</tt>.
     */
    public GeoDistanceRangeFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public GeoDistanceRangeFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoDistanceRangeFilterParser.NAME);
        if (geohash != null) {
            builder.field(name, geohash);
        } else {
            builder.startArray(name).value(lon).value(lat).endArray();
        }
        builder.field("from", from);
        builder.field("to", to);
        builder.field("include_lower", includeLower);
        builder.field("include_upper", includeUpper);
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
