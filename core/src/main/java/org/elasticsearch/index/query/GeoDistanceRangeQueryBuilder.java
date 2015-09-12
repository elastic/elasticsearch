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

public class GeoDistanceRangeQueryBuilder extends QueryBuilder {

    private final String name;

    private Object from;
    private Object to;
    private boolean includeLower = true;
    private boolean includeUpper = true;

    private double lat;

    private double lon;

    private String geohash;

    private GeoDistance geoDistance;

    private String queryName;

    private String optimizeBbox;

    private Boolean coerce;

    private Boolean ignoreMalformed;

    public GeoDistanceRangeQueryBuilder(String name) {
        this.name = name;
    }

    public GeoDistanceRangeQueryBuilder point(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
        return this;
    }

    public GeoDistanceRangeQueryBuilder lat(double lat) {
        this.lat = lat;
        return this;
    }

    public GeoDistanceRangeQueryBuilder lon(double lon) {
        this.lon = lon;
        return this;
    }

    public GeoDistanceRangeQueryBuilder from(Object from) {
        this.from = from;
        return this;
    }

    public GeoDistanceRangeQueryBuilder to(Object to) {
        this.to = to;
        return this;
    }

    public GeoDistanceRangeQueryBuilder gt(Object from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    public GeoDistanceRangeQueryBuilder gte(Object from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    public GeoDistanceRangeQueryBuilder lt(Object to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    public GeoDistanceRangeQueryBuilder lte(Object to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    public GeoDistanceRangeQueryBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    public GeoDistanceRangeQueryBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    public GeoDistanceRangeQueryBuilder geohash(String geohash) {
        this.geohash = geohash;
        return this;
    }

    public GeoDistanceRangeQueryBuilder geoDistance(GeoDistance geoDistance) {
        this.geoDistance = geoDistance;
        return this;
    }

    public GeoDistanceRangeQueryBuilder optimizeBbox(String optimizeBbox) {
        this.optimizeBbox = optimizeBbox;
        return this;
    }

    public GeoDistanceRangeQueryBuilder coerce(boolean coerce) {
        this.coerce = coerce;
        return this;
    }

    public GeoDistanceRangeQueryBuilder ignoreMalformed(boolean ignoreMalformed) {
        this.ignoreMalformed = ignoreMalformed;
        return this;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public GeoDistanceRangeQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoDistanceRangeQueryParser.NAME);
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
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        if (coerce != null) {
            builder.field("coerce", coerce);
        }
        if (ignoreMalformed != null) {
            builder.field("ignore_malformed", ignoreMalformed);
        }
        builder.endObject();
    }
}
