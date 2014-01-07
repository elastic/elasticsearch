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

package org.elasticsearch.search.aggregations.bucket.range.geodistance;

import com.google.common.collect.Lists;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 *
 */
public class GeoDistanceBuilder extends AggregationBuilder<GeoDistanceBuilder> {

    public static class Range implements ToXContent {

        private String key;
        private Double from;
        private Double to;

        public Range(String key, Double from, Double to) {
            this.key = key;
            this.from = from;
            this.to = to;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (from != null) {
                builder.field("from", from.doubleValue());
            }
            if (to != null) {
                builder.field("to", to.doubleValue());
            }
            if (key != null) {
                builder.field("key", key);
            }
            return builder.endObject();
        }

    }

    private String field;
    private DistanceUnit unit;
    private GeoDistance distanceType;
    private GeoPoint point;

    private List<Range> ranges = Lists.newArrayList();

    public GeoDistanceBuilder(String name) {
        super(name, InternalGeoDistance.TYPE.name());
    }

    public GeoDistanceBuilder field(String field) {
        this.field = field;
        return this;
    }

    public GeoDistanceBuilder unit(DistanceUnit unit) {
        this.unit = unit;
        return this;
    }

    public GeoDistanceBuilder distanceType(GeoDistance distanceType) {
        this.distanceType = distanceType;
        return this;
    }

    public GeoDistanceBuilder point(String latLon) {
        return point(GeoPoint.parseFromLatLon(latLon));
    }

    public GeoDistanceBuilder point(GeoPoint point) {
        this.point = point;
        return this;
    }

    public GeoDistanceBuilder geohash(String geohash) {
        if (this.point == null) {
            this.point = new GeoPoint();
        }
        this.point.resetFromGeoHash(geohash);
        return this;
    }

    public GeoDistanceBuilder lat(double lat) {
        if (this.point == null) {
            point = new GeoPoint();
        }
        point.resetLat(lat);
        return this;
    }

    public GeoDistanceBuilder lon(double lon) {
        if (this.point == null) {
            point = new GeoPoint();
        }
        point.resetLon(lon);
        return this;
    }

    public GeoDistanceBuilder addRange(String key, double from, double to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    public GeoDistanceBuilder addRange(double from, double to) {
        return addRange(null, from, to);
    }

    public GeoDistanceBuilder addUnboundedTo(String key, double to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    public GeoDistanceBuilder addUnboundedTo(double to) {
        return addUnboundedTo(null, to);
    }

    public GeoDistanceBuilder addUnboundedFrom(String key, double from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    public GeoDistanceBuilder addUnboundedFrom(double from) {
        return addUnboundedFrom(null, from);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (ranges.isEmpty()) {
            throw new SearchSourceBuilderException("at least one range must be defined for geo_distance aggregation [" + name + "]");
        }
        if (point == null) {
            throw new SearchSourceBuilderException("center point must be defined for geo_distance aggregation [" + name + "]");
        }

        if (field != null) {
            builder.field("field", field);
        }

        if (unit != null) {
            builder.field("unit", unit);
        }

        if (distanceType != null) {
            builder.field("distance_type", distanceType.name().toLowerCase(Locale.ROOT));
        }

        builder.startObject("center")
                .field("lat", point.lat())
                .field("lon", point.lon())
                .endObject();

        builder.startArray("ranges");
        for (Range range : ranges) {
            range.toXContent(builder, params);
        }
        builder.endArray();

        return builder.endObject();
    }

}
