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

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Builder for the {@link GeoDistance} aggregation.
 */
public class GeoDistanceBuilder extends AggregationBuilder<GeoDistanceBuilder> {

    /**
     * A range of values.
     */
    public static class Range implements ToXContent {

        private String key;
        private Double from;
        private Double to;

        /**
         * Create a new range.
         * @param key   the identifier of this range
         * @param from  the lower bound (inclusive)
         * @param to    the upper bound (exclusive)
         */
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

    private List<Range> ranges = new ArrayList<>();

    /**
     * Sole constructor.
     */
    public GeoDistanceBuilder(String name) {
        super(name, InternalGeoDistance.TYPE.name());
    }

    /**
     * Set the field to use to compute distances.
     */
    public GeoDistanceBuilder field(String field) {
        this.field = field;
        return this;
    }

    /**
     * Set the unit to use for distances, default is kilometers.
     */
    public GeoDistanceBuilder unit(DistanceUnit unit) {
        this.unit = unit;
        return this;
    }

    /**
     * Set the {@link GeoDistance distance type} to use, defaults to
     * {@link GeoDistance#SLOPPY_ARC}.
     */
    public GeoDistanceBuilder distanceType(GeoDistance distanceType) {
        this.distanceType = distanceType;
        return this;
    }

    /**
     * Set the point to calculate distances from using a
     * <code>lat,lon</code> notation or geohash.
     */
    public GeoDistanceBuilder point(String latLon) {
        return point(GeoPoint.parseFromLatLon(latLon));
    }

    /**
     * Set the point to calculate distances from.
     */
    public GeoDistanceBuilder point(GeoPoint point) {
        this.point = point;
        return this;
    }

    /**
     * Set the point to calculate distances from using its geohash.
     */
    public GeoDistanceBuilder geohash(String geohash) {
        if (this.point == null) {
            this.point = new GeoPoint();
        }
        this.point.resetFromGeoHash(geohash);
        return this;
    }

    /**
     * Set the latitude of the point to calculate distances from.
     */
    public GeoDistanceBuilder lat(double lat) {
        if (this.point == null) {
            point = new GeoPoint();
        }
        point.resetLat(lat);
        return this;
    }

    /**
     * Set the longitude of the point to calculate distances from.
     */
    public GeoDistanceBuilder lon(double lon) {
        if (this.point == null) {
            point = new GeoPoint();
        }
        point.resetLon(lon);
        return this;
    }

    /**
     * Add a new range to this aggregation.
     *
     * @param key  the key to use for this range in the response
     * @param from the lower bound on the distances, inclusive
     * @param to   the upper bound on the distances, exclusive
     */
    public GeoDistanceBuilder addRange(String key, double from, double to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    /**
     * Same as {@link #addRange(String, double, double)} but the key will be
     * automatically generated based on <code>from</code> and <code>to</code>.
     */
    public GeoDistanceBuilder addRange(double from, double to) {
        return addRange(null, from, to);
    }

    /**
     * Add a new range with no lower bound.
     *
     * @param key the key to use for this range in the response
     * @param to  the upper bound on the distances, exclusive
     */
    public GeoDistanceBuilder addUnboundedTo(String key, double to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, double)} but the key will be
     * computed automatically.
     */
    public GeoDistanceBuilder addUnboundedTo(double to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Add a new range with no upper bound.
     *
     * @param key  the key to use for this range in the response
     * @param from the lower bound on the distances, inclusive
     */
    public GeoDistanceBuilder addUnboundedFrom(String key, double from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, double)} but the key will be
     * computed automatically.
     */
    public GeoDistanceBuilder addUnboundedFrom(double from) {
        return addUnboundedFrom(null, from);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (ranges.isEmpty()) {
            throw new SearchSourceBuilderException("at least one range must be defined for geo_distance aggregation [" + getName() + "]");
        }
        if (point == null) {
            throw new SearchSourceBuilderException("center point must be defined for geo_distance aggregation [" + getName() + "]");
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
