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

package org.elasticsearch.search.sort;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * A geo distance based sorting on a geo point like field.
 */
public class GeoDistanceSortBuilder extends SortBuilder {

    final String fieldName;
    private final List<GeoPoint> points = new ArrayList<>();
    private final List<String> geohashes = new ArrayList<>();

    private GeoDistance geoDistance;
    private DistanceUnit unit;
    private SortOrder order;
    private String sortMode;
    private QueryBuilder nestedFilter;
    private String nestedPath;
    private Boolean coerce;
    private Boolean ignoreMalformed;

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
        points.add(new GeoPoint(lat, lon));
        return this;
    }

    /**
     * The point to create the range distance facets from.
     *
     * @param points reference points.
     */
    public GeoDistanceSortBuilder points(GeoPoint... points) {
        this.points.addAll(Arrays.asList(points));
        return this;
    }

    /**
     * The geohash of the geo point to create the range distance facets from.
     */
    public GeoDistanceSortBuilder geohashes(String... geohashes) {
        this.geohashes.addAll(Arrays.asList(geohashes));
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

    /**
     * Defines which distance to use for sorting in the case a document contains multiple geo points.
     * Possible values: min and max
     */
    public GeoDistanceSortBuilder sortMode(String sortMode) {
        this.sortMode = sortMode;
        return this;
    }

    /**
     * Sets the nested filter that the nested objects should match with in order to be taken into account
     * for sorting.
     */
    public GeoDistanceSortBuilder setNestedFilter(QueryBuilder nestedFilter) {
        this.nestedFilter = nestedFilter;
        return this;
    }

    /**
     * Sets the nested path if sorting occurs on a field that is inside a nested object. By default when sorting on a
     * field inside a nested object, the nearest upper nested object is selected as nested path.
     */
    public GeoDistanceSortBuilder setNestedPath(String nestedPath) {
        this.nestedPath = nestedPath;
        return this;
    }

    public GeoDistanceSortBuilder coerce(boolean coerce) {
        this.coerce = coerce;
        return this;
    }

    public GeoDistanceSortBuilder ignoreMalformed(boolean ignoreMalformed) {
        this.ignoreMalformed = ignoreMalformed;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("_geo_distance");
        if (geohashes.size() == 0 && points.size() == 0) {
            throw new ElasticsearchParseException("No points provided for _geo_distance sort.");
        }

        builder.startArray(fieldName);
        for (GeoPoint point : points) {
            builder.value(point);
        }
        for (String geohash : geohashes) {
            builder.value(geohash);
        }
        builder.endArray();

        if (unit != null) {
            builder.field("unit", unit);
        }
        if (geoDistance != null) {
            builder.field("distance_type", geoDistance.name().toLowerCase(Locale.ROOT));
        }
        if (order == SortOrder.DESC) {
            builder.field("reverse", true);
        }
        if (sortMode != null) {
            builder.field("mode", sortMode);
        }

        if (nestedPath != null) {
            builder.field("nested_path", nestedPath);
        }
        if (nestedFilter != null) {
            builder.field("nested_filter", nestedFilter, params);
        }
        if (coerce != null) {
            builder.field("coerce", coerce);
        }
        if (ignoreMalformed != null) {
            builder.field("ignore_malformed", ignoreMalformed);
        }

        builder.endObject();
        return builder;
    }
}
