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

package org.elasticsearch.search.facet.geodistance;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A geo distance builder allowing to create a facet of distances from a specific location including the
 * number of hits within each distance range, and aggregated data (like totals of either the distance or
 * cusotm value fields).
 */
public class GeoDistanceFacetBuilder extends FacetBuilder {

    private String fieldName;

    private String valueFieldName;

    private double lat;

    private double lon;

    private String geohash;

    private GeoDistance geoDistance;

    private DistanceUnit unit;

    private Map<String, Object> params;

    private String valueScript;

    private String lang;

    private List<Entry> entries = Lists.newArrayList();

    /**
     * Constructs a new geo distance with the provided facet name.
     */
    public GeoDistanceFacetBuilder(String name) {
        super(name);
    }

    /**
     * The geo point field that will be used to extract the document location(s).
     */
    public GeoDistanceFacetBuilder field(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    /**
     * A custom value field (numeric) that will be used to provide aggregated data for each facet (for example, total).
     */
    public GeoDistanceFacetBuilder valueField(String valueFieldName) {
        this.valueFieldName = valueFieldName;
        return this;
    }

    /**
     * A custom value script (result is numeric) that will be used to provide aggregated data for each facet (for example, total).
     */
    public GeoDistanceFacetBuilder valueScript(String valueScript) {
        this.valueScript = valueScript;
        return this;
    }

    /**
     * The language of the {@link #valueScript(String)} script.
     */
    public GeoDistanceFacetBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    /**
     * Parameters for {@link #valueScript(String)} to improve performance when executing the same script with different parameters.
     */
    public GeoDistanceFacetBuilder scriptParam(String name, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(name, value);
        return this;
    }

    /**
     * The point to create the range distance facets from.
     *
     * @param lat latitude.
     * @param lon longitude.
     */
    public GeoDistanceFacetBuilder point(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
        return this;
    }

    /**
     * The latitude to create the range distance facets from.
     */
    public GeoDistanceFacetBuilder lat(double lat) {
        this.lat = lat;
        return this;
    }

    /**
     * The longitude to create the range distance facets from.
     */
    public GeoDistanceFacetBuilder lon(double lon) {
        this.lon = lon;
        return this;
    }

    /**
     * The geohash of the geo point to create the range distance facets from.
     */
    public GeoDistanceFacetBuilder geohash(String geohash) {
        this.geohash = geohash;
        return this;
    }

    /**
     * The geo distance type used to compute the distance.
     */
    public GeoDistanceFacetBuilder geoDistance(GeoDistance geoDistance) {
        this.geoDistance = geoDistance;
        return this;
    }

    /**
     * Adds a range entry with explicit from and to.
     *
     * @param from The from distance limit
     * @param to   The to distance limit
     */
    public GeoDistanceFacetBuilder addRange(double from, double to) {
        entries.add(new Entry(from, to));
        return this;
    }

    /**
     * Adds a range entry with explicit from and unbounded to.
     *
     * @param from the from distance limit, to is unbounded.
     */
    public GeoDistanceFacetBuilder addUnboundedTo(double from) {
        entries.add(new Entry(from, Double.POSITIVE_INFINITY));
        return this;
    }

    /**
     * Adds a range entry with explicit to and unbounded from.
     *
     * @param to the to distance limit, from is unbounded.
     */
    public GeoDistanceFacetBuilder addUnboundedFrom(double to) {
        entries.add(new Entry(Double.NEGATIVE_INFINITY, to));
        return this;
    }

    /**
     * The distance unit to use. Defaults to {@link org.elasticsearch.common.unit.DistanceUnit#KILOMETERS}
     */
    public GeoDistanceFacetBuilder unit(DistanceUnit unit) {
        this.unit = unit;
        return this;
    }

    /**
     * Marks the facet to run in a global scope, not bounded by any query.
     */
    public GeoDistanceFacetBuilder global(boolean global) {
        super.global(global);
        return this;
    }

    public GeoDistanceFacetBuilder facetFilter(FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     */
    public GeoDistanceFacetBuilder nested(String nested) {
        this.nested = nested;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (fieldName == null) {
            throw new SearchSourceBuilderException("field must be set on geo_distance facet for facet [" + name + "]");
        }
        if (entries.isEmpty()) {
            throw new SearchSourceBuilderException("at least one range must be defined for geo_distance facet [" + name + "]");
        }

        builder.startObject(name);

        builder.startObject(GeoDistanceFacet.TYPE);

        if (geohash != null) {
            builder.field(fieldName, geohash);
        } else {
            builder.startArray(fieldName).value(lon).value(lat).endArray();
        }

        if (valueFieldName != null) {
            builder.field("value_field", valueFieldName);
        }

        if (valueScript != null) {
            builder.field("value_script", valueScript);
            if (lang != null) {
                builder.field("lang", lang);
            }
            if (this.params != null) {
                builder.field("params", this.params);
            }
        }

        builder.startArray("ranges");
        for (Entry entry : entries) {
            builder.startObject();
            if (!Double.isInfinite(entry.from)) {
                builder.field("from", entry.from);
            }
            if (!Double.isInfinite(entry.to)) {
                builder.field("to", entry.to);
            }
            builder.endObject();
        }
        builder.endArray();

        if (unit != null) {
            builder.field("unit", unit);
        }
        if (geoDistance != null) {
            builder.field("distance_type", geoDistance.name().toLowerCase(Locale.ROOT));
        }

        builder.endObject();

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
        return builder;
    }

    private static class Entry {
        final double from;
        final double to;

        private Entry(double from, double to) {
            this.from = from;
            this.to = to;
        }
    }
}
