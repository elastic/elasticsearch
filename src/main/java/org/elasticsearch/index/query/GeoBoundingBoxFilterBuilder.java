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

package org.elasticsearch.index.query;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 *
 */
public class GeoBoundingBoxFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private GeoPoint topLeft;

    private String topLeftGeohash;

    private GeoPoint bottomRight;

    private String bottomRightGeohash;

    private Boolean cache;
    private String cacheKey;

    private String filterName;

    private String type;

    public GeoBoundingBoxFilterBuilder(String name) {
        this.name = name;
    }

    /**
     * Adds top left point.
     *
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxFilterBuilder topLeft(double lat, double lon) {
        topLeft = new GeoPoint(lat, lon);
        return this;
    }

    /**
     * Adds bottom right point.
     *
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxFilterBuilder bottomRight(double lat, double lon) {
        bottomRight = new GeoPoint(lat, lon);
        return this;
    }

    public GeoBoundingBoxFilterBuilder topLeft(String geohash) {
        this.topLeftGeohash = geohash;
        return this;
    }

    public GeoBoundingBoxFilterBuilder bottomRight(String geohash) {
        this.bottomRightGeohash = geohash;
        return this;
    }

    /**
     * Adds top left and bottom right by geohash cell.
     *
     * @param geohash the geohash of the cell definign the boundingbox
     */
    public GeoBoundingBoxFilterBuilder geohash(String geohash) {
        topLeft = new GeoPoint();
        bottomRight = new GeoPoint();
        GeoHashUtils.decodeCell(geohash, topLeft, bottomRight);
        return this;
    }


    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public GeoBoundingBoxFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>false</tt>.
     */
    public GeoBoundingBoxFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public GeoBoundingBoxFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    /**
     * Sets the type of executing of the geo bounding box. Can be either `memory` or `indexed`. Defaults
     * to `memory`.
     */
    public GeoBoundingBoxFilterBuilder type(String type) {
        this.type = type;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoBoundingBoxFilterParser.NAME);

        builder.startObject(name);
        if (topLeftGeohash != null) {
            builder.field("top_left", topLeftGeohash);
        } else if (topLeft != null) {
            builder.startArray("top_left").value(topLeft.lon()).value(topLeft.lat()).endArray();
        } else {
            throw new ElasticSearchIllegalArgumentException("geo_bounding_box requires 'top_left' to be set");
        }

        if (bottomRightGeohash != null) {
            builder.field("bottom_right", bottomRightGeohash);
        } else if (bottomRight != null) {
            builder.startArray("bottom_right").value(bottomRight.lon()).value(bottomRight.lat()).endArray();
        } else {
            throw new ElasticSearchIllegalArgumentException("geo_bounding_box requires 'bottom_right' to be set");
        }
        builder.endObject();

        if (filterName != null) {
            builder.field("_name", filterName);
        }
        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }
        if (type != null) {
            builder.field("type", type);
        }

        builder.endObject();
    }
}
