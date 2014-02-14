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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 *
 */
public class GeoBoundingBoxFilterBuilder extends BaseFilterBuilder {

    public static final String TOP_LEFT = GeoBoundingBoxFilterParser.TOP_LEFT;
    public static final String BOTTOM_RIGHT = GeoBoundingBoxFilterParser.BOTTOM_RIGHT;

    private static final int TOP = 0;
    private static final int LEFT = 1;
    private static final int BOTTOM = 2;
    private static final int RIGHT = 3;
    
    private final String name;

    private double[] box = {Double.NaN, Double.NaN, Double.NaN, Double.NaN};

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
        box[TOP] = lat;
        box[LEFT] = lon;
        return this;
    }

    public GeoBoundingBoxFilterBuilder topLeft(GeoPoint point) {
        return topLeft(point.lat(), point.lon());
    }

    public GeoBoundingBoxFilterBuilder topLeft(String geohash) {
        return topLeft(GeoHashUtils.decode(geohash));
    }

    /**
     * Adds bottom right corner.
     *
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxFilterBuilder bottomRight(double lat, double lon) {
        box[BOTTOM] = lat;
        box[RIGHT] = lon;
        return this;
    }

    public GeoBoundingBoxFilterBuilder bottomRight(GeoPoint point) {
        return bottomRight(point.lat(), point.lon());
    }

    public GeoBoundingBoxFilterBuilder bottomRight(String geohash) {
        return bottomRight(GeoHashUtils.decode(geohash));
    }

    /**
     * Adds bottom left corner.
     *
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxFilterBuilder bottomLeft(double lat, double lon) {
        box[BOTTOM] = lat;
        box[LEFT] = lon;
        return this;
    }

    public GeoBoundingBoxFilterBuilder bottomLeft(GeoPoint point) {
        return bottomLeft(point.lat(), point.lon());
    }

    public GeoBoundingBoxFilterBuilder bottomLeft(String geohash) {
        return bottomLeft(GeoHashUtils.decode(geohash));
    }
    
    /**
     * Adds top right point.
     *
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxFilterBuilder topRight(double lat, double lon) {
        box[TOP] = lat;
        box[RIGHT] = lon;
        return this;
    }

    public GeoBoundingBoxFilterBuilder topRight(GeoPoint point) {
        return topRight(point.lat(), point.lon());
    }

    public GeoBoundingBoxFilterBuilder topRight(String geohash) {
        return topRight(GeoHashUtils.decode(geohash));
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
        // check values
        if(Double.isNaN(box[TOP])) {
            throw new ElasticsearchIllegalArgumentException("geo_bounding_box requires top latitude to be set");
        } else if(Double.isNaN(box[BOTTOM])) {
            throw new ElasticsearchIllegalArgumentException("geo_bounding_box requires bottom latitude to be set");
        } else if(Double.isNaN(box[RIGHT])) {
            throw new ElasticsearchIllegalArgumentException("geo_bounding_box requires right longitude to be set");
        } else if(Double.isNaN(box[LEFT])) {
            throw new ElasticsearchIllegalArgumentException("geo_bounding_box requires left longitude to be set");
        }
                
        builder.startObject(GeoBoundingBoxFilterParser.NAME);

        builder.startObject(name);
        builder.array(TOP_LEFT, box[LEFT], box[TOP]);
        builder.array(BOTTOM_RIGHT, box[RIGHT], box[BOTTOM]);
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
