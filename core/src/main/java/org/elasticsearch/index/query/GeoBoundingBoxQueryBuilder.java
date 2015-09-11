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

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class GeoBoundingBoxQueryBuilder extends QueryBuilder {

    public static final String TOP_LEFT = GeoBoundingBoxQueryParser.TOP_LEFT;
    public static final String BOTTOM_RIGHT = GeoBoundingBoxQueryParser.BOTTOM_RIGHT;

    private static final int TOP = 0;
    private static final int LEFT = 1;
    private static final int BOTTOM = 2;
    private static final int RIGHT = 3;
    
    private final String name;

    private double[] box = {Double.NaN, Double.NaN, Double.NaN, Double.NaN};

    private String queryName;
    private String type;
    private Boolean coerce;
    private Boolean ignoreMalformed;

    public GeoBoundingBoxQueryBuilder(String name) {
        this.name = name;
    }

    /**
     * Adds top left point.
     *
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxQueryBuilder topLeft(double lat, double lon) {
        box[TOP] = lat;
        box[LEFT] = lon;
        return this;
    }

    public GeoBoundingBoxQueryBuilder topLeft(GeoPoint point) {
        return topLeft(point.lat(), point.lon());
    }

    public GeoBoundingBoxQueryBuilder topLeft(String geohash) {
        return topLeft(GeoPoint.fromGeohash(geohash));
    }

    /**
     * Adds bottom right corner.
     *
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxQueryBuilder bottomRight(double lat, double lon) {
        box[BOTTOM] = lat;
        box[RIGHT] = lon;
        return this;
    }

    public GeoBoundingBoxQueryBuilder bottomRight(GeoPoint point) {
        return bottomRight(point.lat(), point.lon());
    }

    public GeoBoundingBoxQueryBuilder bottomRight(String geohash) {
        return bottomRight(GeoPoint.fromGeohash(geohash));
    }

    /**
     * Adds bottom left corner.
     *
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxQueryBuilder bottomLeft(double lat, double lon) {
        box[BOTTOM] = lat;
        box[LEFT] = lon;
        return this;
    }

    public GeoBoundingBoxQueryBuilder bottomLeft(GeoPoint point) {
        return bottomLeft(point.lat(), point.lon());
    }

    public GeoBoundingBoxQueryBuilder bottomLeft(String geohash) {
        return bottomLeft(GeoPoint.fromGeohash(geohash));
    }
    
    /**
     * Adds top right point.
     *
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxQueryBuilder topRight(double lat, double lon) {
        box[TOP] = lat;
        box[RIGHT] = lon;
        return this;
    }

    public GeoBoundingBoxQueryBuilder topRight(GeoPoint point) {
        return topRight(point.lat(), point.lon());
    }

    public GeoBoundingBoxQueryBuilder topRight(String geohash) {
        return topRight(GeoPoint.fromGeohash(geohash));
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public GeoBoundingBoxQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    public GeoBoundingBoxQueryBuilder coerce(boolean coerce) {
        this.coerce = coerce;
        return this;
    }

    public GeoBoundingBoxQueryBuilder ignoreMalformed(boolean ignoreMalformed) {
        this.ignoreMalformed = ignoreMalformed;
        return this;
    }

    /**
     * Sets the type of executing of the geo bounding box. Can be either `memory` or `indexed`. Defaults
     * to `memory`.
     */
    public GeoBoundingBoxQueryBuilder type(String type) {
        this.type = type;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        // check values
        if(Double.isNaN(box[TOP])) {
            throw new IllegalArgumentException("geo_bounding_box requires top latitude to be set");
        } else if(Double.isNaN(box[BOTTOM])) {
            throw new IllegalArgumentException("geo_bounding_box requires bottom latitude to be set");
        } else if(Double.isNaN(box[RIGHT])) {
            throw new IllegalArgumentException("geo_bounding_box requires right longitude to be set");
        } else if(Double.isNaN(box[LEFT])) {
            throw new IllegalArgumentException("geo_bounding_box requires left longitude to be set");
        }
                
        builder.startObject(GeoBoundingBoxQueryParser.NAME);

        builder.startObject(name);
        builder.array(TOP_LEFT, box[LEFT], box[TOP]);
        builder.array(BOTTOM_RIGHT, box[RIGHT], box[BOTTOM]);
        builder.endObject();

        if (queryName != null) {
            builder.field("_name", queryName);
        }
        if (type != null) {
            builder.field("type", type);
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
