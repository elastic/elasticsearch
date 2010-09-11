/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.query.xcontent;

import org.elasticsearch.common.lucene.geo.GeoBoundingBoxFilter;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilderException;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class GeoBoundingBoxFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private GeoBoundingBoxFilter.Point topLeft;

    private String topLeftGeohash;

    private GeoBoundingBoxFilter.Point bottomRight;

    private String bottomRightGeohash;

    private String filterName;

    public GeoBoundingBoxFilterBuilder(String name) {
        this.name = name;
    }

    public GeoBoundingBoxFilterBuilder topLeft(double lat, double lon) {
        topLeft = new GeoBoundingBoxFilter.Point();
        topLeft.lat = lat;
        topLeft.lon = lon;
        return this;
    }

    public GeoBoundingBoxFilterBuilder bottomRight(double lat, double lon) {
        bottomRight = new GeoBoundingBoxFilter.Point();
        bottomRight.lat = lat;
        bottomRight.lon = lon;
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
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public GeoBoundingBoxFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    @Override protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoBoundingBoxFilterParser.NAME);

        builder.startObject(name);
        if (topLeftGeohash != null) {
            builder.field("top_left", topLeftGeohash);
        } else if (topLeft != null) {
            builder.startArray("top_left").value(topLeft.lat).value(topLeft.lon).endArray();
        } else {
            throw new QueryBuilderException("geo_bounding_box requires 'top_left' to be set");
        }

        if (bottomRightGeohash != null) {
            builder.field("bottom_right", bottomRightGeohash);
        } else if (bottomRight != null) {
            builder.startArray("bottom_right").value(bottomRight.lat).value(bottomRight.lon).endArray();
        } else {
            throw new QueryBuilderException("geo_bounding_box requires 'bottom_right' to be set");
        }
        builder.endObject();

        if (filterName != null) {
            builder.field("_name", filterName);
        }

        builder.endObject();
    }
}
