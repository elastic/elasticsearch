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

package org.elasticsearch.common.geo.builders;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;

import com.spatial4j.core.shape.Rectangle;
import com.vividsolutions.jts.geom.Coordinate;

public class EnvelopeBuilder extends ShapeBuilder {

    public static final GeoShapeType TYPE = GeoShapeType.ENVELOPE; 

    protected Coordinate northEast;
    protected Coordinate southWest;
    
    public EnvelopeBuilder topLeft(Coordinate northEast) {
        this.northEast = northEast;
        return this;
    }

    public EnvelopeBuilder topLeft(double longitude, double latitude) {
        return topLeft(coordinate(longitude, latitude));
    }

    public EnvelopeBuilder bottomRight(Coordinate southWest) {
        this.southWest = southWest;
        return this;
    }

    public EnvelopeBuilder bottomRight(double longitude, double latitude) {
        return bottomRight(coordinate(longitude, latitude));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_TYPE, TYPE.shapename);
        builder.startArray(FIELD_COORDINATES);
        toXContent(builder, northEast);
        toXContent(builder, southWest);
        builder.endArray();
        return builder.endObject();
    }

    @Override
    public Rectangle build() {
        return SPATIAL_CONTEXT.makeRectangle(
                northEast.x, southWest.x,
                southWest.y, northEast.y);
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }
}
