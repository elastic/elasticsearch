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

package org.elasticsearch.common.geo.builders;

import com.spatial4j.core.shape.Rectangle;
import com.vividsolutions.jts.geom.Coordinate;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class EnvelopeBuilder extends ShapeBuilder {

    public static final GeoShapeType TYPE = GeoShapeType.ENVELOPE; 

    protected Coordinate topLeft;
    protected Coordinate bottomRight;

    public EnvelopeBuilder() {
        this(Orientation.RIGHT);
    }

    public EnvelopeBuilder(Orientation orientation) {
        super(orientation);
    }

    public EnvelopeBuilder topLeft(Coordinate topLeft) {
        this.topLeft = topLeft;
        return this;
    }

    public EnvelopeBuilder topLeft(double longitude, double latitude) {
        return topLeft(coordinate(longitude, latitude));
    }

    public EnvelopeBuilder bottomRight(Coordinate bottomRight) {
        this.bottomRight = bottomRight;
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
        toXContent(builder, topLeft);
        toXContent(builder, bottomRight);
        builder.endArray();
        return builder.endObject();
    }

    @Override
    public Rectangle build() {
        return SPATIAL_CONTEXT.makeRectangle(topLeft.x, bottomRight.x, bottomRight.y, topLeft.y);
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }
}
