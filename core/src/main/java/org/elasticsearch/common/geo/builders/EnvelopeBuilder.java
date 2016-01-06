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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class EnvelopeBuilder extends ShapeBuilder {

    public static final GeoShapeType TYPE = GeoShapeType.ENVELOPE;

    public static final EnvelopeBuilder PROTOTYPE = new EnvelopeBuilder();

    private Coordinate topLeft;
    private Coordinate bottomRight;

    public EnvelopeBuilder topLeft(Coordinate topLeft) {
        this.topLeft = topLeft;
        return this;
    }

    public EnvelopeBuilder topLeft(double longitude, double latitude) {
        return topLeft(coordinate(longitude, latitude));
    }

    public Coordinate topLeft() {
        return this.topLeft;
    }

    public EnvelopeBuilder bottomRight(Coordinate bottomRight) {
        this.bottomRight = bottomRight;
        return this;
    }

    public EnvelopeBuilder bottomRight(double longitude, double latitude) {
        return bottomRight(coordinate(longitude, latitude));
    }

    public Coordinate bottomRight() {
        return this.bottomRight;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_TYPE, TYPE.shapeName());
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

    @Override
    public int hashCode() {
        return Objects.hash(topLeft, bottomRight);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EnvelopeBuilder other = (EnvelopeBuilder) obj;
        return Objects.equals(topLeft, other.topLeft) &&
                Objects.equals(bottomRight, other.bottomRight);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeCoordinateTo(topLeft, out);
        writeCoordinateTo(bottomRight, out);
    }

    @Override
    public EnvelopeBuilder readFrom(StreamInput in) throws IOException {
        return new EnvelopeBuilder()
                .topLeft(readCoordinateFrom(in))
                .bottomRight(readCoordinateFrom(in));
    }
}
