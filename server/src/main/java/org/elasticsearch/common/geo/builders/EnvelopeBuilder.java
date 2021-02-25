/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo.builders;

import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.parsers.GeoWKTParser;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.jts.geom.Coordinate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class EnvelopeBuilder extends ShapeBuilder<Rectangle, org.elasticsearch.geometry.Rectangle, EnvelopeBuilder> {

    public static final GeoShapeType TYPE = GeoShapeType.ENVELOPE;

    private final Coordinate topLeft;
    private final Coordinate bottomRight;

    /**
     * Build an envelope from the top left and bottom right coordinates.
     */
    public EnvelopeBuilder(Coordinate topLeft, Coordinate bottomRight) {
        Objects.requireNonNull(topLeft, "topLeft of envelope cannot be null");
        Objects.requireNonNull(bottomRight, "bottomRight of envelope cannot be null");
        if (Double.isNaN(topLeft.z) != Double.isNaN(bottomRight.z)) {
            throw new IllegalArgumentException("expected same number of dimensions for topLeft and bottomRight");
        }
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
    }

    /**
     * Read from a stream.
     */
    public EnvelopeBuilder(StreamInput in) throws IOException {
        topLeft = readFromStream(in);
        bottomRight = readFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeCoordinateTo(topLeft, out);
        writeCoordinateTo(bottomRight, out);
    }

    public Coordinate topLeft() {
        return this.topLeft;
    }

    public Coordinate bottomRight() {
        return this.bottomRight;
    }

    @Override
    protected StringBuilder contentToWKT() {
        StringBuilder sb = new StringBuilder();

        sb.append(GeoWKTParser.LPAREN);
        // minX, maxX, maxY, minY
        sb.append(topLeft.x);
        sb.append(GeoWKTParser.COMMA);
        sb.append(GeoWKTParser.SPACE);
        sb.append(bottomRight.x);
        sb.append(GeoWKTParser.COMMA);
        sb.append(GeoWKTParser.SPACE);
        // TODO support Z??
        sb.append(topLeft.y);
        sb.append(GeoWKTParser.COMMA);
        sb.append(GeoWKTParser.SPACE);
        sb.append(bottomRight.y);
        sb.append(GeoWKTParser.RPAREN);

        return sb;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
        builder.startArray(ShapeParser.FIELD_COORDINATES.getPreferredName());
        toXContent(builder, topLeft);
        toXContent(builder, bottomRight);
        builder.endArray();
        return builder.endObject();
    }

    @Override
    public Rectangle buildS4J() {
        return SPATIAL_CONTEXT.makeRectangle(topLeft.x, bottomRight.x, bottomRight.y, topLeft.y);
    }

    @Override
    public org.elasticsearch.geometry.Rectangle buildGeometry() {
        return new org.elasticsearch.geometry.Rectangle(topLeft.x, bottomRight.x, topLeft.y, bottomRight.y);
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public int numDimensions() {
        return Double.isNaN(topLeft.z) ? 2 : 3;
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
}
