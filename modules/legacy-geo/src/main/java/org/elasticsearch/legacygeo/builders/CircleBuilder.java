/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo.builders;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.DistanceUnit.Distance;
import org.elasticsearch.legacygeo.GeoShapeType;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Circle;

import java.io.IOException;
import java.util.Objects;

public class CircleBuilder extends ShapeBuilder<Circle, org.elasticsearch.geometry.Circle, CircleBuilder> {

    public static final ParseField FIELD_RADIUS = new ParseField("radius");
    public static final GeoShapeType TYPE = GeoShapeType.CIRCLE;

    private DistanceUnit unit = DistanceUnit.DEFAULT;
    private double radius;
    private Coordinate center;

    /**
     * Creates a circle centered at [0.0, 0.0].
     * Center can be changed by calling {@link #center(Coordinate)} later.
     */
    public CircleBuilder() {
        this.center = ZERO_ZERO;
    }

    /**
     * Read from a stream.
     */
    public CircleBuilder(StreamInput in) throws IOException {
        center(readFromStream(in));
        radius(in.readDouble(), DistanceUnit.readFromStream(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeCoordinateTo(center, out);
        out.writeDouble(radius);
        unit.writeTo(out);
    }

    /**
     * Set the center of the circle
     *
     * @param center coordinate of the circles center
     * @return this
     */
    public CircleBuilder center(Coordinate center) {
        this.center = center;
        return this;
    }

    /**
     * set the center of the circle
     * @param lon longitude of the center
     * @param lat latitude of the center
     * @return this
     */
    public CircleBuilder center(double lon, double lat) {
        return center(new Coordinate(lon, lat));
    }

    /**
     * Get the center of the circle
     */
    public Coordinate center() {
        return center;
    }

    /**
     * Set the radius of the circle. The String value will be parsed by {@link DistanceUnit}
     * @param radius Value and unit of the circle combined in a string
     * @return this
     */
    public CircleBuilder radius(String radius) {
        return radius(DistanceUnit.Distance.parseDistance(radius));
    }

    /**
     * Set the radius of the circle
     * @param radius radius of the circle (see {@link org.elasticsearch.common.unit.DistanceUnit.Distance})
     * @return this
     */
    public CircleBuilder radius(Distance radius) {
        return radius(radius.value, radius.unit);
    }

    /**
     * Set the radius of the circle
     * @param radiusValue value of the circles radius
     * @param unitValue unit name of the radius value (see {@link DistanceUnit})
     * @return this
     */
    public CircleBuilder radius(double radiusValue, String unitValue) {
        return radius(radiusValue, DistanceUnit.fromString(unitValue));
    }

    /**
     * Set the radius of the circle
     * @param radiusValue value of the circles radius
     * @param unitValue unit of the radius value (see {@link DistanceUnit})
     * @return this
     */
    public CircleBuilder radius(double radiusValue, DistanceUnit unitValue) {
        this.unit = unitValue;
        this.radius = radiusValue;
        return this;
    }

    /**
     * Get the radius of the circle without unit
     */
    public double radius() {
        return this.radius;
    }

    /**
     * Get the radius unit of the circle
     */
    public DistanceUnit unit() {
        return this.unit;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
        builder.field(FIELD_RADIUS.getPreferredName(), unit.toString(radius));
        builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
        toXContent(builder, center);
        return builder.endObject();
    }

    @Override
    public Circle buildS4J() {
        return SPATIAL_CONTEXT.makeCircle(center.x, center.y, 360 * radius / unit.getEarthCircumference());
    }

    @Override
    public org.elasticsearch.geometry.Circle buildGeometry() {
        return new org.elasticsearch.geometry.Circle(center.x, center.y, unit.toMeters(radius));
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public String toWKT() {
        throw new UnsupportedOperationException("The WKT spec does not support CIRCLE geometry");
    }

    public int numDimensions() {
        return Double.isNaN(center.z) ? 2 : 3;
    }

    @Override
    public int hashCode() {
        return Objects.hash(center, radius, unit.ordinal());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CircleBuilder other = (CircleBuilder) obj;
        return Objects.equals(center, other.center)
            && Objects.equals(radius, other.radius)
            && Objects.equals(unit.ordinal(), other.unit.ordinal());
    }
}
