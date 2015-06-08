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

import com.spatial4j.core.shape.Circle;
import com.vividsolutions.jts.geom.Coordinate;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.DistanceUnit.Distance;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class CircleBuilder extends ShapeBuilder {

    public static final String FIELD_RADIUS = "radius";
    public static final GeoShapeType TYPE = GeoShapeType.CIRCLE;

    private DistanceUnit unit;
    private double radius;
    private Coordinate center;
    
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
     * Set the radius of the circle. The String value will be parsed by {@link DistanceUnit}
     * @param radius Value and unit of the circle combined in a string
     * @return this
     */
    public CircleBuilder radius(String radius) {
        return radius(DistanceUnit.Distance.parseDistance(radius));
    }

    /**
     * Set the radius of the circle
     * @param radius radius of the circle (see {@link DistanceUnit.Distance})
     * @return this
     */
    public CircleBuilder radius(Distance radius) {
        return radius(radius.value, radius.unit);
    }

    /**
     * Set the radius of the circle
     * @param radius value of the circles radius
     * @param unit unit name of the radius value (see {@link DistanceUnit})
     * @return this
     */
    public CircleBuilder radius(double radius, String unit) {
        return radius(radius, DistanceUnit.fromString(unit));
    }

    /**
     * Set the radius of the circle
     * @param radius value of the circles radius
     * @param unit unit of the radius value (see {@link DistanceUnit})
     * @return this
     */
    public CircleBuilder radius(double radius, DistanceUnit unit) {
        this.unit = unit;
        this.radius = radius;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_TYPE, TYPE.shapename);
        builder.field(FIELD_RADIUS, unit.toString(radius));
        builder.field(FIELD_COORDINATES);
        toXContent(builder, center);
        return builder.endObject();
    }

    @Override
    public Circle build() {
        return SPATIAL_CONTEXT.makeCircle(center.x, center.y, 360 * radius / unit.getEarthCircumference());
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }
}
