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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.geo.XShapeCollection;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.spatial4j.core.shape.Shape;
import com.vividsolutions.jts.geom.Coordinate;

public class MultiPolygonBuilder extends ShapeBuilder {

    public static final GeoShapeType TYPE = GeoShapeType.MULTIPOLYGON;

    protected final ArrayList<BasePolygonBuilder<?>> polygons = new ArrayList<>();

    public MultiPolygonBuilder() {
        this(Orientation.RIGHT);
    }

    public MultiPolygonBuilder(Orientation orientation) {
        super(orientation);
    }

    public MultiPolygonBuilder polygon(BasePolygonBuilder<?> polygon) {
        this.polygons.add(polygon);
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_TYPE, TYPE.shapename);
        builder.startArray(FIELD_COORDINATES);
        for(BasePolygonBuilder<?> polygon : polygons) {
            builder.startArray();
            polygon.coordinatesArray(builder, params);
            builder.endArray();
        }
        builder.endArray();
        return builder.endObject();
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public Shape build() {

        List<Shape> shapes = new ArrayList<>(this.polygons.size());

        if(wrapdateline) {
            for (BasePolygonBuilder<?> polygon : this.polygons) {
                for(Coordinate[][] part : polygon.coordinates()) {
                    shapes.add(jtsGeometry(PolygonBuilder.polygon(FACTORY, part)));
                }
            }
        } else {
            for (BasePolygonBuilder<?> polygon : this.polygons) {
                shapes.add(jtsGeometry(polygon.toPolygon(FACTORY)));
            }
        }
        if (shapes.size() == 1)
            return shapes.get(0);
        else
            return new XShapeCollection<>(shapes, SPATIAL_CONTEXT);
        //note: ShapeCollection is probably faster than a Multi* geom.
    }


}
