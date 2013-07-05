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
import java.util.ArrayList;
import java.util.Iterator;

import org.elasticsearch.common.xcontent.XContentBuilder;

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

public class MultiPolygonBuilder extends ShapeBuilder {

    public static final GeoShapeType TYPE = GeoShapeType.MULTIPOLYGON;

    protected final ArrayList<BasePolygonBuilder<?>> polygons = new ArrayList<BasePolygonBuilder<?>>();

    public MultiPolygonBuilder polygon(BasePolygonBuilder<?> polygon) {
        this.polygons.add(polygon);
        return this;
    }

    public InternalPolygonBuilder polygon() {
        InternalPolygonBuilder polygon = new InternalPolygonBuilder(this);
        this.polygon(polygon);
        return polygon;
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

        Polygon[] polygons;
        
        if(wrapdateline) {
            ArrayList<Polygon> polygonSet = new ArrayList<Polygon>(this.polygons.size());
            for (BasePolygonBuilder<?> polygon : this.polygons) {
                for(Coordinate[][] part : polygon.coordinates()) {
                    polygonSet.add(PolygonBuilder.polygon(FACTORY, part));
                }
            }

            polygons = polygonSet.toArray(new Polygon[polygonSet.size()]);
        } else {
            polygons = new Polygon[this.polygons.size()];
            Iterator<BasePolygonBuilder<?>> iterator = this.polygons.iterator();
            for (int i = 0; iterator.hasNext(); i++) {
                polygons[i] = iterator.next().toPolygon(FACTORY);
            }
        }

        Geometry geometry = polygons.length == 1
                ? polygons[0]
                : FACTORY.createMultiPolygon(polygons);

        return new JtsGeometry(geometry, SPATIAL_CONTEXT, !wrapdateline);
    }

    public static class InternalPolygonBuilder extends BasePolygonBuilder<InternalPolygonBuilder> {

        private final MultiPolygonBuilder collection;

        private InternalPolygonBuilder(MultiPolygonBuilder collection) {
            super();
            this.collection = collection;
            this.shell = new Ring<InternalPolygonBuilder>(this);
        }

        @Override
        public MultiPolygonBuilder close() {
            super.close();
            return collection;
        }
    }
}
