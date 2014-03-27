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

import org.elasticsearch.common.xcontent.XContentBuilder;

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class MultiLineStringBuilder extends ShapeBuilder {

    public static final GeoShapeType TYPE = GeoShapeType.MULTILINESTRING;

    private final ArrayList<BaseLineStringBuilder<?>> lines = new ArrayList<>();

    public InternalLineStringBuilder linestring() {
        InternalLineStringBuilder line = new InternalLineStringBuilder(this);
        this.lines.add(line);
        return line;
    }

    public MultiLineStringBuilder linestring(BaseLineStringBuilder<?> line) {
        this.lines.add(line);
        return this;
    }

    public Coordinate[][] coordinates() {
        Coordinate[][] result = new Coordinate[lines.size()][];
        for (int i = 0; i < result.length; i++) {
            result[i] = lines.get(i).coordinates(false);
        }
        return result;
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_TYPE, TYPE.shapename);
        builder.field(FIELD_COORDINATES);
        builder.startArray();
        for(BaseLineStringBuilder<?> line : lines) {
            line.coordinatesToXcontent(builder, false);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public Shape build() {
        final Geometry geometry;
        if(wrapdateline) {
            ArrayList<LineString> parts = new ArrayList<>();
            for (BaseLineStringBuilder<?> line : lines) {
                BaseLineStringBuilder.decompose(FACTORY, line.coordinates(false), parts);
            }
            if(parts.size() == 1) {
                geometry = parts.get(0);
            } else {
                LineString[] lineStrings = parts.toArray(new LineString[parts.size()]);
                geometry = FACTORY.createMultiLineString(lineStrings);
            }
        } else {
            LineString[] lineStrings = new LineString[lines.size()];
            Iterator<BaseLineStringBuilder<?>> iterator = lines.iterator();
            for (int i = 0; iterator.hasNext(); i++) {
                lineStrings[i] = FACTORY.createLineString(iterator.next().coordinates(false));
            }
            geometry = FACTORY.createMultiLineString(lineStrings);
        }
        return jtsGeometry(geometry);
    }

    public static class InternalLineStringBuilder extends BaseLineStringBuilder<InternalLineStringBuilder> {

        private final MultiLineStringBuilder collection;
        
        public InternalLineStringBuilder(MultiLineStringBuilder collection) {
            super();
            this.collection = collection;
        }
        
        public MultiLineStringBuilder end() {
            return collection;
        }

        public Coordinate[] coordinates() {
            return super.coordinates(false);
        }

        @Override
        public GeoShapeType type() {
            return null;
        }
    }
}
