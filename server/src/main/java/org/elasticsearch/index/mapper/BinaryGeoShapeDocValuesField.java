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
package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.geo.GeoShapeCoordinateEncoder;
import org.elasticsearch.common.geo.TriangleTreeWriter;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BinaryGeoShapeDocValuesField extends CustomDocValuesField {

    private List<Geometry> geometries;

    public BinaryGeoShapeDocValuesField(String name, Geometry geometry) {
        super(name);
        this.geometries = new ArrayList<>(1);
        add(geometry);
    }

    public void add(Geometry geometry) {
        geometries.add(geometry);
    }

    @Override
    public BytesRef binaryValue() {
        try {
            final Geometry geometry;
            if (geometries.size() > 1) {
                geometry = new GeometryCollection(geometries);
            } else {
                geometry = geometries.get(0);
            }
            final TriangleTreeWriter writer = new TriangleTreeWriter(geometry, GeoShapeCoordinateEncoder.INSTANCE);
            BytesStreamOutput output = new BytesStreamOutput();
            writer.writeTo(output);
            return output.bytes().toBytesRef();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to encode shape", e);
        }
    }
}
