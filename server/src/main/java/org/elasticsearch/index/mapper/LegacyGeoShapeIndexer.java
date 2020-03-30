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

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.geo.XShapeCollection;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LegacyGeoShapeIndexer implements AbstractGeometryFieldMapper.Indexer<ShapeBuilder<?, ?, ?>, Shape> {

    private LegacyGeoShapeFieldMapper.GeoShapeFieldType fieldType;

    public LegacyGeoShapeIndexer(LegacyGeoShapeFieldMapper.GeoShapeFieldType fieldType) {
        this.fieldType = fieldType;
    }


    @Override
    public Shape prepareForIndexing(ShapeBuilder<?, ?, ?> shapeBuilder) {
        return shapeBuilder.buildS4J();
    }

    @Override
    public Class<Shape> processedClass() {
        return Shape.class;
    }

    @Override
    public List<IndexableField>  indexShape(ParseContext context, Shape shape) {
        if (fieldType.pointsOnly()) {
            // index configured for pointsOnly
            if (shape instanceof XShapeCollection && XShapeCollection.class.cast(shape).pointsOnly()) {
                // MULTIPOINT data: index each point separately
                @SuppressWarnings("unchecked") List<Shape> shapes = ((XShapeCollection) shape).getShapes();
                List<IndexableField> fields = new ArrayList<>();
                for (Shape s : shapes) {
                    fields.addAll(Arrays.asList(fieldType.defaultPrefixTreeStrategy().createIndexableFields(s)));
                }
                return fields;
            } else if (shape instanceof Point == false) {
                throw new MapperParsingException("[{" + fieldType.name() + "}] is configured for points only but a "
                    + ((shape instanceof JtsGeometry) ? ((JtsGeometry)shape).getGeom().getGeometryType() : shape.getClass())
                    + " was found");
            }
        }
        return Arrays.asList(fieldType.defaultPrefixTreeStrategy().createIndexableFields(shape));
    }
}
