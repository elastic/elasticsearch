/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
