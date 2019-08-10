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

import org.apache.lucene.document.LatLonShape;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.VectorGeoShapeQueryProcessor;

/**
 * FieldMapper for indexing {@link LatLonShape}s.
 * <p>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.elasticsearch.index.query.GeoShapeQueryBuilder}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 * <p>
 * or:
 * <p>
 * "field" : "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))
 */
public class GeoShapeFieldMapper extends AbstractGeometryFieldMapper<Geometry, Geometry> {
    public static final String CONTENT_TYPE = "geo_shape";

    public static class Builder extends AbstractGeometryFieldMapper.Builder<AbstractGeometryFieldMapper.Builder, GeoShapeFieldMapper> {
        public Builder(String name) {
            super (name, new GeoShapeFieldType(), new GeoShapeFieldType());
        }

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new GeoShapeFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context), coerce(context),
                ignoreZValue(), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            GeoShapeFieldType fieldType = (GeoShapeFieldType)fieldType();
            boolean orientation = fieldType.orientation == ShapeBuilder.Orientation.RIGHT;

            GeometryParser geometryParser = new GeometryParser(orientation, coerce(context).value(), ignoreZValue().value());

            fieldType.setGeometryIndexer(new GeoShapeIndexer(orientation, fieldType.name()));
            fieldType.setGeometryParser( (parser, mapper) -> geometryParser.parse(parser));
            fieldType.setGeometryQueryBuilder(new VectorGeoShapeQueryProcessor());
        }
    }

    public static final class GeoShapeFieldType extends AbstractGeometryFieldType<Geometry, Geometry> {
        public GeoShapeFieldType() {
            super();
        }

        protected GeoShapeFieldType(GeoShapeFieldType ref) {
            super(ref);
        }

        @Override
        public GeoShapeFieldType clone() {
            return new GeoShapeFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    public GeoShapeFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                               Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                               Explicit<Boolean> ignoreZValue, Settings indexSettings,
                               MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, ignoreZValue, indexSettings,
            multiFields, copyTo);
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
