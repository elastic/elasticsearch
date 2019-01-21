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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
public class GeoShapeFieldMapper extends BaseGeoShapeFieldMapper {

    public static class Builder extends BaseGeoShapeFieldMapper.Builder<BaseGeoShapeFieldMapper.Builder, GeoShapeFieldMapper> {
        public Builder(String name) {
            super (name, new GeoShapeFieldType(), new GeoShapeFieldType());
        }

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new GeoShapeFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context), coerce(context),
                ignoreZValue(), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static final class GeoShapeFieldType extends BaseGeoShapeFieldType {
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

    /** parsing logic for {@link LatLonShape} indexing */
    @Override
    public void parse(ParseContext context) throws IOException {
        try {
            Object shape = context.parseExternalValue(Object.class);
            if (shape == null) {
                ShapeBuilder shapeBuilder = ShapeParser.parse(context.parser(), this);
                if (shapeBuilder == null) {
                    return;
                }
                shape = shapeBuilder.buildLucene();
            }
            indexShape(context, shape);
        } catch (Exception e) {
            if (ignoreMalformed.value() == false) {
                throw new MapperParsingException("failed to parse field [{}] of type [{}]", e, fieldType().name(),
                    fieldType().typeName());
            }
            context.addIgnoredField(fieldType().name());
        }
    }

    private void indexShape(ParseContext context, Object luceneShape) {
        if (luceneShape instanceof GeoPoint) {
            GeoPoint pt = (GeoPoint) luceneShape;
            indexFields(context, LatLonShape.createIndexableFields(name(), pt.lat(), pt.lon()));
        } else if (luceneShape instanceof double[]) {
            double[] pt = (double[]) luceneShape;
            indexFields(context, LatLonShape.createIndexableFields(name(), pt[1], pt[0]));
        } else if (luceneShape instanceof Line) {
            indexFields(context, LatLonShape.createIndexableFields(name(), (Line)luceneShape));
        } else if (luceneShape instanceof Polygon) {
            indexFields(context, LatLonShape.createIndexableFields(name(), (Polygon) luceneShape));
        } else if (luceneShape instanceof double[][]) {
            double[][] pts = (double[][])luceneShape;
            for (int i = 0; i < pts.length; ++i) {
                indexFields(context, LatLonShape.createIndexableFields(name(), pts[i][1], pts[i][0]));
            }
        } else if (luceneShape instanceof Line[]) {
            Line[] lines = (Line[]) luceneShape;
            for (int i = 0; i < lines.length; ++i) {
                indexFields(context, LatLonShape.createIndexableFields(name(), lines[i]));
            }
        } else if (luceneShape instanceof Polygon[]) {
            Polygon[] polys = (Polygon[]) luceneShape;
            for (int i = 0; i < polys.length; ++i) {
                indexFields(context, LatLonShape.createIndexableFields(name(), polys[i]));
            }
        } else if (luceneShape instanceof Rectangle) {
            // index rectangle as a polygon
            Rectangle r = (Rectangle) luceneShape;
            Polygon p = new Polygon(new double[]{r.minLat, r.minLat, r.maxLat, r.maxLat, r.minLat},
                new double[]{r.minLon, r.maxLon, r.maxLon, r.minLon, r.minLon});
            indexFields(context, LatLonShape.createIndexableFields(name(), p));
        } else if (luceneShape instanceof Object[]) {
            // recurse to index geometry collection
            for (Object o : (Object[])luceneShape) {
                indexShape(context, o);
            }
        } else {
            throw new IllegalArgumentException("invalid shape type found [" + luceneShape.getClass() + "] while indexing shape");
        }
    }

    private void indexFields(ParseContext context, Field[] fields) {
        ArrayList<IndexableField> flist = new ArrayList<>(Arrays.asList(fields));
        createFieldNamesField(context, flist);
        for (IndexableField f : flist) {
            context.doc().add(f);
        }
    }
}
