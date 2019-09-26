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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.VectorGeoShapeQueryProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    public static List<CRSHandlerFactory> CRS_HANDLER_FACTORIES = new ArrayList<>();

    public static class Defaults extends AbstractGeometryFieldMapper.Defaults {
        public static final Explicit<String> CRS = new Explicit<>("EPSG:4326", false);
    }

    public static class Builder extends AbstractGeometryFieldMapper.Builder<AbstractGeometryFieldMapper.Builder, GeoShapeFieldMapper> {
        CRSHandler crsHandler;
        protected String crs;

        public Builder(String name) {
            super (name, new GeoShapeFieldType(), new GeoShapeFieldType());
        }

        public Builder(String name, Map<String, Object> params) {
            this(name);
            this.crs = params.containsKey("crs") ? (String)params.get("crs") : Defaults.CRS.value();
            this.crsHandler = resolveCRSHandler(this.crs().value());
        }

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new GeoShapeFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context), coerce(context),
                ignoreZValue(), crs(), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }

        protected Explicit<String> crs() {
            if (crs != null) {
                return new Explicit<>(crs, true);
            }
            return Defaults.CRS;
        }

        public Builder crs(final String crs) {
            this.crs = crs;
            return this;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            GeoShapeFieldType fieldType = (GeoShapeFieldType)fieldType();
            boolean orientation = fieldType.orientation == ShapeBuilder.Orientation.RIGHT;

            // @todo the GeometryParser can be static since it doesn't hold state?
            GeometryParser geometryParser = new GeometryParser(orientation, coerce(context).value(), ignoreZValue().value());
            fieldType.setGeometryParser( (parser, mapper) -> geometryParser.parse(parser));

            fieldType.setGeometryIndexer(crsHandler.newIndexer(orientation, fieldType.name()));
            fieldType.setGeometryQueryBuilder(crsHandler.newQueryProcessor());
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

    protected Explicit<String> crs;

    public GeoShapeFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                               Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                               Explicit<Boolean> ignoreZValue, Explicit<String> crs, Settings indexSettings,
                               MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, ignoreZValue, indexSettings,
            multiFields, copyTo);
        this.crs = crs;
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public Explicit<String> crs() {
        return crs;
    }

    @Override
    public void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || crs.explicit()) {
            builder.startObject("crs")
                .field("type", "name")
                .startObject("properties")
                .field("name", crs.value())
                .endObject()
                .endObject();
        }
    }

    public static void registerCRSHandlers(List<CRSHandlerFactory> crsHandlerFactories) {
        CRS_HANDLER_FACTORIES.addAll(crsHandlerFactories);
    }

    public static CRSHandler resolveCRSHandler(String crs) {
        CRSHandler crsHandler;
        for (CRSHandlerFactory factory : CRS_HANDLER_FACTORIES) {
            if ((crsHandler = factory.newCRSHandler(crs)) != null) {
                return crsHandler;
            }
        }
        throw new IllegalArgumentException("crs [" + crs + "] not supported");
    }

    public interface CRSHandlerFactory {
        CRSHandler newCRSHandler(String crs);
    }

    public interface CRSHandler {
        Indexer newIndexer(boolean orientation, String fieldName);
        QueryProcessor newQueryProcessor();

        Object resolveCRS(String crsSpec);
    }

    public static CRSHandlerFactory DEFAULT_CRS_HANDLER_FACTORY = new CRSHandlerFactory() {
        @Override
        public CRSHandler newCRSHandler(String crs) {
            if (crs.equals(Defaults.CRS.value())) {
                return new DefaultCRSHandler();
            }
            return null;
        }
    };

    protected static class DefaultCRSHandler implements CRSHandler {
        @Override
        public Indexer newIndexer(boolean orientation, String fieldName) {
            return new GeoShapeIndexer(orientation, fieldName);
        }

        @Override
        public QueryProcessor newQueryProcessor() {
            return new VectorGeoShapeQueryProcessor();
        }

        @Override
        public Object resolveCRS(String crsSpec) {
            throw new ElasticsearchException("resolveCRS not supported for default CRSHandler");
        }
    };

    static {
        CRS_HANDLER_FACTORIES.add(DEFAULT_CRS_HANDLER_FACTORY);
    }
}
