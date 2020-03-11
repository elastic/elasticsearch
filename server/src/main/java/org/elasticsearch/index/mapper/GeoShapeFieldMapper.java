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
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.VectorGeoShapeQueryProcessor;

import java.util.HashMap;
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

    public static Map<String, DataHandlerFactory> DATA_HANDLER_FACTORIES = new HashMap<>();

    public static class Defaults extends AbstractGeometryFieldMapper.Defaults {
        public static final Explicit<String> DATA_HANDLER = new Explicit<>("default", false);
    }

    public static class Builder extends AbstractGeometryFieldMapper.Builder<AbstractGeometryFieldMapper.Builder, GeoShapeFieldMapper> {
        DataHandler dataHandler;

        public Builder(String name) {
            super (name, new GeoShapeFieldType(), new GeoShapeFieldType());
            this.dataHandler = resolveDataHandler(Defaults.DATA_HANDLER.value());
        }

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new GeoShapeFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context), coerce(context),
                ignoreZValue(), docValues(), context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo);
        }

        @Override
        protected boolean defaultDocValues(Version indexCreated) {
            return dataHandler.defaultDocValues(indexCreated);
        }

        @Override
        public Builder docValues(boolean hasDocValues) {
            return (Builder)super.docValues(dataHandler.supportsDocValues(), hasDocValues);
        }

        @Override
        protected Explicit<Boolean> docValues() {
            // TODO(talevy): see how to best make this pluggable with the DataHandler work
            // although these values can be true, an ElasticsearchParseException
            // prevents this Explicit(true,true) path to ever occur in practice
            if (dataHandler.supportsDocValues()) {
                if (docValuesSet && fieldType.hasDocValues()) {
                    return new Explicit<>(true, true);
                } else if (docValuesSet) {
                    return new Explicit<>(false, true);
                }
            }
            return AbstractGeometryFieldMapper.Defaults.DOC_VALUES;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            GeoShapeFieldType fieldType = (GeoShapeFieldType)fieldType();
            boolean orientation = fieldType.orientation == ShapeBuilder.Orientation.RIGHT;

            // @todo the GeometryParser can be static since it doesn't hold state?
            GeometryParser geometryParser = new GeometryParser(orientation, coerce(context).value(), ignoreZValue().value());
            fieldType.setGeometryParser( (parser, mapper) -> geometryParser.parse(parser));

            fieldType.setGeometryIndexer(dataHandler.newIndexer(orientation, fieldType));
            fieldType.setGeometryQueryBuilder(dataHandler.newQueryProcessor());
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
                               Explicit<Boolean> ignoreZValue, Explicit<Boolean> docValues, Settings indexSettings,
                               MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, ignoreZValue, docValues, indexSettings,
            multiFields, copyTo);
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        if (mergeWith instanceof LegacyGeoShapeFieldMapper) {
            LegacyGeoShapeFieldMapper legacy = (LegacyGeoShapeFieldMapper) mergeWith;
            throw new IllegalArgumentException("[" + fieldType().name() + "] with field mapper [" + fieldType().typeName() + "] " +
                "using [BKD] strategy cannot be merged with " + "[" + legacy.fieldType().typeName() + "] with [" +
                legacy.fieldType().strategy() + "] strategy");
        }
        super.doMerge(mergeWith);
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static void registerDataHandlers(Map<String, DataHandlerFactory> dataHandlerFactories) {
        DATA_HANDLER_FACTORIES.putAll(dataHandlerFactories);
    }

    public static DataHandler resolveDataHandler(String handlerKey) {
        if (DATA_HANDLER_FACTORIES.containsKey(handlerKey)) {
            return DATA_HANDLER_FACTORIES.get(handlerKey).newDataHandler();
        }
        throw new IllegalArgumentException("dataHandler [" + handlerKey + "] not supported");
    }

    public interface DataHandlerFactory {
        DataHandler newDataHandler();
    }

    public abstract static class DataHandler {
        public abstract Indexer newIndexer(boolean orientation, MappedFieldType fieldType);
        public abstract QueryProcessor newQueryProcessor();
        public abstract boolean defaultDocValues(Version indexCreatedVersion);
        public abstract boolean supportsDocValues();
    }

    static {
        DATA_HANDLER_FACTORIES.put(Defaults.DATA_HANDLER.value(), () ->
            new DataHandler() {
                @Override
                public boolean supportsDocValues() {
                    return false;
                }

                @Override
                public boolean defaultDocValues(Version indexCreatedVersion) {
                    return false;
                }

                @Override
                public Indexer newIndexer(boolean orientation, MappedFieldType fieldType) {
                    return new GeoShapeIndexer(orientation, fieldType.name());
                }

                @Override
                public QueryProcessor newQueryProcessor() {
                    return new VectorGeoShapeQueryProcessor();
                }
            });
    }

    public interface Extension {
        Map<String, DataHandlerFactory> getDataHandlerFactories();
    }
}
