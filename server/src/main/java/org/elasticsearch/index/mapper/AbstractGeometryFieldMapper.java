/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Base field mapper class for all spatial field types
 */
public abstract class AbstractGeometryFieldMapper extends FieldMapper {

    public static class Names {
        public static final ParseField IGNORE_MALFORMED = new ParseField("ignore_malformed");
        public static final ParseField IGNORE_Z_VALUE = new ParseField("ignore_z_value");
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Boolean> IGNORE_Z_VALUE = new Explicit<>(true, false);
    }

    public abstract static class Builder<T extends Builder, Y extends AbstractGeometryFieldMapper>
            extends FieldMapper.Builder<T, Y> {
        protected Boolean ignoreMalformed;
        protected Boolean ignoreZValue;

        public Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType) {
            super(name, fieldType, defaultFieldType);
        }

        public Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType, boolean ignoreMalformed,
                       boolean ignoreZValue) {
            super(name, fieldType, defaultFieldType);
            this.ignoreMalformed = ignoreMalformed;
            this.ignoreZValue = ignoreZValue;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return this;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }

        public Explicit<Boolean> ignoreMalformed() {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            return AbstractShapeGeometryFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        protected Explicit<Boolean> ignoreZValue(BuilderContext context) {
            if (ignoreZValue != null) {
                return new Explicit<>(ignoreZValue, true);
            }
            return Defaults.IGNORE_Z_VALUE;
        }

        public Explicit<Boolean> ignoreZValue() {
            if (ignoreZValue != null) {
                return new Explicit<>(ignoreZValue, true);
            }
            return AbstractShapeGeometryFieldMapper.Defaults.IGNORE_Z_VALUE;
        }

        public Builder ignoreZValue(final boolean ignoreZValue) {
            this.ignoreZValue = ignoreZValue;
            return this;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            // field mapper handles this at build time
            // but prefix tree strategies require a name, so throw a similar exception
            if (name().isEmpty()) {
                throw new IllegalArgumentException("name cannot be empty string");
            }
        }
    }

    public abstract static class TypeParser<T extends Builder> implements Mapper.TypeParser {
        protected abstract T newBuilder(String name, Map<String, Object> params);

        public T parse(String name, Map<String, Object> node, Map<String, Object> params, ParserContext parserContext) {
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();

                if (Names.IGNORE_MALFORMED.match(propName, LoggingDeprecationHandler.INSTANCE)) {
                    params.put(Names.IGNORE_MALFORMED.getPreferredName(), XContentMapValues.nodeBooleanValue(propNode,
                        name + ".ignore_malformed"));
                    iterator.remove();
                } else if (Names.IGNORE_Z_VALUE.getPreferredName().equals(propName)) {
                    params.put(GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName(),
                        XContentMapValues.nodeBooleanValue(propNode, name + "." + Names.IGNORE_Z_VALUE.getPreferredName()));
                    iterator.remove();
                }
            }
            T builder = newBuilder(name, params);
            if (params.containsKey(GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName())) {
                builder.ignoreZValue((Boolean)params.get(GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName()));
            }

            if (params.containsKey(Names.IGNORE_MALFORMED.getPreferredName())) {
                builder.ignoreMalformed((Boolean)params.get(Names.IGNORE_MALFORMED.getPreferredName()));
            }
            return builder;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public T parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            Map<String, Object> params = new HashMap<>();
            return parse(name, node, params, parserContext);
        }
    }

    public abstract static class AbstractGeometryFieldType extends MappedFieldType {
        protected QueryProcessor geometryQueryBuilder;

        protected AbstractGeometryFieldType() {
            setIndexOptions(IndexOptions.DOCS);
            setTokenized(false);
            setStored(false);
            setStoreTermVectors(false);
            setOmitNorms(true);
        }

        protected AbstractGeometryFieldType(AbstractGeometryFieldType ref) {
            super(ref);
        }

        public void setGeometryQueryBuilder(QueryProcessor geometryQueryBuilder)  {
            this.geometryQueryBuilder = geometryQueryBuilder;
        }

        public QueryProcessor geometryQueryBuilder() {
            return geometryQueryBuilder;
        }

        /**
         * interface representing a query builder that generates a query from the given geometry
         */
        public interface QueryProcessor {
            Query process(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context);

            @Deprecated
            default Query process(Geometry shape, String fieldName, SpatialStrategy strategy, ShapeRelation relation,
                                  QueryShardContext context) {
                return process(shape, fieldName, relation, context);
            }
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context,
                "Geometry fields do not support exact searching, use dedicated geometry queries instead: ["
                + name() + "]");
        }
    }

    protected Explicit<Boolean> ignoreMalformed;
    protected Explicit<Boolean> ignoreZValue;

    protected AbstractGeometryFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                          Settings indexSettings, Explicit<Boolean> ignoreMalformed,
                                          Explicit<Boolean> ignoreZValue, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.ignoreZValue = ignoreZValue;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        AbstractGeometryFieldMapper gsfm = (AbstractGeometryFieldMapper)mergeWith;

        if (gsfm.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gsfm.ignoreMalformed;
        }
        if (gsfm.ignoreZValue.explicit()) {
            this.ignoreZValue = gsfm.ignoreZValue;
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED.getPreferredName(), ignoreMalformed.value());
        }
        if (includeDefaults || ignoreZValue.explicit()) {
            builder.field(Names.IGNORE_Z_VALUE.getPreferredName(), ignoreZValue.value());
        }
    }

    public Explicit<Boolean> ignoreMalformed() {
        return ignoreMalformed;
    }

    public Explicit<Boolean> ignoreZValue() {
        return ignoreZValue;
    }
}
