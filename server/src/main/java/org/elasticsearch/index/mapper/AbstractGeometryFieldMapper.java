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

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.CentroidCalculator;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper.DeprecatedParameters;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.GeoPointFieldMapper.Names.IGNORE_MALFORMED;

/**
 * Base class for {@link GeoShapeFieldMapper} and {@link LegacyGeoShapeFieldMapper}
 */
public abstract class AbstractGeometryFieldMapper<Parsed, Processed> extends FieldMapper {


    public static class Names {
        public static final ParseField ORIENTATION = new ParseField("orientation");
        public static final ParseField COERCE = new ParseField("coerce");
    }

    public static class Defaults {
        public static final Explicit<Orientation> ORIENTATION = new Explicit<>(Orientation.RIGHT, false);
        public static final Explicit<Boolean> COERCE = new Explicit<>(false, false);
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Boolean> IGNORE_Z_VALUE = new Explicit<>(true, false);
        public static final Explicit<Boolean> DOC_VALUES = new Explicit<>(false, false);
    }


    /**
     * Interface representing an preprocessor in geo-shape indexing pipeline
     */
    public interface Indexer<Parsed, Processed> {

        Processed prepareForIndexing(Parsed geometry);

        Class<Processed> processedClass();

        List<IndexableField> indexShape(ParseContext context, Processed shape);

        void indexDocValueField(ParseContext context, ShapeField.DecodedTriangle[] triangles,
                                CentroidCalculator centroidCalculator);
    }

    /**
     * interface representing parser in geo shape indexing pipeline
     */
    public interface Parser<Parsed> {

        Parsed parse(XContentParser parser, AbstractGeometryFieldMapper mapper) throws IOException, ParseException;

    }

    /**
     * interface representing a query builder that generates a query from the given shape
     */
    public interface QueryProcessor {
        Query process(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context);

        @Deprecated
        default Query process(Geometry shape, String fieldName, SpatialStrategy strategy, ShapeRelation relation,
                              QueryShardContext context) {
            return process(shape, fieldName, relation, context);
        }
    }

    public abstract static class Builder<T extends Builder, Y extends AbstractGeometryFieldMapper>
            extends FieldMapper.Builder<T, Y> {
        protected Boolean coerce;
        protected Boolean ignoreMalformed;
        protected Boolean ignoreZValue;
        protected Orientation orientation;

        /** default builder - used for external mapper*/
        public Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType) {
            super(name, fieldType, defaultFieldType);
        }

        public Builder coerce(boolean coerce) {
            this.coerce = coerce;
            return this;
        }

        protected Explicit<Boolean> coerce(BuilderContext context) {
            if (coerce != null) {
                return new Explicit<>(coerce, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(COERCE_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.COERCE;
        }

        public Builder orientation(Orientation orientation) {
            this.orientation = orientation;
            return this;
        }

        protected Explicit<Orientation> orientation() {
            if (orientation != null) {
                return new Explicit<>(orientation, true);
            }
            return Defaults.ORIENTATION;
        }

        @Override
        protected boolean defaultDocValues(Version indexCreated) {
            return false;
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

        protected Explicit<Boolean> ignoreZValue() {
            if (ignoreZValue != null) {
                return new Explicit<>(ignoreZValue, true);
            }
            return Defaults.IGNORE_Z_VALUE;
        }

        public Builder ignoreZValue(final boolean ignoreZValue) {
            this.ignoreZValue = ignoreZValue;
            return this;
        }

        protected Explicit<Boolean> docValues() {
            if (docValuesSet && fieldType.hasDocValues()) {
                return new Explicit<>(true, true);
            } else if (docValuesSet) {
                return new Explicit<>(false, true);
            }
            return Defaults.DOC_VALUES;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            // field mapper handles this at build time
            // but prefix tree strategies require a name, so throw a similar exception
            if (name().isEmpty()) {
                throw new IllegalArgumentException("name cannot be empty string");
            }

            AbstractGeometryFieldType ft = (AbstractGeometryFieldType)fieldType();
            ft.setOrientation(orientation().value());
        }
    }

    protected static final String DEPRECATED_PARAMETERS_KEY = "deprecated_parameters";

    public static class TypeParser implements Mapper.TypeParser {
        protected boolean parseXContentParameters(String name, Map.Entry<String, Object> entry, Map<String, Object> params)
                throws MapperParsingException {
            if (DeprecatedParameters.parse(name, entry.getKey(), entry.getValue(),
                (DeprecatedParameters)params.get(DEPRECATED_PARAMETERS_KEY))) {
                return true;
            }
            return false;
        }

        protected Builder newBuilder(String name, Map<String, Object> params) {
            if (params.containsKey(DEPRECATED_PARAMETERS_KEY)) {
                return new LegacyGeoShapeFieldMapper.Builder(name, (DeprecatedParameters)params.get(DEPRECATED_PARAMETERS_KEY));
            }
            return new GeoShapeFieldMapper.Builder(name);
        }

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Map<String, Object> params = new HashMap<>();
            boolean parsedDeprecatedParameters = false;
            params.put(DEPRECATED_PARAMETERS_KEY, new DeprecatedParameters());
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (parseXContentParameters(name, entry, params)) {
                    parsedDeprecatedParameters = true;
                    iterator.remove();
                } else if (Names.ORIENTATION.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    params.put(Names.ORIENTATION.getPreferredName(), ShapeBuilder.Orientation.fromString(fieldNode.toString()));
                    iterator.remove();
                } else if (IGNORE_MALFORMED.equals(fieldName)) {
                    params.put(IGNORE_MALFORMED, XContentMapValues.nodeBooleanValue(fieldNode, name + ".ignore_malformed"));
                    iterator.remove();
                } else if (Names.COERCE.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    params.put(Names.COERCE.getPreferredName(),
                        XContentMapValues.nodeBooleanValue(fieldNode, name + "." + Names.COERCE.getPreferredName()));
                    iterator.remove();
                } else if (GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName().equals(fieldName)) {
                    params.put(GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName(),
                        XContentMapValues.nodeBooleanValue(fieldNode,
                            name + "." + GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName()));
                    iterator.remove();
                } else if (TypeParsers.DOC_VALUES.equals(fieldName)) {
                    params.put(TypeParsers.DOC_VALUES, XContentMapValues.nodeBooleanValue(fieldNode, name + "." + TypeParsers.DOC_VALUES));
                    iterator.remove();
                }
            }
            if (parsedDeprecatedParameters == false) {
                params.remove(DEPRECATED_PARAMETERS_KEY);
            }
            Builder builder = newBuilder(name, params);

            if (params.containsKey(TypeParsers.DOC_VALUES)) {
                builder.docValues((Boolean) params.get(TypeParsers.DOC_VALUES));
            }

            if (params.containsKey(Names.COERCE.getPreferredName())) {
                builder.coerce((Boolean)params.get(Names.COERCE.getPreferredName()));
            }

            if (params.containsKey(GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName())) {
                builder.ignoreZValue((Boolean)params.get(GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName()));
            }

            if (params.containsKey(IGNORE_MALFORMED)) {
                builder.ignoreMalformed((Boolean)params.get(IGNORE_MALFORMED));
            }

            if (params.containsKey(Names.ORIENTATION.getPreferredName())) {
                builder.orientation((Orientation)params.get(Names.ORIENTATION.getPreferredName()));
            }

            return builder;
        }
    }

    public abstract static class AbstractGeometryFieldType<Parsed, Processed> extends MappedFieldType {
        protected Orientation orientation = Defaults.ORIENTATION.value();

        protected Indexer<Parsed, Processed> geometryIndexer;

        protected Parser<Parsed> geometryParser;

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
            this.orientation = ref.orientation;
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            AbstractGeometryFieldType that = (AbstractGeometryFieldType) o;
            return orientation == that.orientation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), orientation);
        }

        public Orientation orientation() { return this.orientation; }

        public void setOrientation(Orientation orientation) {
            checkIfFrozen();
            this.orientation = orientation;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context,
                "Geometry fields do not support exact searching, use dedicated geometry queries instead");
        }

        public void setGeometryIndexer(Indexer<Parsed, Processed> geometryIndexer) {
            this.geometryIndexer = geometryIndexer;
        }

        protected Indexer<Parsed, Processed> geometryIndexer() {
            return geometryIndexer;
        }

        public void setGeometryParser(Parser<Parsed> geometryParser)  {
            this.geometryParser = geometryParser;
        }

        protected Parser<Parsed> geometryParser() {
            return geometryParser;
        }

        public void setGeometryQueryBuilder(QueryProcessor geometryQueryBuilder)  {
            this.geometryQueryBuilder = geometryQueryBuilder;
        }

        public QueryProcessor geometryQueryBuilder() {
            return geometryQueryBuilder;
        }
    }

    protected Explicit<Boolean> coerce;
    protected Explicit<Boolean> ignoreMalformed;
    protected Explicit<Boolean> ignoreZValue;
    protected Explicit<Boolean> docValues;

    protected AbstractGeometryFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                          Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                                          Explicit<Boolean> ignoreZValue, Explicit<Boolean> docValues, Settings indexSettings,
                                          MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.coerce = coerce;
        this.ignoreMalformed = ignoreMalformed;
        this.ignoreZValue = ignoreZValue;
        this.docValues = docValues;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        AbstractGeometryFieldMapper gsfm = (AbstractGeometryFieldMapper)mergeWith;
        if (gsfm.coerce.explicit()) {
            this.coerce = gsfm.coerce;
        }
        if (gsfm.ignoreMalformed.explicit()) {
            this.ignoreMalformed = gsfm.ignoreMalformed;
        }
        if (gsfm.ignoreZValue.explicit()) {
            this.ignoreZValue = gsfm.ignoreZValue;
        }
        if (gsfm.docValues.explicit()) {
            this.docValues = gsfm.docValues;
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());
        AbstractGeometryFieldType ft = (AbstractGeometryFieldType)fieldType();
        if (includeDefaults || ft.orientation() != Defaults.ORIENTATION.value()) {
            builder.field(Names.ORIENTATION.getPreferredName(), ft.orientation());
        }
        if (includeDefaults || coerce.explicit()) {
            builder.field(Names.COERCE.getPreferredName(), coerce.value());
        }
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(IGNORE_MALFORMED, ignoreMalformed.value());
        }
        if (includeDefaults || ignoreZValue.explicit()) {
            builder.field(GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName(), ignoreZValue.value());
        }
        if (includeDefaults || docValues.explicit()) {
            builder.field(TypeParsers.DOC_VALUES, docValues.value());
        }
    }

    public Explicit<Boolean> coerce() {
        return coerce;
    }

    public Explicit<Boolean> ignoreMalformed() {
        return ignoreMalformed;
    }

    public Explicit<Boolean> ignoreZValue() {
        return ignoreZValue;
    }

    public Explicit<Boolean> docValues() {
        return docValues;
    }

    public Orientation orientation() {
        return ((AbstractGeometryFieldType)fieldType).orientation();
    }

    /** parsing logic for geometry indexing */
    @Override
    public void parse(ParseContext context) throws IOException {
        AbstractGeometryFieldType fieldType = (AbstractGeometryFieldType)fieldType();

        @SuppressWarnings("unchecked") Indexer<Parsed, Processed> geometryIndexer = fieldType.geometryIndexer();
        @SuppressWarnings("unchecked") Parser<Parsed> geometryParser = fieldType.geometryParser();
        try {
            Processed shape = context.parseExternalValue(geometryIndexer.processedClass());
            if (shape == null) {
                Parsed geometry = geometryParser.parse(context.parser(), this);
                if (geometry == null) {
                    return;
                }
                shape = geometryIndexer.prepareForIndexing(geometry);
            }

            List<IndexableField> fields = new ArrayList<>(geometryIndexer.indexShape(context, shape));
            final byte[] scratch = new byte[7 * Integer.BYTES];
            if (fieldType().hasDocValues()) {
                // doc values are generated from the indexed fields.
                ShapeField.DecodedTriangle[] triangles = new ShapeField.DecodedTriangle[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    BytesRef bytesRef = fields.get(i).binaryValue();
                    assert bytesRef.length == 7 * Integer.BYTES;
                    System.arraycopy(bytesRef.bytes, bytesRef.offset, scratch, 0, 7 * Integer.BYTES);
                    ShapeField.decodeTriangle(scratch, triangles[i] = new ShapeField.DecodedTriangle());
                 }

                geometryIndexer.indexDocValueField(context, triangles, new CentroidCalculator((Geometry) shape));
            }
            createFieldNamesField(context, fields);
            for (IndexableField field : fields) {
                context.doc().add(field);
            }
        } catch (Exception e) {
            if (ignoreMalformed.value() == false) {
                throw new MapperParsingException("failed to parse field [{}] of type [{}]", e, fieldType().name(),
                    fieldType().typeName());
            }
            context.addIgnoredField(fieldType().name());
        }
    }

}
