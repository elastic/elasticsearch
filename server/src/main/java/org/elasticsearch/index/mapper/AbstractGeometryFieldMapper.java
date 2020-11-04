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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoJsonGeometryFormat;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Base field mapper class for all spatial field types
 */
public abstract class AbstractGeometryFieldMapper<Parsed, Processed> extends FieldMapper {

    public static Parameter<Explicit<Boolean>> ignoreMalformedParam(Function<FieldMapper, Explicit<Boolean>> initializer,
                                                                    boolean ignoreMalformedByDefault) {
        return Parameter.explicitBoolParam("ignore_malformed", true, initializer, ignoreMalformedByDefault);
    }

    public static Parameter<Explicit<Boolean>> ignoreZValueParam(Function<FieldMapper, Explicit<Boolean>> initializer) {
        return Parameter.explicitBoolParam("ignore_z_value", true, initializer, true);
    }

    /**
     * Interface representing an preprocessor in geometry indexing pipeline
     */
    public interface Indexer<Parsed, Processed> {
        Processed prepareForIndexing(Parsed geometry);
        Class<Processed> processedClass();
        List<IndexableField> indexShape(ParseContext context, Processed shape);
    }

    /**
     * Interface representing parser in geometry indexing pipeline.
     */
    public abstract static class Parser<Parsed> {
        /**
         * Parse the given xContent value to an object of type {@link Parsed}. The value can be
         * in any supported format.
         */
        public abstract Parsed parse(XContentParser parser) throws IOException, ParseException;

        /**
         * Given a parsed value and a format string, formats the value into a plain Java object.
         *
         * Supported formats include 'geojson' and 'wkt'. The different formats are defined
         * as subclasses of {@link org.elasticsearch.common.geo.GeometryFormat}.
         */
        public abstract Object format(Parsed value, String format);

        /**
         * Parses the given value, then formats it according to the 'format' string.
         *
         * By default, this method simply parses the value using {@link Parser#parse}, then formats
         * it with {@link Parser#format}. However some {@link Parser} implementations override this
         * as they can avoid parsing the value if it is already in the right format.
         */
        public Object parseAndFormatObject(Object value, String format) {
            Parsed geometry;
            try (XContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                Collections.singletonMap("dummy_field", value), XContentType.JSON)) {
                parser.nextToken(); // start object
                parser.nextToken(); // field name
                parser.nextToken(); // field value
                geometry = parse(parser);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            return format(geometry, format);
        }
    }

    public abstract static class AbstractGeometryFieldType extends MappedFieldType {

        protected final Parser<?> geometryParser;
        protected final boolean parsesArrayValue;

        protected AbstractGeometryFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues,
                                            boolean parsesArrayValue, Parser<?> geometryParser, Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.NONE, meta);
            this.parsesArrayValue = parsesArrayValue;
            this.geometryParser = geometryParser;
        }

        @Override
        public final Query termQuery(Object value, QueryShardContext context) {
            throw new IllegalArgumentException("Geometry fields do not support exact searching, use dedicated geometry queries instead: ["
                    + name() + "]");
        }

        @Override
        public final ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            String geoFormat = format != null ? format : GeoJsonGeometryFormat.NAME;

            Function<Object, Object> valueParser = value -> geometryParser.parseAndFormatObject(value, geoFormat);
            if (parsesArrayValue) {
                return new ArraySourceValueFetcher(name(), context) {
                    @Override
                    protected Object parseSourceValue(Object value) {
                        return valueParser.apply(value);
                    }
                };
            } else {
                return new SourceValueFetcher(name(), context) {
                    @Override
                    protected Object parseSourceValue(Object value) {
                        return valueParser.apply(value);
                    }
                };
            }
        }
    }

    private final Explicit<Boolean> ignoreMalformed;
    private final Explicit<Boolean> ignoreZValue;
    private final Indexer<Parsed, Processed> indexer;
    private final Parser<Parsed> parser;

    protected AbstractGeometryFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                          Explicit<Boolean> ignoreMalformed, Explicit<Boolean> ignoreZValue,
                                          MultiFields multiFields, CopyTo copyTo,
                                          Indexer<Parsed, Processed> indexer, Parser<Parsed> parser) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.ignoreZValue = ignoreZValue;
        this.indexer = indexer;
        this.parser = parser;
    }

    @Override
    public AbstractGeometryFieldType fieldType() {
        return (AbstractGeometryFieldType) mappedFieldType;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    protected abstract void addStoredFields(ParseContext context, Processed geometry);
    protected abstract void addDocValuesFields(String name, Processed geometry, List<IndexableField> fields, ParseContext context);
    protected abstract void addMultiFields(ParseContext context, Processed geometry) throws IOException;

    /** parsing logic for geometry indexing */
    @Override
    public void parse(ParseContext context) throws IOException {
        MappedFieldType mappedFieldType = fieldType();

        try {
            Processed shape = context.parseExternalValue(indexer.processedClass());
            if (shape == null) {
                Parsed geometry = parser.parse(context.parser());
                if (geometry == null) {
                    return;
                }
                shape = indexer.prepareForIndexing(geometry);
            }

            List<IndexableField> fields = new ArrayList<>();
            if (mappedFieldType.isSearchable() || mappedFieldType.hasDocValues()) {
                fields.addAll(indexer.indexShape(context, shape));
            }

            // indexed:
            List<IndexableField> indexedFields = new ArrayList<>();
            if (mappedFieldType.isSearchable()) {
                indexedFields.addAll(fields);
            }
            // stored:
            if (fieldType().isStored()) {
                addStoredFields(context, shape);
            }
            // docValues:
            if (fieldType().hasDocValues()) {
                addDocValuesFields(mappedFieldType.name(), shape, fields, context);
            } else if (fieldType().isStored() || fieldType().isSearchable()) {
                createFieldNamesField(context);
            }

            // add the indexed fields to the doc:
            for (IndexableField field : indexedFields) {
                context.doc().add(field);
            }

            // add multifields (e.g., used for completion suggester)
            addMultiFields(context, shape);
        } catch (Exception e) {
            if (ignoreMalformed.value() == false) {
                throw new MapperParsingException("failed to parse field [{}] of type [{}]", e, fieldType().name(),
                    fieldType().typeName());
            }
            context.addIgnoredField(mappedFieldType.name());
        }
    }

    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    public boolean ignoreZValue() {
        return ignoreZValue.value();
    }
}
