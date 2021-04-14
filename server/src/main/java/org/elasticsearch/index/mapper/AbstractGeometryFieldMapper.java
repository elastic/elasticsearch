/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoJsonGeometryFormat;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Base field mapper class for all spatial field types
 */
public abstract class AbstractGeometryFieldMapper<T> extends FieldMapper {

    public static Parameter<Explicit<Boolean>> ignoreMalformedParam(Function<FieldMapper, Explicit<Boolean>> initializer,
                                                                    boolean ignoreMalformedByDefault) {
        return Parameter.explicitBoolParam("ignore_malformed", true, initializer, ignoreMalformedByDefault);
    }

    public static Parameter<Explicit<Boolean>> ignoreZValueParam(Function<FieldMapper, Explicit<Boolean>> initializer) {
        return Parameter.explicitBoolParam("ignore_z_value", true, initializer, true);
    }

    /**
     * Interface representing parser in geometry indexing pipeline.
     */
    public abstract static class Parser<T> {
        /**
         * Parse the given xContent value to one or more objects of type {@link T}. The value can be
         * in any supported format.
         */
        public abstract void parse(
            XContentParser parser,
            CheckedConsumer<T, IOException> consumer,
            Consumer<Exception> onMalformed) throws IOException;

        /**
         * Given a parsed value and a format string, formats the value into a plain Java object.
         *
         * Supported formats include 'geojson' and 'wkt'. The different formats are defined
         * as subclasses of {@link org.elasticsearch.common.geo.GeometryFormat}.
         */
        public abstract Object format(T value, String format);

        /**
         * Parses the given value, then formats it according to the 'format' string.
         *
         * Used by value fetchers to validate and format geo objects
         */
        public Object parseAndFormatObject(Object value, String format) {
            Object[] formatted = new Object[1];
            try (XContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                Collections.singletonMap("dummy_field", value), XContentType.JSON)) {
                parser.nextToken(); // start object
                parser.nextToken(); // field name
                parser.nextToken(); // field value
                parse(parser, v -> {
                    formatted[0] = format(v, format);
                }, e -> {} /* ignore malformed */);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return formatted[0];
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
        public final Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Geometry fields do not support exact searching, use dedicated geometry queries instead: ["
                    + name() + "]");
        }

        @Override
        public final ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
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
    private final Parser<T> parser;

    protected AbstractGeometryFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                          Map<String, NamedAnalyzer> indexAnalyzers,
                                          Explicit<Boolean> ignoreMalformed, Explicit<Boolean> ignoreZValue,
                                          MultiFields multiFields, CopyTo copyTo,
                                          Parser<T> parser) {
        super(simpleName, mappedFieldType, indexAnalyzers, multiFields, copyTo, false, null);
        this.ignoreMalformed = ignoreMalformed;
        this.ignoreZValue = ignoreZValue;
        this.parser = parser;
    }

    protected AbstractGeometryFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                          Explicit<Boolean> ignoreMalformed, Explicit<Boolean> ignoreZValue,
                                          MultiFields multiFields, CopyTo copyTo,
                                          Parser<T> parser) {
        this(simpleName, mappedFieldType, Collections.emptyMap(), ignoreMalformed, ignoreZValue, multiFields, copyTo, parser);
    }

    @Override
    public AbstractGeometryFieldType fieldType() {
        return (AbstractGeometryFieldType) mappedFieldType;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    protected abstract void index(ParseContext context, T geometry) throws IOException;

    /** parsing logic for geometry indexing */
    @Override
    public final void parse(ParseContext context) throws IOException {
      parser.parse(context.parser(), v -> index(context, v), e -> {
          if (ignoreMalformed()) {
              context.addIgnoredField(fieldType().name());
          } else {
              throw new MapperParsingException(
                  "Failed to parse field [" + fieldType().name() + "] of type [" + contentType() + "]",
                  e
              );
          }
      });
    }

    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    public boolean ignoreZValue() {
        return ignoreZValue.value();
    }
}
