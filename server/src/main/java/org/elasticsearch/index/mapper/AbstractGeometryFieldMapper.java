/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoJsonGeometryFormat;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

        private void fetchFromSource(Object sourceMap, Consumer<Object> consumer, String format) {
            try (XContentParser parser = MapXContentParser.wrapObject(sourceMap)) {
                parse(parser, v -> consumer.accept(format(v, format)), e -> {}); /* ignore malformed */
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public abstract static class AbstractGeometryFieldType<T> extends MappedFieldType {

        protected final Parser<T> geometryParser;
        protected final boolean parsesArrayValue;

        protected AbstractGeometryFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues,
                                            boolean parsesArrayValue, Parser<T> geometryParser, Map<String, String> meta) {
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
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            String geoFormat = format != null ? format : GeoJsonGeometryFormat.NAME;

            if (parsesArrayValue) {
                return new ArraySourceValueFetcher(name(), context) {
                    @Override
                    protected Object parseSourceValue(Object value) {
                        List<Object> values = new ArrayList<>();
                        geometryParser.fetchFromSource(value, values::add, geoFormat);
                        return values;
                    }
                };
            } else {
                return new SourceValueFetcher(name(), context) {
                    @Override
                    protected Object parseSourceValue(Object value) {
                        SetOnce<Object> holder = new SetOnce<>();
                        geometryParser.fetchFromSource(value, holder::set, geoFormat);
                        return holder.get();
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

    protected AbstractGeometryFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Parser<T> parser,
        String onScriptError
    ) {
        super(simpleName, mappedFieldType, Collections.emptyMap(), multiFields, copyTo, true, onScriptError);
        this.ignoreMalformed = new Explicit<>(false, true);
        this.ignoreZValue = new Explicit<>(false, true);
        this.parser = parser;
    }

    @Override
    public AbstractGeometryFieldType<T> fieldType() {
        return (AbstractGeometryFieldType<T>) mappedFieldType;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    /**
     * Build an index document using a parsed geometry
     * @param context   the ParseContext holding the document
     * @param geometry  the parsed geometry object
     */
    protected abstract void index(ParseContext context, T geometry) throws IOException;

    @Override
    public final void parse(ParseContext context) throws IOException {
        if (hasScript) {
            throw new MapperParsingException("failed to parse field [" + fieldType().name() + "] of type + " + contentType() + "]",
                new IllegalArgumentException("Cannot index data directly into a field with a [script] parameter"));
        }
        parser.parse(context.parser(), v -> index(context, v), e -> {
            if (ignoreMalformed()) {
                context.addIgnoredField(fieldType().name());
            } else {
                throw new MapperParsingException(
                    "failed to parse field [" + fieldType().name() + "] of type [" + contentType() + "]", e
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
