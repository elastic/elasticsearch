/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Base field mapper class for all spatial field types
 */
public abstract class AbstractGeometryFieldMapper<T> extends FieldMapper {

    public static Parameter<Explicit<Boolean>> ignoreMalformedParam(
        Function<FieldMapper, Explicit<Boolean>> initializer,
        boolean ignoreMalformedByDefault
    ) {
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
        public abstract void parse(XContentParser parser, CheckedConsumer<T, IOException> consumer, Consumer<Exception> onMalformed)
            throws IOException;

        private void fetchFromSource(Object sourceMap, Consumer<T> consumer) {
            try (XContentParser parser = wrapObject(sourceMap)) {
                parse(parser, v -> consumer.accept(normalizeFromSource(v)), e -> {}); /* ignore malformed */
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /**
         * Normalize a geometry when reading from source. When reading from source we can skip
         * some expensive steps as the geometry has already been indexed.
         */
        // TODO: move geometry normalization to the geometry parser.
        public abstract T normalizeFromSource(T geometry);

        private static XContentParser wrapObject(Object sourceMap) throws IOException {
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                Collections.singletonMap("dummy_field", sourceMap),
                XContentType.JSON
            );
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            return parser;
        }
    }

    public abstract static class AbstractGeometryFieldType<T> extends MappedFieldType {

        protected final Parser<T> geometryParser;

        protected AbstractGeometryFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            Parser<T> geometryParser,
            Map<String, String> meta
        ) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.NONE, meta);
            this.geometryParser = geometryParser;
        }

        @Override
        public final Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException(
                "Geometry fields do not support exact searching, use dedicated geometry queries instead: [" + name() + "]"
            );
        }

        /**
         * Gets the formatter by name.
         */
        protected abstract Function<List<T>, List<Object>> getFormatter(String format);

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            Function<List<T>, List<Object>> formatter = getFormatter(format != null ? format : GeometryFormatterFactory.GEOJSON);
            return new ArraySourceValueFetcher(name(), context) {
                @Override
                protected Object parseSourceValue(Object value) {
                    final List<T> values = new ArrayList<>();
                    geometryParser.fetchFromSource(value, values::add);
                    return formatter.apply(values);
                }
            };
        }

        public ValueFetcher valueFetcher(Set<String> sourcePaths, Object nullValue, String format) {
            Function<List<T>, List<Object>> formatter = getFormatter(format != null ? format : GeometryFormatterFactory.GEOJSON);
            return new ArraySourceValueFetcher(sourcePaths, nullValue) {
                @Override
                protected Object parseSourceValue(Object value) {
                    final List<T> values = new ArrayList<>();
                    geometryParser.fetchFromSource(value, values::add);
                    return formatter.apply(values);
                }
            };
        }
    }

    private final Explicit<Boolean> ignoreMalformed;
    private final Explicit<Boolean> ignoreZValue;
    private final Parser<T> parser;

    protected AbstractGeometryFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        Explicit<Boolean> ignoreMalformed,
        Explicit<Boolean> ignoreZValue,
        MultiFields multiFields,
        CopyTo copyTo,
        Parser<T> parser
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo, false, null);
        this.ignoreMalformed = ignoreMalformed;
        this.ignoreZValue = ignoreZValue;
        this.parser = parser;
    }

    protected AbstractGeometryFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Parser<T> parser,
        OnScriptError onScriptError
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo, true, onScriptError);
        this.ignoreMalformed = Explicit.EXPLICIT_FALSE;
        this.ignoreZValue = Explicit.EXPLICIT_FALSE;
        this.parser = parser;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AbstractGeometryFieldType<T> fieldType() {
        return (AbstractGeometryFieldType<T>) mappedFieldType;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    /**
     * Build an index document using a parsed geometry
     * @param context   the ParseContext holding the document
     * @param geometry  the parsed geometry object
     */
    protected abstract void index(DocumentParserContext context, T geometry) throws IOException;

    @Override
    public final void parse(DocumentParserContext context) throws IOException {
        if (hasScript) {
            throw new DocumentParsingException(
                context.parser().getTokenLocation(),
                "failed to parse field [" + fieldType().name() + "] of type + " + contentType() + "]",
                new IllegalArgumentException("Cannot index data directly into a field with a [script] parameter")
            );
        }
        parser.parse(context.parser(), v -> index(context, v), e -> {
            if (ignoreMalformed()) {
                context.addIgnoredField(fieldType().name());
            } else {
                throw new DocumentParsingException(
                    context.parser().getTokenLocation(),
                    "failed to parse field [" + fieldType().name() + "] of type [" + contentType() + "]",
                    e
                );
            }
        });
    }

    @Override
    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    public boolean ignoreZValue() {
        return ignoreZValue.value();
    }

    @Override
    public final boolean parsesArrayValue() {
        return true;
    }
}
