/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
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

    // The GeoShapeFieldMapper class does not exist in server any more.
    // For backwards compatibility we add the name of the class manually.
    protected static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(
        "org.elasticsearch.index.mapper.GeoShapeFieldMapper"
    );

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
        public abstract void parse(XContentParser parser, CheckedConsumer<T, IOException> consumer, MalformedValueHandler malformedHandler)
            throws IOException;

        private void fetchFromSource(Object sourceMap, Consumer<T> consumer) {
            try (XContentParser parser = wrapObject(sourceMap)) {
                parseFromSource(parser, consumer);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void parseFromSource(XContentParser parser, Consumer<T> consumer) throws IOException {
            parse(parser, v -> consumer.accept(normalizeFromSource(v)), NoopMalformedValueHandler.INSTANCE);
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

    public interface MalformedValueHandler {
        void notify(Exception parsingException) throws IOException;

        void notify(Exception parsingException, XContentBuilder malformedDataForSyntheticSource) throws IOException;
    }

    public record NoopMalformedValueHandler() implements MalformedValueHandler {
        public static final NoopMalformedValueHandler INSTANCE = new NoopMalformedValueHandler();

        @Override
        public void notify(Exception parsingException) {}

        @Override
        public void notify(Exception parsingException, XContentBuilder malformedDataForSyntheticSource) {}
    }

    public record DefaultMalformedValueHandler(CheckedBiConsumer<Exception, XContentBuilder, IOException> consumer)
        implements
            MalformedValueHandler {
        @Override
        public void notify(Exception parsingException) throws IOException {
            consumer.accept(parsingException, null);
        }

        @Override
        public void notify(Exception parsingException, XContentBuilder malformedDataForSyntheticSource) throws IOException {
            consumer.accept(parsingException, malformedDataForSyntheticSource);
        }
    }

    public abstract static class AbstractGeometryFieldType<T> extends MappedFieldType {

        protected final Parser<T> geometryParser;
        protected final T nullValue;

        protected AbstractGeometryFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            Parser<T> geometryParser,
            T nullValue,
            Map<String, String> meta
        ) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.NONE, meta);
            this.nullValue = nullValue;
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

        public ValueFetcher valueFetcher(Set<String> sourcePaths, T nullValue, String format) {
            Function<List<T>, List<Object>> formatter = getFormatter(format != null ? format : GeometryFormatterFactory.GEOJSON);
            return new ArraySourceValueFetcher(sourcePaths, nullValueAsSource(nullValue)) {
                @Override
                protected Object parseSourceValue(Object value) {
                    final List<T> values = new ArrayList<>();
                    geometryParser.fetchFromSource(value, values::add);
                    return formatter.apply(values);
                }
            };
        }

        protected BlockLoader blockLoaderFromSource(BlockLoaderContext blContext) {
            ValueFetcher fetcher = valueFetcher(blContext.sourcePaths(name()), nullValue, GeometryFormatterFactory.WKB);
            // TODO consider optimization using BlockSourceReader.lookupFromFieldNames(blContext.fieldNames(), name())
            return new BlockSourceReader.GeometriesBlockLoader(fetcher, BlockSourceReader.lookupMatchingAll());
        }

        protected abstract Object nullValueAsSource(T nullValue);

        protected BlockLoader blockLoaderFromFallbackSyntheticSource(BlockLoaderContext blContext) {
            return new FallbackSyntheticSourceBlockLoader(new GeometriesFallbackSyntheticSourceReader(), name()) {
                @Override
                public Builder builder(BlockFactory factory, int expectedCount) {
                    return factory.bytesRefs(expectedCount);
                }
            };
        }

        private class GeometriesFallbackSyntheticSourceReader implements FallbackSyntheticSourceBlockLoader.Reader<BytesRef> {
            private final Function<List<T>, List<Object>> formatter;

            private GeometriesFallbackSyntheticSourceReader() {
                this.formatter = getFormatter(GeometryFormatterFactory.WKB);
            }

            @Override
            public void convertValue(Object value, List<BytesRef> accumulator) {
                final List<T> values = new ArrayList<>();

                geometryParser.fetchFromSource(value, v -> {
                    if (v != null) {
                        values.add(v);
                    } else if (nullValue != null) {
                        values.add(nullValue);
                    }
                });
                var formatted = formatter.apply(values);

                for (var formattedValue : formatted) {
                    if (formattedValue instanceof byte[] wkb) {
                        accumulator.add(new BytesRef(wkb));
                    } else {
                        throw new IllegalArgumentException(
                            "Unsupported source type for spatial geometry: " + formattedValue.getClass().getSimpleName()
                        );
                    }
                }
            }

            @Override
            public void parse(XContentParser parser, List<BytesRef> accumulator) throws IOException {
                final List<T> values = new ArrayList<>();

                geometryParser.parseFromSource(parser, v -> {
                    if (v != null) {
                        values.add(v);
                    } else if (nullValue != null) {
                        values.add(nullValue);
                    }
                });
                var formatted = formatter.apply(values);

                for (var formattedValue : formatted) {
                    if (formattedValue instanceof byte[] wkb) {
                        accumulator.add(new BytesRef(wkb));
                    } else {
                        throw new IllegalArgumentException(
                            "Unsupported source type for spatial geometry: " + formattedValue.getClass().getSimpleName()
                        );
                    }
                }
            }

            @Override
            public void writeToBlock(List<BytesRef> values, BlockLoader.Builder blockBuilder) {
                var bytesRefBuilder = (BlockLoader.BytesRefBuilder) blockBuilder;

                for (var value : values) {
                    bytesRefBuilder.appendBytesRef(value);
                }
            }
        }
    }

    private final Explicit<Boolean> ignoreMalformed;
    private final Explicit<Boolean> ignoreZValue;
    private final Parser<T> parser;

    protected AbstractGeometryFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        Explicit<Boolean> ignoreMalformed,
        Explicit<Boolean> ignoreZValue,
        Parser<T> parser
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.ignoreMalformed = ignoreMalformed;
        this.ignoreZValue = ignoreZValue;
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
    protected boolean supportsParsingObject() {
        return true;
    }

    @Override
    public final void parse(DocumentParserContext context) throws IOException {
        if (builderParams.hasScript()) {
            throw new DocumentParsingException(
                context.parser().getTokenLocation(),
                "failed to parse field [" + fieldType().name() + "] of type + " + contentType() + "]",
                new IllegalArgumentException("Cannot index data directly into a field with a [script] parameter")
            );
        }
        parser.parse(context.parser(), v -> index(context, v), new DefaultMalformedValueHandler((e, b) -> onMalformedValue(context, b, e)));
    }

    protected void onMalformedValue(DocumentParserContext context, XContentBuilder malformedDataForSyntheticSource, Exception cause)
        throws IOException {
        if (ignoreMalformed()) {
            context.addIgnoredField(fieldType().name());
        } else {
            throw new DocumentParsingException(
                context.parser().getTokenLocation(),
                "failed to parse field [" + fieldType().name() + "] of type [" + contentType() + "]",
                cause
            );
        }
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
