/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryFormat;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.Mapper.TypeParser.ParserContext;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Base class for for spatial fields that only support indexing points */
public abstract class AbstractPointGeometryFieldMapper<T> extends AbstractGeometryFieldMapper<T> {

    public static Parameter<ParsedPoint> nullValueParam(Function<FieldMapper, ParsedPoint> initializer,
                                                        TriFunction<String, ParserContext, Object, ParsedPoint> parser,
                                                        Supplier<ParsedPoint> def) {
        return new Parameter<>("null_value", false, def, parser, initializer);
    }

    protected final ParsedPoint nullValue;

    protected AbstractPointGeometryFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                               MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                               Explicit<Boolean> ignoreZValue, ParsedPoint nullValue, CopyTo copyTo,
                                               Parser<? extends T> parser) {
        super(simpleName, mappedFieldType, ignoreMalformed, ignoreZValue, multiFields, copyTo, parser);
        this.nullValue = nullValue;
    }

    protected AbstractPointGeometryFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                               MultiFields multiFields, CopyTo copyTo,
                                               Parser<? extends T> parser, String onScriptError) {
        super(simpleName, mappedFieldType, multiFields, copyTo, parser, onScriptError);
        this.nullValue = null;
    }

    @Override
    public final boolean parsesArrayValue() {
        return true;
    }

    public ParsedPoint getNullValue() {
        return nullValue;
    }

    /** represents a Point that has been parsed by {@link PointParser} */
    public interface ParsedPoint {
        void validate(String fieldName);
        void normalize(String fieldName);
        void resetCoords(double x, double y);
        Point asGeometry();
        default boolean isNormalizable(double coord) {
            return Double.isNaN(coord) == false && Double.isInfinite(coord) == false;
        }
    }

    /** A parser implementation that can parse the various point formats */
    public static class PointParser<P extends ParsedPoint> extends Parser<P> {
        /**
         * Note that this parser is only used for formatting values.
         */
        private final GeometryParser geometryParser;
        private final String field;
        private final Supplier<P> pointSupplier;
        private final CheckedBiFunction<XContentParser, P, P, IOException> objectParser;
        private final P nullValue;
        private final boolean ignoreZValue;
        private final boolean ignoreMalformed;

        public PointParser(String field,
                           Supplier<P> pointSupplier,
                           CheckedBiFunction<XContentParser, P, P, IOException> objectParser,
                           P nullValue,
                           boolean ignoreZValue,
                           boolean ignoreMalformed) {
            this.field = field;
            this.pointSupplier = pointSupplier;
            this.objectParser = objectParser;
            this.nullValue = nullValue == null ? null : process(nullValue);
            this.ignoreZValue = ignoreZValue;
            this.ignoreMalformed = ignoreMalformed;
            this.geometryParser = new GeometryParser(true, true, true);
        }

        private P process(P in) {
            if (ignoreMalformed == false) {
                in.validate(field);
            } else {
                in.normalize(field);
            }
            return in;
        }

        @Override
        public void parse(
            XContentParser parser,
            CheckedConsumer<P, IOException> consumer,
            Consumer<Exception> onMalformed
        ) throws IOException {
            if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                XContentParser.Token token = parser.nextToken();
                P point = pointSupplier.get();
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    double x = parser.doubleValue();
                    parser.nextToken();
                    double y = parser.doubleValue();
                    token = parser.nextToken();
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        GeoPoint.assertZValue(ignoreZValue, parser.doubleValue());
                    } else if (token != XContentParser.Token.END_ARRAY) {
                        throw new ElasticsearchParseException("field type does not accept > 3 dimensions");
                    }

                    point.resetCoords(x, y);
                    consumer.accept(process(point));
                } else {
                    while (token != XContentParser.Token.END_ARRAY) {
                        parseAndConsumeFromObject(parser, point, consumer, onMalformed);
                        point = pointSupplier.get();
                        token = parser.nextToken();
                    }
                }
            } else if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                if (nullValue != null) {
                    consumer.accept(nullValue);
                }
            } else {
                parseAndConsumeFromObject(parser, pointSupplier.get(), consumer, onMalformed);
            }
        }

        private void parseAndConsumeFromObject(
            XContentParser parser,
            P point,
            CheckedConsumer<P, IOException> consumer,
            Consumer<Exception> onMalformed
        ) {
            try {
                point = objectParser.apply(parser, point);
                consumer.accept(process(point));
            } catch (Exception e) {
                onMalformed.accept(e);
            }
        }

        @Override
        public Object format(P point, String format) {
            GeometryFormat<Geometry> geometryFormat = geometryParser.geometryFormat(format);
            return geometryFormat.toXContentAsObject(point.asGeometry());
        }
    }
}
