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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedBiFunction;
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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/** Base class for for spatial fields that only support indexing points */
public abstract class AbstractPointGeometryFieldMapper<Parsed, Processed> extends AbstractGeometryFieldMapper<Parsed, Processed> {

    public static Parameter<ParsedPoint> nullValueParam(Function<FieldMapper, ParsedPoint> initializer,
                                                        TriFunction<String, ParserContext, Object, ParsedPoint> parser,
                                                        Supplier<ParsedPoint> def) {
        return new Parameter<>("null_value", false, def, parser, initializer);
    }

    protected final ParsedPoint nullValue;

    protected AbstractPointGeometryFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                               MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                               Explicit<Boolean> ignoreZValue, ParsedPoint nullValue, CopyTo copyTo,
                                               Indexer<Parsed, Processed> indexer, Parser<Parsed> parser) {
        super(simpleName, mappedFieldType, ignoreMalformed, ignoreZValue, multiFields, copyTo, indexer, parser);
        this.nullValue = nullValue;
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
    public static class PointParser<P extends ParsedPoint> extends Parser<List<P>> {
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
            this.nullValue = nullValue;
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
        public List<P> parse(XContentParser parser) throws IOException, ParseException {

            if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                XContentParser.Token token = parser.nextToken();
                P point = pointSupplier.get();
                ArrayList<P> points = new ArrayList<>();
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
                    points.add(process(point));
                } else {
                    while (token != XContentParser.Token.END_ARRAY) {
                        points.add(process(objectParser.apply(parser, point)));
                        point = pointSupplier.get();
                        token = parser.nextToken();
                    }
                }
                return points;
            } else if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                if (nullValue == null) {
                    return null;
                } else {
                    return Collections.singletonList(nullValue);
                }
            } else {
                return Collections.singletonList(process(objectParser.apply(parser, pointSupplier.get())));
            }
        }

        @Override
        public Object format(List<P> points, String format) {
            List<Object> result = new ArrayList<>();
            GeometryFormat<Geometry> geometryFormat = geometryParser.geometryFormat(format);
            for (ParsedPoint point : points) {
                Geometry geometry = point.asGeometry();
                result.add(geometryFormat.toXContentAsObject(geometry));
            }
            return result;
        }
    }
}
