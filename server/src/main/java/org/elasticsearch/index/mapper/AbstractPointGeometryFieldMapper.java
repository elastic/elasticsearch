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

import org.apache.lucene.document.FieldType;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryFormat;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/** Base class for for spatial fields that only support indexing points */
public abstract class AbstractPointGeometryFieldMapper<Parsed, Processed> extends AbstractGeometryFieldMapper<Parsed, Processed> {

    public static class Names extends AbstractGeometryFieldMapper.Names {
        public static final ParseField NULL_VALUE = new ParseField("null_value");
    }

    public static final FieldType DEFAULT_FIELD_TYPE = new FieldType();
    static {
        DEFAULT_FIELD_TYPE.setDimensions(2, Integer.BYTES);
        DEFAULT_FIELD_TYPE.setStored(false);
        DEFAULT_FIELD_TYPE.freeze();
    }

    public abstract static class Builder extends AbstractGeometryFieldMapper.Builder {

        protected ParsedPoint nullValue;

        public Builder(String name, FieldType fieldType) {
            super(name, fieldType);
        }

        public void setNullValue(ParsedPoint nullValue) {
            this.nullValue = nullValue;
        }

        public abstract AbstractPointGeometryFieldMapper build(BuilderContext context, String simpleName, FieldType fieldType,
                                                               MultiFields multiFields,
                                                               Explicit<Boolean> ignoreMalformed,
                                                               Explicit<Boolean> ignoreZValue,
                                                               ParsedPoint nullValue, CopyTo copyTo);


        @Override
        public AbstractPointGeometryFieldMapper build(BuilderContext context) {
            return build(context, name, fieldType,
                multiFieldsBuilder.build(this, context), ignoreMalformed(context),
                ignoreZValue(context), nullValue, copyTo);
        }
    }

    public abstract static class TypeParser<T extends Builder> extends AbstractGeometryFieldMapper.TypeParser<Builder> {
        protected abstract ParsedPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed);

        @Override
        public T parse(String name, Map<String, Object> node, Map<String, Object> params, ParserContext parserContext) {
            T builder = (T)(super.parse(name, node, params, parserContext));
            parseField(builder, name, node, parserContext);
            Object nullValue = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();

                if (Names.NULL_VALUE.match(propName, LoggingDeprecationHandler.INSTANCE)) {
                    nullValue = propNode;
                    iterator.remove();
                }
            }

            if (nullValue != null) {
                builder.setNullValue(parseNullValue(nullValue, (Boolean)builder.ignoreZValue().value(),
                    (Boolean)builder.ignoreMalformed().value()));
            }

            return builder;
        }
    }

    ParsedPoint nullValue;

    public abstract static class AbstractPointGeometryFieldType<Parsed, Processed>
            extends AbstractGeometryFieldType<Parsed, Processed> {
        protected AbstractPointGeometryFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues,
                                                 Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, true, meta);
        }
    }

    protected AbstractPointGeometryFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                                               MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                               Explicit<Boolean> ignoreZValue, ParsedPoint nullValue, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, ignoreMalformed, ignoreZValue, multiFields, copyTo);
        this.nullValue = nullValue;
    }

    @Override
    public final boolean parsesArrayValue() {
        return true;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        super.mergeOptions(other, conflicts);
        AbstractPointGeometryFieldMapper gpfm = (AbstractPointGeometryFieldMapper)other;
        // TODO make this un-updateable
        if (gpfm.nullValue != null) {
            this.nullValue = gpfm.nullValue;
        }
    }

    @Override
    public void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (nullValue != null || includeDefaults) {
            builder.field(Names.NULL_VALUE.getPreferredName(), nullValue);
        }
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
                    return Collections.emptyList();
                }
                else {
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
