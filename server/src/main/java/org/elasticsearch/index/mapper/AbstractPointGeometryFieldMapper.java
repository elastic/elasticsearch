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
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/** Base class for for spatial fields that only support indexing points */
public abstract class AbstractPointGeometryFieldMapper<Parsed, Processed> extends AbstractGeometryFieldMapper<Parsed, Processed> {

    public static class Names extends AbstractGeometryFieldMapper.Names {
        public static final ParseField NULL_VALUE = new ParseField("null_value");
    }

    public abstract static class Builder<T extends Builder,
            FT extends AbstractPointGeometryFieldType> extends AbstractGeometryFieldMapper.Builder<T, FT> {
        public Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType) {
            super(name, fieldType, defaultFieldType);
        }

        public abstract AbstractPointGeometryFieldMapper build(BuilderContext context, String simpleName, MappedFieldType fieldType,
                                MappedFieldType defaultFieldType, Settings indexSettings,
                                MultiFields multiFields, Explicit<Boolean> ignoreMalformed, Explicit<Boolean> ignoreZValue,
                                CopyTo copyTo);


        @Override
        public AbstractPointGeometryFieldMapper build(BuilderContext context) {
            return build(context, name, fieldType, defaultFieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), ignoreMalformed(context),
                ignoreZValue(context), copyTo);
        }


        @Override
        public FT fieldType() {
            return (FT)fieldType;
        }
    }

    public abstract static class TypeParser<Processed, T extends Builder> extends AbstractGeometryFieldMapper.TypeParser<Builder> {
        protected abstract Processed parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed);

        @Override
        @SuppressWarnings("rawtypes")
        public T parse(String name, Map<String, Object> node, Map<String, Object> params, ParserContext parserContext) {
            T builder = (T)(super.parse(name, node, params, parserContext));
            parseField(builder, name, node, parserContext);
            Object nullValue = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();

                if (Names.NULL_VALUE.match(propName, LoggingDeprecationHandler.INSTANCE)) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    nullValue = propNode;
                    iterator.remove();
                }
            }

            if (nullValue != null) {
                builder.nullValue(parseNullValue(nullValue, (Boolean)builder.ignoreZValue().value(),
                    (Boolean)builder.ignoreMalformed().value()));
            }

            return builder;
        }
    }

    public abstract static class AbstractPointGeometryFieldType<Parsed, Processed>
            extends AbstractGeometryFieldType<Parsed, Processed> {
        protected AbstractPointGeometryFieldType() {
            super();
            setHasDocValues(true);
            setDimensions(2, Integer.BYTES);
        }

        protected AbstractPointGeometryFieldType(AbstractPointGeometryFieldType ref) {
            super(ref);
        }
    }

    protected AbstractPointGeometryFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                               Settings indexSettings, MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                               Explicit<Boolean> ignoreZValue, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, ignoreMalformed, ignoreZValue, multiFields, copyTo);
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        AbstractPointGeometryFieldMapper gpfm = (AbstractPointGeometryFieldMapper)other;
        if (gpfm.fieldType().nullValue() != null) {
            this.fieldType().setNullValue(gpfm.fieldType().nullValue());
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field(Names.NULL_VALUE.getPreferredName(), fieldType().nullValue());
        }
    }

    protected abstract ParsedPoint newParsedPoint();

    /** represents a Point that has been parsed by {@link PointParser} */
    public interface ParsedPoint {
        void validate(String fieldName);
        void normalize(String fieldName);
        void resetCoords(double x, double y);
        default boolean isNormalizable(double coord) {
            return Double.isNaN(coord) == false && Double.isInfinite(coord) == false;
        }
    }

    protected void parsePointIgnoringMalformed(XContentParser parser, ParsedPoint point) throws IOException {
        try {
            if (ignoreMalformed.value() == false) {
                point.validate(name());
            } else {
                point.normalize(name());
            }
        } catch (ElasticsearchParseException e) {
            if (ignoreMalformed.value() == false) {
                throw e;
            }
        }
    }

    /** A parser implementation that can parse the various point formats */
    public static class PointParser<P extends ParsedPoint> implements Parser<List<P>> {

        @Override
        public List<P> parse(XContentParser parser, AbstractGeometryFieldMapper mapper) throws IOException, ParseException {
            return geometryFormat(parser, (AbstractPointGeometryFieldMapper)mapper).fromXContent(parser);
        }

        public GeometryFormat<List<P>> geometryFormat(XContentParser parser, AbstractPointGeometryFieldMapper mapper) {
            if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                return new GeometryFormat<List<P>>() {
                    @Override
                    public List<P> fromXContent(XContentParser parser) throws IOException {
                        XContentParser.Token token = parser.nextToken();
                        P point = (P)(mapper.newParsedPoint());
                        ArrayList<P> points = new ArrayList();
                        if (token == XContentParser.Token.VALUE_NUMBER) {
                            double x = parser.doubleValue();
                            parser.nextToken();
                            double y = parser.doubleValue();
                            token = parser.nextToken();
                            if (token == XContentParser.Token.VALUE_NUMBER) {
                                GeoPoint.assertZValue((Boolean)(mapper.ignoreZValue().value()), parser.doubleValue());
                            } else if (token != XContentParser.Token.END_ARRAY) {
                                throw new ElasticsearchParseException("[{}] field type does not accept > 3 dimensions",
                                    mapper.contentType());
                            }

                            point.resetCoords(x, y);
                            if ((Boolean)(mapper.ignoreMalformed().value()) == false) {
                                point.validate(mapper.name());
                            } else {
                                point.normalize(mapper.name());
                            }
                            points.add(point);
                        } else {
                            while (token != XContentParser.Token.END_ARRAY) {
                                mapper.parsePointIgnoringMalformed(parser, point);
                                points.add(point);
                                point = (P)(mapper.newParsedPoint());
                                token = parser.nextToken();
                            }
                        }
                        return points;
                    }

                    @Override
                    public XContentBuilder toXContent(List<P> points, XContentBuilder builder, Params params) throws IOException {
                        return null;
                    }
                };
            } else if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return new GeometryFormat<List<P>>() {
                    @Override
                    public List<P> fromXContent(XContentParser parser) throws IOException, ParseException {
                        P point = null;
                        ArrayList<P> points = null;
                        if (mapper.fieldType().nullValue() != null) {
                            point = (P)(mapper.fieldType().nullValue());
                            if ((Boolean)(mapper.ignoreMalformed().value()) == false) {
                                point.validate(mapper.name());
                            } else {
                                point.normalize(mapper.name());
                            }
                            points = new ArrayList<>();
                            points.add(point);
                        }
                        return points;
                    }

                    @Override
                    public XContentBuilder toXContent(List<P> points, XContentBuilder builder, Params params) throws IOException {
                        return null;
                    }
                };
            } else {
                return new GeometryFormat<List<P>>() {
                    @Override
                    public List<P> fromXContent(XContentParser parser) throws IOException, ParseException {
                        P point = (P)mapper.newParsedPoint();
                        mapper.parsePointIgnoringMalformed(parser, point);
                        ArrayList<P> points = new ArrayList();
                        points.add(point);
                        return points;
                    }

                    @Override
                    public XContentBuilder toXContent(List<P> points, XContentBuilder builder, Params params) throws IOException {
                        return null;
                    }
                };
            }
        }
    }
}
