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

package org.elasticsearch.index.query;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class GeoBoundingBoxQueryParser implements QueryParser<GeoBoundingBoxQueryBuilder> {

    public static final String NAME = "geo_bbox";

    public static final ParseField IGNORE_MALFORMED_FIELD = new ParseField("ignore_malformed");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField VALIDATION_METHOD_FIELD = new ParseField("validation_method");
    public static final ParseField COERCE_FIELD = new ParseField("coerce", "normalize");
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField TOP_FIELD = new ParseField("top");
    public static final ParseField BOTTOM_FIELD = new ParseField("bottom");
    public static final ParseField LEFT_FIELD = new ParseField("left");
    public static final ParseField RIGHT_FIELD = new ParseField("right");
    public static final ParseField TOP_LEFT_FIELD = new ParseField("top_left");
    public static final ParseField BOTTOM_RIGHT_FIELD = new ParseField("bottom_right");
    public static final ParseField TOP_RIGHT_FIELD = new ParseField("top_right");
    public static final ParseField BOTTOM_LEFT_FIELD = new ParseField("bottom_left");

    @Override
    public String[] names() {
        return new String[]{GeoBoundingBoxQueryBuilder.NAME, "geoBbox", "geo_bounding_box", "geoBoundingBox"};
    }

    @Override
    public GeoBoundingBoxQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;

        double top = Double.NaN;
        double bottom = Double.NaN;
        double left = Double.NaN;
        double right = Double.NaN;

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        boolean coerce = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        boolean ignoreMalformed = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        GeoValidationMethod validationMethod = null;

        GeoPoint sparse = new GeoPoint();

        String type = "memory";

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (parseContext.isDeprecatedSetting(currentFieldName)) {
                            // skip
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, FIELD_FIELD)) {
                            fieldName = parser.text();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, TOP_FIELD)) {
                            top = parser.doubleValue();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, BOTTOM_FIELD)) {
                            bottom = parser.doubleValue();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, LEFT_FIELD)) {
                            left = parser.doubleValue();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, RIGHT_FIELD)) {
                            right = parser.doubleValue();
                        } else {
                            if (parseContext.parseFieldMatcher().match(currentFieldName, TOP_LEFT_FIELD)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                top = sparse.getLat();
                                left = sparse.getLon();
                            } else if (parseContext.parseFieldMatcher().match(currentFieldName, BOTTOM_RIGHT_FIELD)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                bottom = sparse.getLat();
                                right = sparse.getLon();
                            } else if (parseContext.parseFieldMatcher().match(currentFieldName, TOP_RIGHT_FIELD)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                top = sparse.getLat();
                                right = sparse.getLon();
                            } else if (parseContext.parseFieldMatcher().match(currentFieldName, BOTTOM_LEFT_FIELD)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                bottom = sparse.getLat();
                                left = sparse.getLon();
                            } else {
                                throw new ElasticsearchParseException("failed to parse [{}] query. unexpected field [{}]", NAME, currentFieldName);
                            }
                        }
                    } else {
                        throw new ElasticsearchParseException("failed to parse [{}] query. field name expected but [{}] found", NAME, token);
                    }
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, COERCE_FIELD)) {
                    coerce = parser.booleanValue();
                    if (coerce) {
                        ignoreMalformed = true;
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, VALIDATION_METHOD_FIELD)) {
                    validationMethod = GeoValidationMethod.fromString(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, TYPE_FIELD)) {
                    type = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, IGNORE_MALFORMED_FIELD)) {
                    ignoreMalformed = parser.booleanValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. unexpected field [{}]", NAME, currentFieldName);
                }
            }
        }

        final GeoPoint topLeft = sparse.reset(top, left);  //just keep the object
        final GeoPoint bottomRight = new GeoPoint(bottom, right);
        GeoBoundingBoxQueryBuilder builder = new GeoBoundingBoxQueryBuilder(fieldName);
        builder.setCorners(topLeft, bottomRight);
        builder.queryName(queryName);
        builder.boost(boost);
        builder.type(GeoExecType.fromString(type));
        if (validationMethod != null) {
            // ignore deprecated coerce/ignoreMalformed settings if validationMethod is set
            builder.setValidationMethod(validationMethod);
        } else {
            builder.setValidationMethod(GeoValidationMethod.infer(coerce, ignoreMalformed));
        }
        return builder;
    }

    @Override
    public GeoBoundingBoxQueryBuilder getBuilderPrototype() {
        return GeoBoundingBoxQueryBuilder.PROTOTYPE;
    }
}
