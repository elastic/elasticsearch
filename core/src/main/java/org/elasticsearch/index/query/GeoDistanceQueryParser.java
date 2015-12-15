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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;

import java.io.IOException;

/**
 * Parses a GeoDistanceQuery. See also
 *
 * <pre>
 * {
 *     "name.lat" : 1.1,
 *     "name.lon" : 1.2,
 * }
 * </pre>
 */
public class GeoDistanceQueryParser implements QueryParser<GeoDistanceQueryBuilder> {

    public static final ParseField VALIDATION_METHOD_FIELD = new ParseField("validation_method");
    public static final ParseField IGNORE_MALFORMED_FIELD = new ParseField("ignore_malformed");
    public static final ParseField COERCE_FIELD = new ParseField("coerce", "normalize");
    public static final ParseField OPTIMIZE_BBOX_FIELD = new ParseField("optimize_bbox");
    public static final ParseField DISTANCE_TYPE_FIELD = new ParseField("distance_type");
    public static final ParseField UNIT_FIELD = new ParseField("unit");
    public static final ParseField DISTANCE_FIELD = new ParseField("distance");

    @Override
    public String[] names() {
        return new String[]{GeoDistanceQueryBuilder.NAME, "geoDistance"};
    }

    @Override
    public GeoDistanceQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token;

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
        GeoPoint point = new GeoPoint(Double.NaN, Double.NaN);
        String fieldName = null;
        Object vDistance = null;
        DistanceUnit unit = GeoDistanceQueryBuilder.DEFAULT_DISTANCE_UNIT;
        GeoDistance geoDistance = GeoDistanceQueryBuilder.DEFAULT_GEO_DISTANCE;
        String optimizeBbox = GeoDistanceQueryBuilder.DEFAULT_OPTIMIZE_BBOX;
        boolean coerce = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        boolean ignoreMalformed = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        GeoValidationMethod validationMethod = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_ARRAY) {
                fieldName = currentFieldName;
                GeoUtils.parseGeoPoint(parser, point);
            } else if (token == XContentParser.Token.START_OBJECT) {
                // the json in the format of -> field : { lat : 30, lon : 12 }
                String currentName = parser.currentName();
                assert currentFieldName != null;
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentName = parser.currentName();
                    } else if (token.isValue()) {
                        if (currentName.equals(GeoPointFieldMapper.Names.LAT)) {
                            point.resetLat(parser.doubleValue());
                        } else if (currentName.equals(GeoPointFieldMapper.Names.LON)) {
                            point.resetLon(parser.doubleValue());
                        } else if (currentName.equals(GeoPointFieldMapper.Names.GEOHASH)) {
                            point.resetFromGeoHash(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[geo_distance] query does not support [" + currentFieldName
                                    + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, DISTANCE_FIELD)) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        vDistance = parser.text(); // a String
                    } else {
                        vDistance = parser.numberValue(); // a Number
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, UNIT_FIELD)) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, DISTANCE_TYPE_FIELD)) {
                    geoDistance = GeoDistance.fromString(parser.text());
                } else if (currentFieldName.endsWith(GeoPointFieldMapper.Names.LAT_SUFFIX)) {
                    point.resetLat(parser.doubleValue());
                    fieldName = currentFieldName.substring(0, currentFieldName.length() - GeoPointFieldMapper.Names.LAT_SUFFIX.length());
                } else if (currentFieldName.endsWith(GeoPointFieldMapper.Names.LON_SUFFIX)) {
                    point.resetLon(parser.doubleValue());
                    fieldName = currentFieldName.substring(0, currentFieldName.length() - GeoPointFieldMapper.Names.LON_SUFFIX.length());
                } else if (currentFieldName.endsWith(GeoPointFieldMapper.Names.GEOHASH_SUFFIX)) {
                    point.resetFromGeoHash(parser.text());
                    fieldName = currentFieldName.substring(0, currentFieldName.length() - GeoPointFieldMapper.Names.GEOHASH_SUFFIX.length());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, OPTIMIZE_BBOX_FIELD)) {
                    optimizeBbox = parser.textOrNull();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, COERCE_FIELD)) {
                    coerce = parser.booleanValue();
                    if (coerce == true) {
                        ignoreMalformed = true;
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, IGNORE_MALFORMED_FIELD)) {
                    ignoreMalformed = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, VALIDATION_METHOD_FIELD)) {
                    validationMethod = GeoValidationMethod.fromString(parser.text());
                } else {
                    if (fieldName == null) {
                        point.resetFromString(parser.text());
                        fieldName = currentFieldName;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[" + GeoDistanceQueryBuilder.NAME +
                                "] field name already set to [" + fieldName + "] but found [" + currentFieldName + "]");
                    }
                }
            }
        }

        if (vDistance == null) {
            throw new ParsingException(parser.getTokenLocation(), "geo_distance requires 'distance' to be specified");
        }

        GeoDistanceQueryBuilder qb = new GeoDistanceQueryBuilder(fieldName);
        if (vDistance instanceof Number) {
            qb.distance(((Number) vDistance).doubleValue(), unit);
        } else {
            qb.distance((String) vDistance, unit);
        }
        qb.point(point);
        if (validationMethod != null) {
            qb.setValidationMethod(validationMethod);
        } else {
            qb.setValidationMethod(GeoValidationMethod.infer(coerce, ignoreMalformed));
        }
        qb.optimizeBbox(optimizeBbox);
        qb.geoDistance(geoDistance);
        qb.boost(boost);
        qb.queryName(queryName);
        return qb;
    }

    @Override
    public GeoDistanceQueryBuilder getBuilderPrototype() {
        return GeoDistanceQueryBuilder.PROTOTYPE;
    }
}
