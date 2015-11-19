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

import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.search.GeoPointDistanceRangeQuery;
import org.apache.lucene.spatial.util.GeoDistanceUtils;
import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.search.geo.GeoDistanceRangeQuery;

import java.io.IOException;

import static org.apache.lucene.spatial.util.GeoEncodingUtils.TOLERANCE;

/**
 * <pre>
 * {
 *     "name.lat" : 1.1,
 *     "name.lon" : 1.2,
 * }
 * </pre>
 */
public class GeoDistanceRangeQueryParser implements QueryParser {

    public static final String NAME = "geo_distance_range";

    @Inject
    public GeoDistanceRangeQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "geoDistanceRange"};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token;

        String queryName = null;
        String currentFieldName = null;
        GeoPoint point = new GeoPoint();
        String fieldName = null;
        Object vFrom = null;
        Object vTo = null;
        boolean includeLower = true;
        boolean includeUpper = true;
        DistanceUnit unit = DistanceUnit.DEFAULT;
        GeoDistance geoDistance = GeoDistance.DEFAULT;
        String optimizeBbox = "memory";
        final boolean indexCreatedBeforeV2_0 = parseContext.indexVersionCreated().before(Version.V_2_0_0);
        final boolean indexCreatedBeforeV2_2 = parseContext.indexVersionCreated().before(Version.V_2_2_0);
        boolean coerce = false;
        boolean ignoreMalformed = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_ARRAY) {
                GeoUtils.parseGeoPoint(parser, point);
                fieldName = currentFieldName;
            } else if (token == XContentParser.Token.START_OBJECT) {
                // the json in the format of -> field : { lat : 30, lon : 12 }
                fieldName = currentFieldName;
                GeoUtils.parseGeoPoint(parser, point);
            } else if (token.isValue()) {
                if (currentFieldName.equals("from")) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vFrom = parser.text(); // a String
                    } else {
                        vFrom = parser.numberValue(); // a Number
                    }
                } else if (currentFieldName.equals("to")) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vTo = parser.text(); // a String
                    } else {
                        vTo = parser.numberValue(); // a Number
                    }
                } else if ("include_lower".equals(currentFieldName) || "includeLower".equals(currentFieldName)) {
                    includeLower = parser.booleanValue();
                } else if ("include_upper".equals(currentFieldName) || "includeUpper".equals(currentFieldName)) {
                    includeUpper = parser.booleanValue();
                } else if ("gt".equals(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vFrom = parser.text(); // a String
                    } else {
                        vFrom = parser.numberValue(); // a Number
                    }
                    includeLower = false;
                } else if ("gte".equals(currentFieldName) || "ge".equals(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vFrom = parser.text(); // a String
                    } else {
                        vFrom = parser.numberValue(); // a Number
                    }
                    includeLower = true;
                } else if ("lt".equals(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vTo = parser.text(); // a String
                    } else {
                        vTo = parser.numberValue(); // a Number
                    }
                    includeUpper = false;
                } else if ("lte".equals(currentFieldName) || "le".equals(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vTo = parser.text(); // a String
                    } else {
                        vTo = parser.numberValue(); // a Number
                    }
                    includeUpper = true;
                } else if (currentFieldName.equals("unit")) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if (currentFieldName.equals("distance_type") || currentFieldName.equals("distanceType")) {
                    geoDistance = GeoDistance.fromString(parser.text());
                } else if (currentFieldName.endsWith(GeoPointFieldMapper.Names.LAT_SUFFIX)) {
                    point.resetLat(parser.doubleValue());
                    fieldName = currentFieldName.substring(0, currentFieldName.length() - GeoPointFieldMapper.Names.LAT_SUFFIX.length());
                } else if (currentFieldName.endsWith(GeoPointFieldMapper.Names.LON_SUFFIX)) {
                    point.resetLon(parser.doubleValue());
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("optimize_bbox".equals(currentFieldName) || "optimizeBbox".equals(currentFieldName)) {
                    optimizeBbox = parser.textOrNull();
                } else if ("coerce".equals(currentFieldName) || (indexCreatedBeforeV2_0 && "normalize".equals(currentFieldName))) {
                    coerce = parser.booleanValue();
                    if (coerce == true) {
                        ignoreMalformed = true;
                    }
                } else if ("ignore_malformed".equals(currentFieldName) && coerce == false) {
                    ignoreMalformed = parser.booleanValue();
                } else {
                    point.resetFromString(parser.text());
                    fieldName = currentFieldName;
                }
            }
        }

        // validation was not available prior to 2.x, so to support bwc percolation queries we only ignore_malformed on 2.x created indexes
        if (!indexCreatedBeforeV2_0 && !ignoreMalformed) {
            if (point.lat() > 90.0 || point.lat() < -90.0) {
                throw new QueryParsingException(parseContext, "illegal latitude value [{}] for [{}]", point.lat(), NAME);
            }
            if (point.lon() > 180.0 || point.lon() < -180) {
                throw new QueryParsingException(parseContext, "illegal longitude value [{}] for [{}]", point.lon(), NAME);
            }
        }

        if (indexCreatedBeforeV2_2 == false || coerce) {
            GeoUtils.normalizePoint(point, coerce, coerce);
        }

        Double from;
        Double to;
        if (vFrom != null) {
            if (vFrom instanceof Number) {
                from = unit.toMeters(((Number) vFrom).doubleValue());
            } else {
                from = DistanceUnit.parse((String) vFrom, unit, DistanceUnit.DEFAULT);
            }
            if (indexCreatedBeforeV2_2 == true) {
                from = geoDistance.normalize(from, DistanceUnit.DEFAULT);
            }
        } else {
            from = new Double(0);
        }
        if (vTo != null) {
            if (vTo instanceof Number) {
                to = unit.toMeters(((Number) vTo).doubleValue());
            } else {
                to = DistanceUnit.parse((String) vTo, unit, DistanceUnit.DEFAULT);
            }
            if (indexCreatedBeforeV2_2 == true) {
                to = geoDistance.normalize(to, DistanceUnit.DEFAULT);
            }
        } else {
            to = GeoDistanceUtils.maxRadialDistanceMeters(point.lon(), point.lat());
        }

        MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new QueryParsingException(parseContext, "failed to find geo_point field [" + fieldName + "]");
        }
        if (!(fieldType instanceof GeoPointFieldMapper.GeoPointFieldType)) {
            throw new QueryParsingException(parseContext, "field [" + fieldName + "] is not a geo_point field");
        }
        GeoPointFieldMapper.GeoPointFieldType geoFieldType = ((GeoPointFieldMapper.GeoPointFieldType) fieldType);

        IndexGeoPointFieldData indexFieldData = parseContext.getForField(fieldType);

        if (from == null) {
            from = new Double(0);
        }

        if (to == null) {
            // computes the maximum distance at the given latitude
            // todo move to Lucene GeoUtils
            to = new Double(SloppyMath.haversin(point.lat(), point.lon(), point.lat(), (180.0 + point.lon()) % 360))*1000.0;
        }

        final Query query;
        if (indexCreatedBeforeV2_2 == true) {
            query = new GeoDistanceRangeQuery(point, from, to, includeLower, includeUpper, geoDistance, geoFieldType, indexFieldData, optimizeBbox);
        } else {
            // if index created V_2_2 use (soon to be legacy) numeric encoding postings format
            // if index created V_2_3 > use prefix encoded postings format
            final GeoPointField.TermEncoding encoding = (parseContext.indexVersionCreated().before(Version.V_2_3_0)) ?
                GeoPointField.TermEncoding.NUMERIC : GeoPointField.TermEncoding.PREFIX;

            query = new GeoPointDistanceRangeQuery(indexFieldData.getFieldNames().indexName(), encoding, point.lon(), point.lat(),
                (includeLower) ? from : from + TOLERANCE,
                (includeUpper) ? to : to - TOLERANCE);
        }

        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
