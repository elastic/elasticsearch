/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.query;

import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.lucene.geo.GeoDistance;
import org.elasticsearch.common.lucene.geo.GeoDistanceDataComparator;
import org.elasticsearch.common.lucene.geo.GeoHashUtils;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.field.function.FieldsFunction;
import org.elasticsearch.index.field.function.script.ScriptFieldsFunction;
import org.elasticsearch.index.field.function.sort.DoubleFieldsFunctionDataComparator;
import org.elasticsearch.index.field.function.sort.StringFieldsFunctionDataComparator;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.xcontent.XContentGeoPointFieldMapper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class SortParseElement implements SearchParseElement {

    private static final SortField SORT_SCORE = new SortField(null, SortField.SCORE);
    private static final SortField SORT_SCORE_REVERSE = new SortField(null, SortField.SCORE, true);
    private static final SortField SORT_DOC = new SortField(null, SortField.DOC);
    private static final SortField SORT_DOC_REVERSE = new SortField(null, SortField.DOC, true);

    public static final String SCRIPT_FIELD_NAME = "_script";
    public static final String SCORE_FIELD_NAME = "_score";
    public static final String DOC_FIELD_NAME = "_doc";
    public static final String GEO_DISTANCE_FIELD_NAME = "_geo_distance";

    public SortParseElement() {
    }

    @Override public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        List<SortField> sortFields = Lists.newArrayListWithCapacity(2);
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    //TODO move to pluggable parsers, similar to facets. Builders already exists...
                    addCompoundSortField(parser, context, sortFields);
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    addSortField(context, sortFields, parser.text(), false);
                }
            }
        } else {
            addCompoundSortField(parser, context, sortFields);
        }
        if (!sortFields.isEmpty()) {
            context.sort(new Sort(sortFields.toArray(new SortField[sortFields.size()])));
        }
    }

    private void addCompoundSortField(XContentParser parser, SearchContext context, List<SortField> sortFields) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                boolean reverse = false;
                String innerJsonName = null;
                token = parser.nextToken();
                if (token == XContentParser.Token.VALUE_STRING) {
                    String direction = parser.text();
                    if (direction.equals("asc")) {
                        reverse = SCORE_FIELD_NAME.equals(fieldName);
                    } else if (direction.equals("desc")) {
                        reverse = !SCORE_FIELD_NAME.equals(fieldName);
                    }
                    addSortField(context, sortFields, fieldName, reverse);
                } else if (GEO_DISTANCE_FIELD_NAME.equals(fieldName)) {
                    addGeoDistanceSortField(parser, context, sortFields);
                } else if (SCRIPT_FIELD_NAME.equals(fieldName)) {
                    addScriptSortField(parser, context, sortFields);
                } else {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            innerJsonName = parser.currentName();
                        } else if (token.isValue()) {
                            if ("reverse".equals(innerJsonName)) {
                                reverse = parser.booleanValue();
                            } else if ("order".equals(innerJsonName)) {
                                if ("asc".equals(parser.text())) {
                                    reverse = SCORE_FIELD_NAME.equals(fieldName);
                                } else if ("desc".equals(parser.text())) {
                                    reverse = !SCORE_FIELD_NAME.equals(fieldName);
                                }
                            }
                        }
                    }
                    addSortField(context, sortFields, fieldName, reverse);
                }
            }
        }
    }

    private void addSortField(SearchContext context, List<SortField> sortFields, String fieldName, boolean reverse) {
        if (SCORE_FIELD_NAME.equals(fieldName)) {
            if (reverse) {
                sortFields.add(SORT_SCORE_REVERSE);
            } else {
                sortFields.add(SORT_SCORE);
            }
        } else if (DOC_FIELD_NAME.equals(fieldName)) {
            if (reverse) {
                sortFields.add(SORT_DOC_REVERSE);
            } else {
                sortFields.add(SORT_DOC);
            }
        } else {
            FieldMapper fieldMapper = context.mapperService().smartNameFieldMapper(fieldName);
            if (fieldMapper == null) {
                throw new SearchParseException(context, "No mapping found for [" + fieldName + "]");
            }
            sortFields.add(new SortField(fieldName, fieldMapper.fieldDataType().newFieldComparatorSource(context.fieldDataCache()), reverse));
        }
    }

    /**
     * <pre>
     * "_script" : {
     *      "script" : "doc[...]",
     *      "order" : "asc"
     * }
     * </pre>
     */
    private void addScriptSortField(XContentParser parser, SearchContext context, List<SortField> sortFields) throws IOException {
        String script = null;
        String type = null;
        Map<String, Object> params = null;
        boolean reverse = false;

        XContentParser.Token token;
        String currentName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token.isValue()) {
                if ("reverse".equals(currentName)) {
                    reverse = parser.booleanValue();
                } else if ("order".equals(currentName)) {
                    reverse = "desc".equals(parser.text());
                } else if ("script".equals(currentName)) {
                    script = parser.text();
                } else if ("type".equals(currentName)) {
                    type = parser.text();
                } else if ("params".equals(currentName)) {
                    params = parser.map();
                }
            }
        }

        if (script == null) {
            throw new SearchParseException(context, "_script sorting requires setting the script to sort by");
        }
        if (type == null) {
            throw new SearchParseException(context, "_script sorting requires setting the type of the script");
        }
        FieldsFunction fieldsFunction = new ScriptFieldsFunction(script, context.scriptService(), context.mapperService(), context.fieldDataCache());
        FieldComparatorSource fieldComparatorSource;
        if ("string".equals(type)) {
            fieldComparatorSource = StringFieldsFunctionDataComparator.comparatorSource(fieldsFunction, params);
        } else if ("number".equals(type)) {
            fieldComparatorSource = DoubleFieldsFunctionDataComparator.comparatorSource(fieldsFunction, params);
        } else {
            throw new SearchParseException(context, "custom script sort type [" + type + "] not supported");
        }
        sortFields.add(new SortField("_script", fieldComparatorSource, reverse));
    }

    /**
     * <pre>
     * "_geo_distance" : {
     *      "pin.location" : {
     *
     *      },
     *      "order" : "asc"
     * }
     * </pre>
     */
    private void addGeoDistanceSortField(XContentParser parser, SearchContext context, List<SortField> sortFields) throws IOException {
        String fieldName = null;
        double lat = Double.NaN;
        double lon = Double.NaN;
        DistanceUnit unit = DistanceUnit.KILOMETERS;
        GeoDistance geoDistance = GeoDistance.ARC;
        boolean reverse = false;


        XContentParser.Token token;
        String currentName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                token = parser.nextToken();
                lat = parser.doubleValue();
                token = parser.nextToken();
                lon = parser.doubleValue();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {

                }
                fieldName = currentName;
            } else if (token == XContentParser.Token.START_OBJECT) {
                // the json in the format of -> field : { lat : 30, lon : 12 }
                fieldName = currentName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentName = parser.currentName();
                    } else if (token.isValue()) {
                        if (currentName.equals(XContentGeoPointFieldMapper.Names.LAT)) {
                            lat = parser.doubleValue();
                        } else if (currentName.equals(XContentGeoPointFieldMapper.Names.LON)) {
                            lon = parser.doubleValue();
                        } else if (currentName.equals(XContentGeoPointFieldMapper.Names.GEOHASH)) {
                            double[] values = GeoHashUtils.decode(parser.text());
                            lat = values[0];
                            lon = values[1];
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("reverse".equals(currentName)) {
                    reverse = parser.booleanValue();
                } else if ("order".equals(currentName)) {
                    reverse = "desc".equals(parser.text());
                } else if (currentName.equals("unit")) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if (currentName.equals("distance_type") || currentName.equals("distanceType")) {
                    geoDistance = GeoDistance.fromString(parser.text());
                } else {
                    // assume the value is the actual value
                    String value = parser.text();
                    int comma = value.indexOf(',');
                    if (comma != -1) {
                        lat = Double.parseDouble(value.substring(0, comma).trim());
                        lon = Double.parseDouble(value.substring(comma + 1).trim());
                    } else {
                        double[] values = GeoHashUtils.decode(value);
                        lat = values[0];
                        lon = values[1];
                    }

                    fieldName = currentName;
                }
            }
        }

        sortFields.add(new SortField(fieldName, GeoDistanceDataComparator.comparatorSource(fieldName, lat, lon, unit, geoDistance, context.fieldDataCache(), context.mapperService()), reverse));
    }
}
