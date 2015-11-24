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
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for range query
 */
public class RangeQueryParser implements QueryParser<RangeQueryBuilder> {

    public static final ParseField FIELDDATA_FIELD = new ParseField("fielddata").withAllDeprecated("[no replacement]");
    public static final ParseField NAME_FIELD = new ParseField("_name").withAllDeprecated("query name is not supported in short version of range query");
    public static final ParseField LTE_FIELD = new ParseField("lte", "le");
    public static final ParseField GTE_FIELD = new ParseField("gte", "ge");
    public static final ParseField FROM_FIELD = new ParseField("from");
    public static final ParseField TO_FIELD = new ParseField("to");
    public static final ParseField INCLUDE_LOWER_FIELD = new ParseField("include_lower");
    public static final ParseField INCLUDE_UPPER_FIELD = new ParseField("include_upper");
    public static final ParseField GT_FIELD = new ParseField("gt");
    public static final ParseField LT_FIELD = new ParseField("lt");
    public static final ParseField TIME_ZONE_FIELD = new ParseField("time_zone");
    public static final ParseField FORMAT_FIELD = new ParseField("format");

    @Override
    public String[] names() {
        return new String[]{RangeQueryBuilder.NAME};
    }

    @Override
    public RangeQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        Object from = null;
        Object to = null;
        boolean includeLower = RangeQueryBuilder.DEFAULT_INCLUDE_LOWER;
        boolean includeUpper = RangeQueryBuilder.DEFAULT_INCLUDE_UPPER;
        String timeZone = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String format = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if (parseContext.parseFieldMatcher().match(currentFieldName, FROM_FIELD)) {
                            from = parser.objectBytes();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, TO_FIELD)) {
                            to = parser.objectBytes();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, INCLUDE_LOWER_FIELD)) {
                            includeLower = parser.booleanValue();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, INCLUDE_UPPER_FIELD)) {
                            includeUpper = parser.booleanValue();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                            boost = parser.floatValue();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, GT_FIELD)) {
                            from = parser.objectBytes();
                            includeLower = false;
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, GTE_FIELD)) {
                            from = parser.objectBytes();
                            includeLower = true;
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, LT_FIELD)) {
                            to = parser.objectBytes();
                            includeUpper = false;
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, LTE_FIELD)) {
                            to = parser.objectBytes();
                            includeUpper = true;
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, TIME_ZONE_FIELD)) {
                            timeZone = parser.text();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, FORMAT_FIELD)) {
                            format = parser.text();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[range] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, FIELDDATA_FIELD)) {
                    // ignore
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[range] query does not support [" + currentFieldName + "]");
                }
            }
        }

        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(fieldName);
        rangeQuery.from(from);
        rangeQuery.to(to);
        rangeQuery.includeLower(includeLower);
        rangeQuery.includeUpper(includeUpper);
        if (timeZone != null) {
            rangeQuery.timeZone(timeZone);
        }
        rangeQuery.boost(boost);
        rangeQuery.queryName(queryName);
        if (format != null) {
            rangeQuery.format(format);
        }
        return rangeQuery;
    }

    @Override
    public RangeQueryBuilder getBuilderPrototype() {
        return RangeQueryBuilder.PROTOTYPE;
    }
}
