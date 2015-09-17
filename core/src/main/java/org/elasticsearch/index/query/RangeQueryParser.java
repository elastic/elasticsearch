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
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for range query
 */
public class RangeQueryParser extends BaseQueryParser<RangeQueryBuilder> {

    private static final ParseField FIELDDATA_FIELD = new ParseField("fielddata").withAllDeprecated("[no replacement]");
    private static final ParseField NAME_FIELD = new ParseField("_name").withAllDeprecated("query name is not supported in short version of range query");

    @Override
    public String[] names() {
        return new String[]{RangeQueryBuilder.NAME};
    }

    @Override
    public RangeQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
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
                        if ("from".equals(currentFieldName)) {
                            from = parser.objectBytes();
                        } else if ("to".equals(currentFieldName)) {
                            to = parser.objectBytes();
                        } else if ("include_lower".equals(currentFieldName) || "includeLower".equals(currentFieldName)) {
                            includeLower = parser.booleanValue();
                        } else if ("include_upper".equals(currentFieldName) || "includeUpper".equals(currentFieldName)) {
                            includeUpper = parser.booleanValue();
                        } else if ("boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else if ("gt".equals(currentFieldName)) {
                            from = parser.objectBytes();
                            includeLower = false;
                        } else if ("gte".equals(currentFieldName) || "ge".equals(currentFieldName)) {
                            from = parser.objectBytes();
                            includeLower = true;
                        } else if ("lt".equals(currentFieldName)) {
                            to = parser.objectBytes();
                            includeUpper = false;
                        } else if ("lte".equals(currentFieldName) || "le".equals(currentFieldName)) {
                            to = parser.objectBytes();
                            includeUpper = true;
                        } else if ("time_zone".equals(currentFieldName) || "timeZone".equals(currentFieldName)) {
                            timeZone = parser.text();
                        } else if ("format".equals(currentFieldName)) {
                            format = parser.text();
                        } else if ("_name".equals(currentFieldName)) {
                            queryName = parser.text();
                        } else {
                            throw new QueryParsingException(parseContext, "[range] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, FIELDDATA_FIELD)) {
                    // ignore
                } else {
                    throw new QueryParsingException(parseContext, "[range] query does not support [" + currentFieldName + "]");
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
