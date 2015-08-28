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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for regexp query
 */
public class RegexpQueryParser extends BaseQueryParser<RegexpQueryBuilder> {

    private static final ParseField NAME_FIELD = new ParseField("_name").withAllDeprecated("query name is not supported in short version of regexp query");

    @Inject
    public RegexpQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{RegexpQueryBuilder.NAME};
    }

    @Override
    public RegexpQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = parser.currentName();
        String rewrite = null;

        String value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        int flagsValue = RegexpQueryBuilder.DEFAULT_FLAGS_VALUE;
        int maxDeterminizedStates = RegexpQueryBuilder.DEFAULT_MAX_DETERMINIZED_STATES;
        String queryName = null;
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
                        if ("value".equals(currentFieldName)) {
                            value = parser.textOrNull();
                        } else if ("boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else if ("rewrite".equals(currentFieldName)) {
                            rewrite = parser.textOrNull();
                        } else if ("flags".equals(currentFieldName)) {
                            String flags = parser.textOrNull();
                            flagsValue = RegexpFlag.resolveValue(flags);
                        } else if ("max_determinized_states".equals(currentFieldName)) {
                            maxDeterminizedStates = parser.intValue();
                        } else if ("flags_value".equals(currentFieldName)) {
                            flagsValue = parser.intValue();
                        } else if ("_name".equals(currentFieldName)) {
                            queryName = parser.text();
                        } else {
                            throw new QueryParsingException(parseContext, "[regexp] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else {
                if (parseContext.parseFieldMatcher().match(currentFieldName, NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    fieldName = currentFieldName;
                    value = parser.textOrNull();
                }
            }
        }

        if (value == null) {
            throw new QueryParsingException(parseContext, "No value specified for regexp query");
        }
        return new RegexpQueryBuilder(fieldName, value)
                .flags(flagsValue)
                .maxDeterminizedStates(maxDeterminizedStates)
                .rewrite(rewrite)
                .boost(boost)
                .queryName(queryName);
    }

    @Override
    public RegexpQueryBuilder getBuilderPrototype() {
        return RegexpQueryBuilder.PROTOTYPE;
    }
}
