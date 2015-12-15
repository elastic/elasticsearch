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
 * Parser for regexp query
 */
public class RegexpQueryParser implements QueryParser<RegexpQueryBuilder> {

    public static final ParseField NAME_FIELD = new ParseField("_name").withAllDeprecated("query name is not supported in short version of regexp query");
    public static final ParseField FLAGS_VALUE_FIELD = new ParseField("flags_value");
    public static final ParseField MAX_DETERMINIZED_STATES_FIELD = new ParseField("max_determinized_states");
    public static final ParseField FLAGS_FIELD = new ParseField("flags");
    public static final ParseField REWRITE_FIELD = new ParseField("rewrite");
    public static final ParseField VALUE_FIELD = new ParseField("value");

    @Override
    public String[] names() {
        return new String[]{RegexpQueryBuilder.NAME};
    }

    @Override
    public RegexpQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
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
                        if (parseContext.parseFieldMatcher().match(currentFieldName, VALUE_FIELD)) {
                            value = parser.textOrNull();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                            boost = parser.floatValue();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, REWRITE_FIELD)) {
                            rewrite = parser.textOrNull();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, FLAGS_FIELD)) {
                            String flags = parser.textOrNull();
                            flagsValue = RegexpFlag.resolveValue(flags);
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, MAX_DETERMINIZED_STATES_FIELD)) {
                            maxDeterminizedStates = parser.intValue();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, FLAGS_VALUE_FIELD)) {
                            flagsValue = parser.intValue();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[regexp] query does not support [" + currentFieldName + "]");
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
            throw new ParsingException(parser.getTokenLocation(), "No value specified for regexp query");
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
