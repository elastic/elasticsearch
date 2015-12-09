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
 * Parser for prefix query
 */
public class PrefixQueryParser implements QueryParser<PrefixQueryBuilder> {

    public static final ParseField PREFIX_FIELD = new ParseField("value", "prefix");
    public static final ParseField REWRITE_FIELD = new ParseField("rewrite");

    @Override
    public String[] names() {
        return new String[]{PrefixQueryBuilder.NAME};
    }

    @Override
    public PrefixQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String fieldName = parser.currentName();
        String value = null;
        String rewrite = null;

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
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
                        if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                            queryName = parser.text();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, PREFIX_FIELD)) {
                            value = parser.textOrNull();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                            boost = parser.floatValue();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, REWRITE_FIELD)) {
                            rewrite = parser.textOrNull();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[regexp] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else {
                    fieldName = currentFieldName;
                    value = parser.textOrNull();
            }
        }

        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "No value specified for prefix query");
        }
        return new PrefixQueryBuilder(fieldName, value)
                .rewrite(rewrite)
                .boost(boost)
                .queryName(queryName);
    }

    @Override
    public PrefixQueryBuilder getBuilderPrototype() {
        return PrefixQueryBuilder.PROTOTYPE;
    }
}
