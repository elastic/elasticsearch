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
 * Parser for the term query
 */
public class TermQueryParser implements QueryParser<TermQueryBuilder> {

    public static final ParseField TERM_FIELD = new ParseField("term");
    public static final ParseField VALUE_FIELD = new ParseField("value");

    @Override
    public String[] names() {
        return new String[]{TermQueryBuilder.NAME};
    }

    @Override
    public TermQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String queryName = null;
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                // also support a format of "term" : {"field_name" : { ... }}
                if (fieldName != null) {
                    throw new ParsingException(parser.getTokenLocation(), "[term] query does not support different field names, use [bool] query instead");
                }
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if (parseContext.parseFieldMatcher().match(currentFieldName, TERM_FIELD)) {
                            value = parser.objectBytes();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, VALUE_FIELD)) {
                            value = parser.objectBytes();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                            queryName = parser.text();
                        } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                            boost = parser.floatValue();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[term] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (fieldName != null) {
                    throw new ParsingException(parser.getTokenLocation(), "[term] query does not support different field names, use [bool] query instead");
                }
                fieldName = currentFieldName;
                value = parser.objectBytes();
            } else if (token == XContentParser.Token.START_ARRAY) {
                throw new ParsingException(parser.getTokenLocation(), "[term] query does not support array of values");
            }
        }

        TermQueryBuilder termQuery = new TermQueryBuilder(fieldName, value);
        termQuery.boost(boost);
        if (queryName != null) {
            termQuery.queryName(queryName);
        }
        return termQuery;
    }

    @Override
    public TermQueryBuilder getBuilderPrototype() {
        return TermQueryBuilder.PROTOTYPE;
    }
}
