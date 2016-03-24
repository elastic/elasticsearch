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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for span_term query
 */
public class SpanTermQueryParser implements QueryParser<SpanTermQueryBuilder> {

    public static final ParseField TERM_FIELD = new ParseField("term");
  
    @Override
    public String[] names() {
        return new String[]{SpanTermQueryBuilder.NAME, Strings.toCamelCase(SpanTermQueryBuilder.NAME)};
    }

    @Override
    public SpanTermQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, ParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }

        assert token == XContentParser.Token.FIELD_NAME;
        String fieldName = parser.currentName();


        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if (parseContext.parseFieldMatcher().match(currentFieldName, TERM_FIELD)) {
                        value = parser.objectBytes();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, BaseTermQueryBuilder.VALUE_FIELD)) {
                        value = parser.objectBytes();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                        boost = parser.floatValue();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                        queryName = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[span_term] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            value = parser.objectBytes();
            // move to the next token
            parser.nextToken();
        }

        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "No value specified for term query");
        }

        SpanTermQueryBuilder result = new SpanTermQueryBuilder(fieldName, value);
        result.boost(boost).queryName(queryName);
        return result;
    }

    @Override
    public SpanTermQueryBuilder getBuilderPrototype() {
        return SpanTermQueryBuilder.PROTOTYPE;
    }
}
