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
 * Parser for span_first query
 */
public class SpanFirstQueryParser implements QueryParser<SpanFirstQueryBuilder> {

    public static final ParseField MATCH_FIELD = new ParseField("match");
    public static final ParseField END_FIELD = new ParseField("end");
  
    @Override
    public String[] names() {
        return new String[]{SpanFirstQueryBuilder.NAME, Strings.toCamelCase(SpanFirstQueryBuilder.NAME)};
    }

    @Override
    public SpanFirstQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        SpanQueryBuilder match = null;
        Integer end = null;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, MATCH_FIELD)) {
                    QueryBuilder query = parseContext.parseInnerQueryBuilder();
                    if (!(query instanceof SpanQueryBuilder)) {
                        throw new ParsingException(parser.getTokenLocation(), "spanFirst [match] must be of type span query");
                    }
                    match = (SpanQueryBuilder) query;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_first] query does not support [" + currentFieldName + "]");
                }
            } else {
                if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, END_FIELD)) {
                    end = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_first] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (match == null) {
            throw new ParsingException(parser.getTokenLocation(), "spanFirst must have [match] span query clause");
        }
        if (end == null) {
            throw new ParsingException(parser.getTokenLocation(), "spanFirst must have [end] set for it");
        }
        SpanFirstQueryBuilder queryBuilder = new SpanFirstQueryBuilder(match, end);
        queryBuilder.boost(boost).queryName(queryName);
        return queryBuilder;
    }

    @Override
    public SpanFirstQueryBuilder getBuilderPrototype() {
        return SpanFirstQueryBuilder.PROTOTYPE;
    }
}
