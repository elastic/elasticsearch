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
 * Parser for span_within query
 */
public class SpanWithinQueryParser implements QueryParser<SpanWithinQueryBuilder> {

    public static final ParseField BIG_FIELD = new ParseField("big");
    public static final ParseField LITTLE_FIELD = new ParseField("little");
  
    @Override
    public String[] names() {
        return new String[]{SpanWithinQueryBuilder.NAME, Strings.toCamelCase(SpanWithinQueryBuilder.NAME)};
    }

    @Override
    public SpanWithinQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        SpanQueryBuilder big = null;
        SpanQueryBuilder little = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, BIG_FIELD)) {
                    QueryBuilder query = parseContext.parseInnerQueryBuilder();
                    if (query instanceof SpanQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(), "span_within [big] must be of type span query");
                    }
                    big = (SpanQueryBuilder) query;
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, LITTLE_FIELD)) {
                    QueryBuilder query = parseContext.parseInnerQueryBuilder();
                    if (query instanceof SpanQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(), "span_within [little] must be of type span query");
                    }
                    little = (SpanQueryBuilder) query;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_within] query does not support [" + currentFieldName + "]");
                }
            } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                boost = parser.floatValue();
            } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                queryName = parser.text();
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[span_within] query does not support [" + currentFieldName + "]");
            }
        }

        if (big == null) {
            throw new ParsingException(parser.getTokenLocation(), "span_within must include [big]");
        }
        if (little == null) {
            throw new ParsingException(parser.getTokenLocation(), "span_within must include [little]");
        }

        SpanWithinQueryBuilder query = new SpanWithinQueryBuilder(big, little);
        query.boost(boost).queryName(queryName);
        return query;
    }

    @Override
    public SpanWithinQueryBuilder getBuilderPrototype() {
        return SpanWithinQueryBuilder.PROTOTYPE;
    }
}
