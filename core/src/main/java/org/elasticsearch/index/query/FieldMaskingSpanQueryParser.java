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
 * Parser for field_masking_span query
 */
public class FieldMaskingSpanQueryParser implements QueryParser<FieldMaskingSpanQueryBuilder> {

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField QUERY_FIELD = new ParseField("query");

    @Override
    public String[] names() {
        return new String[]{FieldMaskingSpanQueryBuilder.NAME, Strings.toCamelCase(FieldMaskingSpanQueryBuilder.NAME)};
    }

    @Override
    public FieldMaskingSpanQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        SpanQueryBuilder inner = null;
        String field = null;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    QueryBuilder query = parseContext.parseInnerQueryBuilder();
                    if (!(query instanceof SpanQueryBuilder)) {
                        throw new ParsingException(parser.getTokenLocation(), "[field_masking_span] query must be of type span query");
                    }
                    inner = (SpanQueryBuilder) query;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[field_masking_span] query does not support ["
                            + currentFieldName + "]");
                }
            } else {
                if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, FIELD_FIELD)) {
                    field = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[field_masking_span] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (inner == null) {
            throw new ParsingException(parser.getTokenLocation(), "field_masking_span must have [query] span query clause");
        }
        if (field == null) {
            throw new ParsingException(parser.getTokenLocation(), "field_masking_span must have [field] set for it");
        }

        FieldMaskingSpanQueryBuilder queryBuilder = new FieldMaskingSpanQueryBuilder(inner, field);
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        return queryBuilder;
    }

    @Override
    public FieldMaskingSpanQueryBuilder getBuilderPrototype() {
        return FieldMaskingSpanQueryBuilder.PROTOTYPE;
    }
}
