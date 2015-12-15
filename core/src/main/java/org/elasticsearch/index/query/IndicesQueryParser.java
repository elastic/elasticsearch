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
import java.util.ArrayList;
import java.util.Collection;

/**
 * Parser for {@link IndicesQueryBuilder}.
 */
public class IndicesQueryParser implements QueryParser {

    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField NO_MATCH_QUERY = new ParseField("no_match_query");
    public static final ParseField INDEX_FIELD = new ParseField("index");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    

    @Override
    public String[] names() {
        return new String[]{IndicesQueryBuilder.NAME};
    }

    @Override
    public QueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, ParsingException {
        XContentParser parser = parseContext.parser();

        QueryBuilder innerQuery = null;
        Collection<String> indices = new ArrayList<>();
        QueryBuilder noMatchQuery = IndicesQueryBuilder.defaultNoMatchQuery();

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    innerQuery = parseContext.parseInnerQueryBuilder();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, NO_MATCH_QUERY)) {
                    noMatchQuery = parseContext.parseInnerQueryBuilder();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[indices] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, INDICES_FIELD)) {
                    if (indices.isEmpty() == false) {
                        throw new ParsingException(parser.getTokenLocation(), "[indices] indices or index already specified");
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        String value = parser.textOrNull();
                        if (value == null) {
                            throw new ParsingException(parser.getTokenLocation(), "[indices] no value specified for 'indices' entry");
                        }
                        indices.add(value);
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[indices] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, INDEX_FIELD)) {
                    if (indices.isEmpty() == false) {
                        throw new ParsingException(parser.getTokenLocation(), "[indices] indices or index already specified");
                    }
                    indices.add(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, NO_MATCH_QUERY)) {
                    noMatchQuery = parseNoMatchQuery(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[indices] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (innerQuery == null) {
            throw new ParsingException(parser.getTokenLocation(), "[indices] requires 'query' element");
        }
        if (indices.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "[indices] requires 'indices' or 'index' element");
        }
        return new IndicesQueryBuilder(innerQuery, indices.toArray(new String[indices.size()]))
                .noMatchQuery(noMatchQuery)
                .boost(boost)
                .queryName(queryName);
    }

    static QueryBuilder parseNoMatchQuery(String type) {
        if ("all".equals(type)) {
            return QueryBuilders.matchAllQuery();
        } else if ("none".equals(type)) {
            return new MatchNoneQueryBuilder();
        }
        throw new IllegalArgumentException("query type can only be [all] or [none] but not " + "[" + type + "]");
    }

    @Override
    public IndicesQueryBuilder getBuilderPrototype() {
        return IndicesQueryBuilder.PROTOTYPE;
    }
}
