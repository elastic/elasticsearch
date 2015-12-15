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
 * Parser for span_not query
 */
public class SpanNotQueryParser implements QueryParser<SpanNotQueryBuilder> {

    public static final ParseField POST_FIELD = new ParseField("post");
    public static final ParseField PRE_FIELD = new ParseField("pre");
    public static final ParseField DIST_FIELD = new ParseField("dist");
    public static final ParseField EXCLUDE_FIELD = new ParseField("exclude");
    public static final ParseField INCLUDE_FIELD = new ParseField("include");
  
    @Override
    public String[] names() {
        return new String[]{SpanNotQueryBuilder.NAME, Strings.toCamelCase(SpanNotQueryBuilder.NAME)};
    }

    @Override
    public SpanNotQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        SpanQueryBuilder include = null;
        SpanQueryBuilder exclude = null;

        Integer dist = null;
        Integer pre  = null;
        Integer post = null;

        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, INCLUDE_FIELD)) {
                    QueryBuilder query = parseContext.parseInnerQueryBuilder();
                    if (!(query instanceof SpanQueryBuilder)) {
                        throw new ParsingException(parser.getTokenLocation(), "spanNot [include] must be of type span query");
                    }
                    include = (SpanQueryBuilder) query;
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, EXCLUDE_FIELD)) {
                    QueryBuilder query = parseContext.parseInnerQueryBuilder();
                    if (!(query instanceof SpanQueryBuilder)) {
                        throw new ParsingException(parser.getTokenLocation(), "spanNot [exclude] must be of type span query");
                    }
                    exclude = (SpanQueryBuilder) query;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_not] query does not support [" + currentFieldName + "]");
                }
            } else {
                if (parseContext.parseFieldMatcher().match(currentFieldName, DIST_FIELD)) {
                    dist = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, PRE_FIELD)) {
                    pre = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, POST_FIELD)) {
                    post = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_not] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (include == null) {
            throw new ParsingException(parser.getTokenLocation(), "spanNot must have [include] span query clause");
        }
        if (exclude == null) {
            throw new ParsingException(parser.getTokenLocation(), "spanNot must have [exclude] span query clause");
        }
        if (dist != null && (pre != null || post != null)) {
            throw new ParsingException(parser.getTokenLocation(), "spanNot can either use [dist] or [pre] & [post] (or none)");
        }

        SpanNotQueryBuilder spanNotQuery = new SpanNotQueryBuilder(include, exclude);
        if (dist != null) {
            spanNotQuery.dist(dist);
        }
        if (pre != null) {
            spanNotQuery.pre(pre);
        }
        if (post != null) {
            spanNotQuery.post(post);
        }
        spanNotQuery.boost(boost);
        spanNotQuery.queryName(queryName);
        return spanNotQuery;
    }

    @Override
    public SpanNotQueryBuilder getBuilderPrototype() {
        return SpanNotQueryBuilder.PROTOTYPE;
    }
}
