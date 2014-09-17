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

import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;

/**
 *
 */
public class SpanMultiTermQueryParser implements QueryParser {

    public static final String NAME = "span_multi";
    public static final String MATCH_NAME = "match";

    @Inject
    public SpanMultiTermQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Token token = parser.nextToken();
        if (!MATCH_NAME.equals(parser.currentName()) || token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "spanMultiTerm must have [" + MATCH_NAME + "] multi term query clause");
        }

        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new QueryParsingException(parseContext.index(), "spanMultiTerm must have [" + MATCH_NAME + "] multi term query clause");
        }

        Query subQuery = parseContext.parseInnerQuery();
        if (!(subQuery instanceof MultiTermQuery)) {
            throw new QueryParsingException(parseContext.index(), "spanMultiTerm [" + MATCH_NAME + "] must be of type multi term query");
        }

        parser.nextToken();
        return new SpanMultiTermQueryWrapper<>((MultiTermQuery) subQuery);
    }
}
