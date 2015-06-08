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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
@Deprecated
public class FilteredQueryParser implements QueryParser {

    public static final String NAME = "filtered";

    @Inject
    public FilteredQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = Queries.newMatchAllQuery();
        Query filter = null;
        boolean filterFound = false;
        float boost = 1.0f;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                } else if ("filter".equals(currentFieldName)) {
                    filterFound = true;
                    filter = parseContext.parseInnerFilter();
                } else {
                    throw new QueryParsingException(parseContext, "[filtered] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("strategy".equals(currentFieldName)) {
                    // ignore
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else {
                    throw new QueryParsingException(parseContext, "[filtered] query does not support [" + currentFieldName + "]");
                }
            }
        }

        // parsed internally, but returned null during parsing...
        if (query == null) {
            return null;
        }

        if (filter == null) {
            if (!filterFound) {
                // we allow for null filter, so it makes compositions on the client side to be simpler
                return query;
            } else {
                // even if the filter is not found, and its null, we should simply ignore it, and go
                // by the query
                return query;
            }
        }
        if (Queries.isConstantMatchAllQuery(filter)) {
            // this is an instance of match all filter, just execute the query
            return query;
        }

        // if its a match_all query, use constant_score
        if (Queries.isConstantMatchAllQuery(query)) {
            Query q = new ConstantScoreQuery(filter);
            q.setBoost(boost);
            return q;
        }

        BooleanQuery filteredQuery = Queries.filtered(query, filter);

        filteredQuery.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, filteredQuery);
        }
        return filteredQuery;
    }
}
