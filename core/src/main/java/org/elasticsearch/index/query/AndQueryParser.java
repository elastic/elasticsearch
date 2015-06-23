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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
@Deprecated
public class AndQueryParser extends BaseQueryParser {

    @Inject
    public AndQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{AndQueryBuilder.NAME};
    }

    @Override
    public QueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        final ArrayList<QueryBuilder> queries = newArrayList();
        boolean queriesFound = false;

        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                queriesFound = true;
                QueryBuilder filter = parseContext.parseInnerFilterToQueryBuilder();
                if (filter != null) {
                    queries.add(filter);
                }
            }
        } else {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                    // skip
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("filters".equals(currentFieldName)) {
                        queriesFound = true;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            QueryBuilder filter = parseContext.parseInnerFilterToQueryBuilder();
                            if (filter != null) {
                                queries.add(filter);
                            }
                        }
                    } else {
                        queriesFound = true;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            QueryBuilder filter = parseContext.parseInnerFilterToQueryBuilder();
                            if (filter != null) {
                                queries.add(filter);
                            }
                        }
                    }
                } else if (token.isValue()) {
                    if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    } else {
                        throw new QueryParsingException(parseContext, "[and] query does not support [" + currentFieldName + "]");
                    }
                }
            }
        }

        if (!queriesFound) {
            throw new QueryParsingException(parseContext, "[and] query requires 'filters' to be set on it'");
        }

        AndQueryBuilder andQuery = new AndQueryBuilder();
        for (QueryBuilder query : queries) {
            andQuery.add(query);
        }
        andQuery.queryName(queryName);
        return andQuery;
    }

    @Override
    public AndQueryBuilder getBuilderPrototype() {
        return AndQueryBuilder.PROTOTYPE;
    }
}