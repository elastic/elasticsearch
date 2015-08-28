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

/**
 * Parser for or query
 * @deprecated use bool query instead
 */
@Deprecated
public class OrQueryParser extends BaseQueryParser<OrQueryBuilder> {

    @Inject
    public OrQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{OrQueryBuilder.NAME};
    }

    @Override
    public OrQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        final ArrayList<QueryBuilder> queries = new ArrayList<>();
        boolean queriesFound = false;

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
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
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("filters".equals(currentFieldName)) {
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
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else {
                        throw new QueryParsingException(parseContext, "[or] query does not support [" + currentFieldName + "]");
                    }
                }
            }
        }

        if (!queriesFound) {
            throw new QueryParsingException(parseContext, "[or] query requires 'filters' to be set on it'");
        }

        OrQueryBuilder orQuery = new OrQueryBuilder();
        for (QueryBuilder query : queries) {
            orQuery.add(query);
        }
        orQuery.queryName(queryName);
        orQuery.boost(boost);
        return orQuery;
    }

    @Override
    public OrQueryBuilder getBuilderPrototype() {
        return OrQueryBuilder.PROTOTYPE;
    }
}