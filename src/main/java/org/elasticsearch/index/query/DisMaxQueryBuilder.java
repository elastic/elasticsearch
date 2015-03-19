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

import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * A query that generates the union of documents produced by its sub-queries, and that scores each document
 * with the maximum score for that document as produced by any sub-query, plus a tie breaking increment for any
 * additional matching sub-queries.
 *
 *
 */
public class DisMaxQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<DisMaxQueryBuilder> {

    private ArrayList<QueryBuilder> queries = newArrayList();

    private float boost = -1;

    private float tieBreaker = -1;

    private String queryName;

    public static final String NAME = "dis_max";

    @Inject
    public DisMaxQueryBuilder() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    /**
     * Add a sub-query to this disjunction.
     */
    public DisMaxQueryBuilder add(QueryBuilder queryBuilder) {
        queries.add(queryBuilder);
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public DisMaxQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * The score of each non-maximum disjunct for a document is multiplied by this weight
     * and added into the final score.  If non-zero, the value should be small, on the order of 0.1, which says that
     * 10 occurrences of word in a lower-scored field that is also in a higher scored field is just as good as a unique
     * word in the lower scored field (i.e., one that is not in any higher scored field.
     */
    public DisMaxQueryBuilder tieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public DisMaxQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(DisMaxQueryBuilder.NAME);
        if (tieBreaker != -1) {
            builder.field("tie_breaker", tieBreaker);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.startArray("queries");
        for (QueryBuilder queryBuilder : queries) {
            queryBuilder.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        float boost = 1.0f;
        float tieBreaker = 0.0f;

        List<Query> queries = newArrayList();
        boolean queriesFound = false;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("queries".equals(currentFieldName)) {
                    queriesFound = true;
                    Query query = parseContext.parseInnerQuery();
                    if (query != null) {
                        queries.add(query);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[dis_max] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("queries".equals(currentFieldName)) {
                    queriesFound = true;
                    while (token != XContentParser.Token.END_ARRAY) {
                        Query query = parseContext.parseInnerQuery();
                        if (query != null) {
                            queries.add(query);
                        }
                        token = parser.nextToken();
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[dis_max] query does not support [" + currentFieldName + "]");
                }
            } else {
                if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("tie_breaker".equals(currentFieldName) || "tieBreaker".equals(currentFieldName)) {
                    tieBreaker = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[dis_max] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (!queriesFound) {
            throw new QueryParsingException(parseContext.index(), "[dis_max] requires 'queries' field");
        }

        if (queries.isEmpty()) {
            return null;
        }

        DisjunctionMaxQuery query = new DisjunctionMaxQuery(queries, tieBreaker);
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
