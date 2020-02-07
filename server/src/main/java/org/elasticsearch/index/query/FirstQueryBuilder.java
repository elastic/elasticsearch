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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.first.FirstQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * First query where a document gets score from the first matching query
 */
public class FirstQueryBuilder extends AbstractQueryBuilder<FirstQueryBuilder> {
    public static final String NAME = "first";
    public static int maxClauseCount = 1024;

    private static final ParseField QUERIES_FIELD = new ParseField("queries");
    private List<QueryBuilder> queryBuilders;

    private static final ConstructingObjectParser<FirstQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, false,
        args -> {
            FirstQueryBuilder cQueryBuilder = new FirstQueryBuilder((List<QueryBuilder>) args[0]);
            if (args[1] != null) cQueryBuilder.boost((Float) args[1]);
            if (args[2] != null) cQueryBuilder.queryName((String) args[2]);
            return cQueryBuilder;
        });

    static {
        PARSER.declareObjectArray(constructorArg(), (p,c) -> parseInnerQueryBuilder(p), QUERIES_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), AbstractQueryBuilder.BOOST_FIELD);
        PARSER.declareString(optionalConstructorArg(), AbstractQueryBuilder.NAME_FIELD);
    }

    public static FirstQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Create a new {@link FirstQueryBuilder}
     *
     * @param queryBuilders a list of queries
     */
    public FirstQueryBuilder(List<QueryBuilder> queryBuilders) {
        if (queryBuilders == null || queryBuilders.size() == 0) {
            throw new IllegalArgumentException("[queries] cannot be null or empty!");
        }
        if (queryBuilders.size() > maxClauseCount) {
            throw new IllegalArgumentException("Too many query clauses! Should be less or equal [" +  maxClauseCount + "]!");
        }
        this.queryBuilders = queryBuilders;
    }

    /**
     * Set the maximum number of clauses permitted for FirstQuery.
     */
    public static void setMaxClauseCount(int maxClauseCount) {
        if (maxClauseCount < 1) {
            throw new IllegalArgumentException("maxClauseCount must be >= 1");
        }
        FirstQueryBuilder.maxClauseCount = maxClauseCount;
    }

    public List<QueryBuilder> getQueryBuilders() {
        return queryBuilders;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(QUERIES_FIELD.getPreferredName());
        for (QueryBuilder clause : queryBuilders) {
            clause.toXContent(builder, params);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public FirstQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queryBuilders = readQueries(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, queryBuilders);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query[] queries = new Query[queryBuilders.size()];
        for (int i = 0; i < queryBuilders.size(); i++) {
            queries[i] = queryBuilders.get(i).toQuery(context);
        }
        return new FirstQuery(queries);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queryBuilders);
    }

    @Override
    protected boolean doEquals(FirstQueryBuilder other) {
        return Objects.equals(queryBuilders, other.queryBuilders);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        boolean rewritten = false;
        List<QueryBuilder> rqueryBuilders = new ArrayList<>();
        for (QueryBuilder queryBuilder : queryBuilders) {
            QueryBuilder rqueryBuilder = queryBuilder.rewrite(queryRewriteContext);
            rqueryBuilders.add(rqueryBuilder);
            if (queryBuilder != rqueryBuilder) {
                rewritten = true;
            }
        }
        // early termination when all clauses are returning MatchNoneQueryBuilder
        if(rqueryBuilders.stream().allMatch(b -> b instanceof MatchNoneQueryBuilder)) {
            return new MatchNoneQueryBuilder();
        }
        if (rewritten) {
            FirstQueryBuilder newFirstQueryBuilder = new FirstQueryBuilder(rqueryBuilders);
            newFirstQueryBuilder.boost(boost());
            newFirstQueryBuilder.queryName(queryName());
            return newFirstQueryBuilder;
        };
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        List<QueryBuilder> clauses = new ArrayList<>();
        clauses.addAll(queryBuilders);
        for (QueryBuilder clause : clauses) {
            InnerHitContextBuilder.extractInnerHits(clause, innerHits);
        }
    }

}
