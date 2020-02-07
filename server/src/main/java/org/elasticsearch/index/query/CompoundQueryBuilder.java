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
import org.elasticsearch.index.query.compound.CompoundQuery;
import org.elasticsearch.index.query.compound.CompoundQuery.CombineMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Compound query that allows to combine scores from queries in a  more flexible way than bool query
 */
public class CompoundQueryBuilder extends AbstractQueryBuilder<CompoundQueryBuilder> {
    public static final String NAME = "compound";
    public static int maxClauseCount = 1024;

    private static final ParseField QUERIES_FIELD = new ParseField("queries");
    private static final ParseField COMBINE_MODE_FIELD = new ParseField("combine_mode");
    private List<QueryBuilder> queryBuilders;
    private CombineMode combineMode;

    private static final ConstructingObjectParser<CompoundQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, false,
        args -> {
            CompoundQueryBuilder cQueryBuilder = new CompoundQueryBuilder(
                    (List<QueryBuilder>) args[0], CombineMode.fromString((String) args[1]));
            if (args[2] != null) cQueryBuilder.boost((Float) args[2]);
            if (args[3] != null) cQueryBuilder.queryName((String) args[3]);
            return cQueryBuilder;
        });

    static {
        PARSER.declareObjectArray(constructorArg(), (p,c) -> parseInnerQueryBuilder(p), QUERIES_FIELD);
        PARSER.declareString(constructorArg(), COMBINE_MODE_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), AbstractQueryBuilder.BOOST_FIELD);
        PARSER.declareString(optionalConstructorArg(), AbstractQueryBuilder.NAME_FIELD);
    }

    public static CompoundQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Create a new {@link CompoundQueryBuilder}
     *
     * @param queryBuilders a list of queries to combine
     * @param combineMode combination mode
     */
    public CompoundQueryBuilder(List<QueryBuilder> queryBuilders, CombineMode combineMode) {
        if (queryBuilders == null) {
            throw new IllegalArgumentException("[queries] cannot be null.");
        }
        if (queryBuilders.size() > maxClauseCount) {
            throw new IllegalArgumentException("Too many clauses! Should be less or equal [" +  maxClauseCount + "]!");
        }
        if (combineMode == null) {
            throw new IllegalArgumentException("[combine_mode] cannot be null.");
        }
        this.queryBuilders = queryBuilders;
        this.combineMode = combineMode;
    }

    /**
     * Set the maximum number of clauses permitted per CompoundQuery.
     */
    public static void setMaxClauseCount(int maxClauseCount) {
        if (maxClauseCount < 1) {
            throw new IllegalArgumentException("maxClauseCount must be >= 1");
        }
        CompoundQueryBuilder.maxClauseCount = maxClauseCount;
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
        builder.field(COMBINE_MODE_FIELD.getPreferredName(), combineMode.name().toLowerCase(Locale.ROOT));
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public CompoundQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queryBuilders = readQueries(in);
        combineMode = CombineMode.readFromStream(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, queryBuilders);
        combineMode.writeTo(out);
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
        return new CompoundQuery(queries, combineMode);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queryBuilders, combineMode);
    }

    @Override
    protected boolean doEquals(CompoundQueryBuilder other) {
        return Objects.equals(queryBuilders, other.queryBuilders) && Objects.equals(combineMode, other.combineMode) ;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (queryBuilders.size() == 0) {
            return new MatchAllQueryBuilder().boost(boost()).queryName(queryName());
        }
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
            CompoundQueryBuilder newCompoundQueryBuilder = new CompoundQueryBuilder(rqueryBuilders, combineMode);
            newCompoundQueryBuilder.boost(boost());
            newCompoundQueryBuilder.queryName(queryName());
            return newCompoundQueryBuilder;
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
