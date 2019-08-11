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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;

/**
 * A Query that matches documents matching boolean combinations of other queries.
 */
public class BoolQueryBuilder extends AbstractQueryBuilder<BoolQueryBuilder> {
    public static final String NAME = "bool";

    public static final boolean ADJUST_PURE_NEGATIVE_DEFAULT = true;

    private static final String MUSTNOT = "mustNot";
    private static final String MUST_NOT = "must_not";
    private static final String FILTER = "filter";
    private static final String SHOULD = "should";
    private static final String MUST = "must";
    private static final ParseField DISABLE_COORD_FIELD = new ParseField("disable_coord")
            .withAllDeprecated("disable_coord has been removed");
    private static final ParseField MINIMUM_SHOULD_MATCH = new ParseField("minimum_should_match");
    private static final ParseField ADJUST_PURE_NEGATIVE = new ParseField("adjust_pure_negative");

    private final List<QueryBuilder> mustClauses = new ArrayList<>();

    private final List<QueryBuilder> mustNotClauses = new ArrayList<>();

    private final List<QueryBuilder> filterClauses = new ArrayList<>();

    private final List<QueryBuilder> shouldClauses = new ArrayList<>();

    private boolean adjustPureNegative = ADJUST_PURE_NEGATIVE_DEFAULT;

    private String minimumShouldMatch;

    /**
     * Build an empty bool query.
     */
    public BoolQueryBuilder() {
    }

    /**
     * Read from a stream.
     */
    public BoolQueryBuilder(StreamInput in) throws IOException {
        super(in);
        mustClauses.addAll(readQueries(in));
        mustNotClauses.addAll(readQueries(in));
        shouldClauses.addAll(readQueries(in));
        filterClauses.addAll(readQueries(in));
        adjustPureNegative = in.readBoolean();
        minimumShouldMatch = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, mustClauses);
        writeQueries(out, mustNotClauses);
        writeQueries(out, shouldClauses);
        writeQueries(out, filterClauses);
        out.writeBoolean(adjustPureNegative);
        out.writeOptionalString(minimumShouldMatch);
    }

    /**
     * Adds a query that <b>must</b> appear in the matching documents and will
     * contribute to scoring. No {@code null} value allowed.
     */
    public BoolQueryBuilder must(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        mustClauses.add(queryBuilder);
        return this;
    }

    /**
     * Gets the queries that <b>must</b> appear in the matching documents.
     */
    public List<QueryBuilder> must() {
        return this.mustClauses;
    }

    /**
     * Adds a query that <b>must</b> appear in the matching documents but will
     * not contribute to scoring. No {@code null} value allowed.
     */
    public BoolQueryBuilder filter(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        filterClauses.add(queryBuilder);
        return this;
    }

    /**
     * Gets the queries that <b>must</b> appear in the matching documents but don't contribute to scoring
     */
    public List<QueryBuilder> filter() {
        return this.filterClauses;
    }

    /**
     * Adds a query that <b>must not</b> appear in the matching documents.
     * No {@code null} value allowed.
     */
    public BoolQueryBuilder mustNot(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        mustNotClauses.add(queryBuilder);
        return this;
    }

    /**
     * Gets the queries that <b>must not</b> appear in the matching documents.
     */
    public List<QueryBuilder> mustNot() {
        return this.mustNotClauses;
    }

    /**
     * Adds a clause that <i>should</i> be matched by the returned documents. For a boolean query with no
     * {@code MUST} clauses one or more <code>SHOULD</code> clauses must match a document
     * for the BooleanQuery to match. No {@code null} value allowed.
     *
     * @see #minimumShouldMatch(int)
     */
    public BoolQueryBuilder should(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        shouldClauses.add(queryBuilder);
        return this;
    }

    /**
     * Gets the list of clauses that <b>should</b> be matched by the returned documents.
     *
     * @see #should(QueryBuilder)
     *  @see #minimumShouldMatch(int)
     */
    public List<QueryBuilder> should() {
        return this.shouldClauses;
    }

    /**
     * @return the string representation of the minimumShouldMatch settings for this query
     */
    public String minimumShouldMatch() {
        return this.minimumShouldMatch;
    }

    /**
     * Sets the minimum should match parameter using the special syntax (for example, supporting percentage).
     * @see BoolQueryBuilder#minimumShouldMatch(int)
     */
    public BoolQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /**
     * Specifies a minimum number of the optional (should) boolean clauses which must be satisfied.
     * <p>
     * By default no optional clauses are necessary for a match
     * (unless there are no required clauses).  If this method is used,
     * then the specified number of clauses is required.
     * <p>
     * Use of this method is totally independent of specifying that
     * any specific clauses are required (or prohibited).  This number will
     * only be compared against the number of matching optional clauses.
     *
     * @param minimumShouldMatch the number of optional clauses that must match
     */
    public BoolQueryBuilder minimumShouldMatch(int minimumShouldMatch) {
        this.minimumShouldMatch = Integer.toString(minimumShouldMatch);
        return this;
    }

    /**
     * Returns <code>true</code> iff this query builder has at least one should, must, must not or filter clause.
     * Otherwise <code>false</code>.
     */
    public boolean hasClauses() {
        return !(mustClauses.isEmpty() && shouldClauses.isEmpty() && mustNotClauses.isEmpty() && filterClauses.isEmpty());
    }

    /**
     * If a boolean query contains only negative ("must not") clauses should the
     * BooleanQuery be enhanced with a {@link MatchAllDocsQuery} in order to act
     * as a pure exclude. The default is <code>true</code>.
     */
    public BoolQueryBuilder adjustPureNegative(boolean adjustPureNegative) {
        this.adjustPureNegative = adjustPureNegative;
        return this;
    }

    /**
     * @return the setting for the adjust_pure_negative setting in this query
     */
    public boolean adjustPureNegative() {
        return this.adjustPureNegative;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        doXArrayContent(MUST, mustClauses, builder, params);
        doXArrayContent(FILTER, filterClauses, builder, params);
        doXArrayContent(MUST_NOT, mustNotClauses, builder, params);
        doXArrayContent(SHOULD, shouldClauses, builder, params);
        builder.field(ADJUST_PURE_NEGATIVE.getPreferredName(), adjustPureNegative);
        if (minimumShouldMatch != null) {
            builder.field(MINIMUM_SHOULD_MATCH.getPreferredName(), minimumShouldMatch);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    private static void doXArrayContent(String field, List<QueryBuilder> clauses, XContentBuilder builder, Params params)
            throws IOException {
        if (clauses.isEmpty()) {
            return;
        }
        builder.startArray(field);
        for (QueryBuilder clause : clauses) {
            clause.toXContent(builder, params);
        }
        builder.endArray();
    }

    public static BoolQueryBuilder fromXContent(XContentParser parser) throws IOException, ParsingException {
        boolean adjustPureNegative = BoolQueryBuilder.ADJUST_PURE_NEGATIVE_DEFAULT;
        float boost = DEFAULT_BOOST;
        String minimumShouldMatch = null;

        final List<QueryBuilder> mustClauses = new ArrayList<>();
        final List<QueryBuilder> mustNotClauses = new ArrayList<>();
        final List<QueryBuilder> shouldClauses = new ArrayList<>();
        final List<QueryBuilder> filterClauses = new ArrayList<>();
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                switch (currentFieldName) {
                case MUST:
                    mustClauses.add(parseInnerQueryBuilder(parser));
                    break;
                case SHOULD:
                    shouldClauses.add(parseInnerQueryBuilder(parser));
                    break;
                case FILTER:
                    filterClauses.add(parseInnerQueryBuilder(parser));
                    break;
                case MUST_NOT:
                case MUSTNOT:
                    mustNotClauses.add(parseInnerQueryBuilder(parser));
                    break;
                default:
                    throw new ParsingException(parser.getTokenLocation(), "[bool] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    switch (currentFieldName) {
                    case MUST:
                        mustClauses.add(parseInnerQueryBuilder(parser));
                        break;
                    case SHOULD:
                        shouldClauses.add(parseInnerQueryBuilder(parser));
                        break;
                    case FILTER:
                        filterClauses.add(parseInnerQueryBuilder(parser));
                        break;
                    case MUST_NOT:
                    case MUSTNOT:
                        mustNotClauses.add(parseInnerQueryBuilder(parser));
                        break;
                    default:
                        throw new ParsingException(parser.getTokenLocation(), "bool query does not support [" + currentFieldName + "]");
                    }
                }
            } else if (token.isValue()) {
                if (DISABLE_COORD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    // ignore
                } else if (MINIMUM_SHOULD_MATCH.match(currentFieldName, parser.getDeprecationHandler())) {
                    minimumShouldMatch = parser.textOrNull();
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (ADJUST_PURE_NEGATIVE.match(currentFieldName, parser.getDeprecationHandler())) {
                    adjustPureNegative = parser.booleanValue();
                } else if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[bool] query does not support [" + currentFieldName + "]");
                }
            }
        }
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        for (QueryBuilder queryBuilder : mustClauses) {
            boolQuery.must(queryBuilder);
        }
        for (QueryBuilder queryBuilder : mustNotClauses) {
            boolQuery.mustNot(queryBuilder);
        }
        for (QueryBuilder queryBuilder : shouldClauses) {
            boolQuery.should(queryBuilder);
        }
        for (QueryBuilder queryBuilder : filterClauses) {
            boolQuery.filter(queryBuilder);
        }
        boolQuery.boost(boost);
        boolQuery.adjustPureNegative(adjustPureNegative);
        boolQuery.minimumShouldMatch(minimumShouldMatch);
        boolQuery.queryName(queryName);
        return boolQuery;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        addBooleanClauses(context, booleanQueryBuilder, mustClauses, BooleanClause.Occur.MUST);
        addBooleanClauses(context, booleanQueryBuilder, mustNotClauses, BooleanClause.Occur.MUST_NOT);
        addBooleanClauses(context, booleanQueryBuilder, shouldClauses, BooleanClause.Occur.SHOULD);
        addBooleanClauses(context, booleanQueryBuilder, filterClauses, BooleanClause.Occur.FILTER);
        BooleanQuery booleanQuery = booleanQueryBuilder.build();
        if (booleanQuery.clauses().isEmpty()) {
            return new MatchAllDocsQuery();
        }

        Query query = Queries.applyMinimumShouldMatch(booleanQuery, minimumShouldMatch);
        return adjustPureNegative ? fixNegativeQueryIfNeeded(query) : query;
    }

    private static void addBooleanClauses(QueryShardContext context, BooleanQuery.Builder booleanQueryBuilder,
                                          List<QueryBuilder> clauses, Occur occurs) throws IOException {
        for (QueryBuilder query : clauses) {
            Query luceneQuery = query.toQuery(context);
            booleanQueryBuilder.add(new BooleanClause(luceneQuery, occurs));
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(adjustPureNegative,
                minimumShouldMatch, mustClauses, shouldClauses, mustNotClauses, filterClauses);
    }

    @Override
    protected boolean doEquals(BoolQueryBuilder other) {
        return Objects.equals(adjustPureNegative, other.adjustPureNegative) &&
                Objects.equals(minimumShouldMatch, other.minimumShouldMatch) &&
                Objects.equals(mustClauses, other.mustClauses) &&
                Objects.equals(shouldClauses, other.shouldClauses) &&
                Objects.equals(mustNotClauses, other.mustNotClauses) &&
                Objects.equals(filterClauses, other.filterClauses);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        BoolQueryBuilder newBuilder = new BoolQueryBuilder();
        boolean changed = false;
        final int clauses = mustClauses.size() + mustNotClauses.size() + filterClauses.size() + shouldClauses.size();
        if (clauses == 0) {
            return new MatchAllQueryBuilder().boost(boost()).queryName(queryName());
        }
        changed |= rewriteClauses(queryRewriteContext, mustClauses, newBuilder::must);
        changed |= rewriteClauses(queryRewriteContext, mustNotClauses, newBuilder::mustNot);
        changed |= rewriteClauses(queryRewriteContext, filterClauses, newBuilder::filter);
        changed |= rewriteClauses(queryRewriteContext, shouldClauses, newBuilder::should);
        // lets do some early termination and prevent any kind of rewriting if we have a mandatory query that is a MatchNoneQueryBuilder
        Optional<QueryBuilder> any = Stream.concat(newBuilder.mustClauses.stream(), newBuilder.filterClauses.stream())
            .filter(b -> b instanceof MatchNoneQueryBuilder).findAny();
        if (any.isPresent()) {
            return any.get();
        }
        if (changed) {
            newBuilder.adjustPureNegative = adjustPureNegative;
            newBuilder.minimumShouldMatch = minimumShouldMatch;
            newBuilder.boost(boost());
            newBuilder.queryName(queryName());
            return newBuilder;
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        List<QueryBuilder> clauses = new ArrayList<>(filter());
        clauses.addAll(must());
        clauses.addAll(should());
        // no need to include must_not (since there will be no hits for it)
        for (QueryBuilder clause : clauses) {
            InnerHitContextBuilder.extractInnerHits(clause, innerHits);
        }
    }

    private static boolean rewriteClauses(QueryRewriteContext queryRewriteContext, List<QueryBuilder> builders,
                                          Consumer<QueryBuilder> consumer) throws IOException {
        boolean changed = false;
        for (QueryBuilder builder : builders) {
            QueryBuilder result = builder.rewrite(queryRewriteContext);
            if (result != builder) {
                changed = true;
            }
            consumer.accept(result);
        }
        return changed;
    }
}
