/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;

/**
 * A Query that matches documents matching boolean combinations of other queries.
 */
public class BoolQueryBuilder extends AbstractQueryBuilder<BoolQueryBuilder> {
    public static final String NAME = "bool";

    public static final boolean ADJUST_PURE_NEGATIVE_DEFAULT = true;

    private static final ParseField MUST_NOT = new ParseField("must_not").withDeprecation("mustNot");
    private static final ParseField FILTER = new ParseField("filter");
    private static final ParseField SHOULD = new ParseField("should");
    private static final ParseField MUST = new ParseField("must");
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
    public BoolQueryBuilder() {}

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
        return (mustClauses.isEmpty() && shouldClauses.isEmpty() && mustNotClauses.isEmpty() && filterClauses.isEmpty()) == false;
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
        if (adjustPureNegative != ADJUST_PURE_NEGATIVE_DEFAULT) {
            builder.field(ADJUST_PURE_NEGATIVE.getPreferredName(), adjustPureNegative);
        }
        if (minimumShouldMatch != null) {
            builder.field(MINIMUM_SHOULD_MATCH.getPreferredName(), minimumShouldMatch);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    private static void doXArrayContent(ParseField field, List<QueryBuilder> clauses, XContentBuilder builder, Params params)
        throws IOException {
        if (clauses.isEmpty()) {
            return;
        }
        builder.startArray(field.getPreferredName());
        for (QueryBuilder clause : clauses) {
            clause.toXContent(builder, params);
        }
        builder.endArray();
    }

    private static final ObjectParser<BoolQueryBuilder, Void> PARSER = new ObjectParser<>("bool", BoolQueryBuilder::new);
    static {
        PARSER.declareObjectArrayOrNull((builder, clauses) -> clauses.forEach(builder::must), (p, c) -> parseInnerQueryBuilder(p), MUST);
        PARSER.declareObjectArrayOrNull(
            (builder, clauses) -> clauses.forEach(builder::should),
            (p, c) -> parseInnerQueryBuilder(p),
            SHOULD
        );
        PARSER.declareObjectArrayOrNull(
            (builder, clauses) -> clauses.forEach(builder::mustNot),
            (p, c) -> parseInnerQueryBuilder(p),
            MUST_NOT
        );
        PARSER.declareObjectArrayOrNull(
            (builder, clauses) -> clauses.forEach(builder::filter),
            (p, c) -> parseInnerQueryBuilder(p),
            FILTER
        );
        PARSER.declareBoolean(BoolQueryBuilder::adjustPureNegative, ADJUST_PURE_NEGATIVE);
        PARSER.declareField(
            BoolQueryBuilder::minimumShouldMatch,
            (p, c) -> p.textOrNull(),
            MINIMUM_SHOULD_MATCH,
            ObjectParser.ValueType.VALUE
        );
        PARSER.declareString(BoolQueryBuilder::queryName, NAME_FIELD);
        PARSER.declareFloat(BoolQueryBuilder::boost, BOOST_FIELD);
    }

    public static BoolQueryBuilder fromXContent(XContentParser parser) throws IOException, ParsingException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
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

    private static void addBooleanClauses(
        SearchExecutionContext context,
        BooleanQuery.Builder booleanQueryBuilder,
        List<QueryBuilder> clauses,
        Occur occurs
    ) throws IOException {
        for (QueryBuilder query : clauses) {
            Query luceneQuery = query.toQuery(context);
            booleanQueryBuilder.add(new BooleanClause(luceneQuery, occurs));
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(adjustPureNegative, minimumShouldMatch, mustClauses, shouldClauses, mustNotClauses, filterClauses);
    }

    @Override
    protected boolean doEquals(BoolQueryBuilder other) {
        return Objects.equals(adjustPureNegative, other.adjustPureNegative)
            && Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
            && Objects.equals(mustClauses, other.mustClauses)
            && Objects.equals(shouldClauses, other.shouldClauses)
            && Objects.equals(mustNotClauses, other.mustNotClauses)
            && Objects.equals(filterClauses, other.filterClauses);
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
        // early termination when must clause is empty and optional clauses is returning MatchNoneQueryBuilder
        if (mustClauses.size() == 0
            && filterClauses.size() == 0
            && shouldClauses.size() > 0
            && mustNotClauses.size() == 0
            && newBuilder.shouldClauses.stream().allMatch(b -> b instanceof MatchNoneQueryBuilder)) {
            return new MatchNoneQueryBuilder("The \"" + getName() + "\" query was rewritten to a \"match_none\" query.");
        }

        // lets do some early termination and prevent any kind of rewriting if we have a mandatory query that is a MatchNoneQueryBuilder
        if (newBuilder.mustClauses.stream().anyMatch(b -> b instanceof MatchNoneQueryBuilder)
            || newBuilder.filterClauses.stream().anyMatch(b -> b instanceof MatchNoneQueryBuilder)
            || newBuilder.mustNotClauses.stream().anyMatch(b -> b instanceof MatchAllQueryBuilder)) {
            return new MatchNoneQueryBuilder("The \"" + getName() + "\" query was rewritten to a \"match_none\" query.");
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

    private static boolean rewriteClauses(
        QueryRewriteContext queryRewriteContext,
        List<QueryBuilder> builders,
        Consumer<QueryBuilder> consumer
    ) throws IOException {
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

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }

    /**
     * Create a new builder with the same clauses but modification of
     * the builder won't modify the original. Modifications of any
     * of the copy's clauses will modify the original. Don't so that.
     */
    public BoolQueryBuilder shallowCopy() {
        BoolQueryBuilder copy = new BoolQueryBuilder();
        copy.adjustPureNegative = adjustPureNegative;
        copy.minimumShouldMatch = minimumShouldMatch;
        for (QueryBuilder q : mustClauses) {
            copy.must(q);
        }
        for (QueryBuilder q : mustNotClauses) {
            copy.mustNot(q);
        }
        for (QueryBuilder q : filterClauses) {
            copy.filter(q);
        }
        for (QueryBuilder q : shouldClauses) {
            copy.should(q);
        }
        return copy;
    }
}
