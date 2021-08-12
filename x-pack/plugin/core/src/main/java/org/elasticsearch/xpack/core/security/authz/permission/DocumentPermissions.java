/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;
import org.elasticsearch.xpack.core.security.authz.support.SecurityQueryTemplateEvaluator;
import org.elasticsearch.xpack.core.security.authz.support.SecurityQueryTemplateEvaluator.DlsQueryEvaluationContext;
import org.elasticsearch.xpack.core.security.support.CacheKey;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * Stores document level permissions in the form queries that match all the accessible documents.<br>
 * The document level permissions may be limited by another set of queries in that case the limited
 * queries are used as an additional filter.
 */
public final class DocumentPermissions implements CacheKey {
    // SortedSet because orders are important when they get serialised for request cache key
    private final SortedSet<BytesReference> queries;
    private final SortedSet<BytesReference> limitedByQueries;
    private List<String> evaluatedQueries;
    private List<String> evaluatedLimitedByQueries;


    private static DocumentPermissions ALLOW_ALL = new DocumentPermissions();

    DocumentPermissions() {
        this.queries = null;
        this.limitedByQueries = null;
    }

    DocumentPermissions(Set<BytesReference> queries) {
        this(queries, null);
    }

    DocumentPermissions(Set<BytesReference> queries, Set<BytesReference> scopedByQueries) {
        if (queries == null && scopedByQueries == null) {
            throw new IllegalArgumentException("one of the queries or scoped queries must be provided");
        }
        this.queries = (queries != null) ? new TreeSet<>(queries) : null;
        this.limitedByQueries = (scopedByQueries != null) ? new TreeSet<>(scopedByQueries) : null;
    }

    public Set<BytesReference> getQueries() {
        return queries == null ? null : Set.copyOf(queries);
    }

    public Set<BytesReference> getLimitedByQueries() {
        return limitedByQueries == null ? null : Set.copyOf(limitedByQueries);
    }

    /**
     * @return {@code true} if either queries or scoped queries are present for document level security else returns {@code false}
     */
    public boolean hasDocumentLevelPermissions() {
        return queries != null || limitedByQueries != null;
    }

    public boolean hasStoredScript() throws IOException {
        if (queries != null) {
            for (BytesReference q : queries) {
                if (DLSRoleQueryValidator.hasStoredScript(q, NamedXContentRegistry.EMPTY)) {
                    return true;
                }
            }
        }
        if (limitedByQueries != null) {
            for (BytesReference q : limitedByQueries) {
                if (DLSRoleQueryValidator.hasStoredScript(q, NamedXContentRegistry.EMPTY)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Creates a {@link BooleanQuery} to be used as filter to restrict access to documents.<br>
     * Document permission queries are used to create an boolean query.<br>
     * If the document permissions are limited, then there is an additional filter added restricting access to documents only allowed by the
     * limited queries.
     *
     * @param user authenticated {@link User}
     * @param scriptService {@link ScriptService} for evaluating query templates
     * @param shardId {@link ShardId}
     * @param searchExecutionContextProvider {@link SearchExecutionContext}
     * @return {@link BooleanQuery} for the filter
     * @throws IOException thrown if there is an exception during parsing
     */
    public BooleanQuery filter(User user, ScriptService scriptService, ShardId shardId,
                               Function<ShardId, SearchExecutionContext> searchExecutionContextProvider) throws IOException {
        if (hasDocumentLevelPermissions()) {
            evaluateQueries(SecurityQueryTemplateEvaluator.wrap(user, scriptService));
            BooleanQuery.Builder filter;
            if (evaluatedQueries != null && evaluatedLimitedByQueries != null) {
                filter = new BooleanQuery.Builder();
                BooleanQuery.Builder scopedFilter = new BooleanQuery.Builder();
                buildRoleQuery(shardId, searchExecutionContextProvider, evaluatedLimitedByQueries, scopedFilter);
                filter.add(scopedFilter.build(), FILTER);

                buildRoleQuery(shardId, searchExecutionContextProvider, evaluatedQueries, filter);
            } else if (evaluatedQueries != null) {
                filter = new BooleanQuery.Builder();
                buildRoleQuery(shardId, searchExecutionContextProvider, evaluatedQueries, filter);
            } else if (evaluatedLimitedByQueries != null) {
                filter = new BooleanQuery.Builder();
                buildRoleQuery(shardId, searchExecutionContextProvider, evaluatedLimitedByQueries, filter);
            } else {
                assert false : "one of queries and limited-by queries must be non-null";
                return null;
            }
            return filter.build();
        }
        return null;
    }

    private void evaluateQueries(DlsQueryEvaluationContext context) {
        if (queries != null && evaluatedQueries == null) {
            evaluatedQueries = queries.stream().map(context::evaluate).collect(Collectors.toUnmodifiableList());
        }
        if (limitedByQueries != null && evaluatedLimitedByQueries == null) {
            evaluatedLimitedByQueries = limitedByQueries.stream().map(context::evaluate).collect(Collectors.toUnmodifiableList());
        }
    }

    private static void buildRoleQuery(ShardId shardId,
                                       Function<ShardId, SearchExecutionContext> searchExecutionContextProvider,
                                       List<String> queries,
                                       BooleanQuery.Builder filter) throws IOException {
        for (String query : queries) {
            SearchExecutionContext context = searchExecutionContextProvider.apply(shardId);
            QueryBuilder queryBuilder = DLSRoleQueryValidator.evaluateAndVerifyRoleQuery(query, context.getXContentRegistry());
            if (queryBuilder != null) {
                failIfQueryUsesClient(queryBuilder, context);
                Query roleQuery = context.toQuery(queryBuilder).query();
                filter.add(roleQuery, SHOULD);
                if (context.hasNested()) {
                    NestedHelper nestedHelper = new NestedHelper(context::getObjectMapper, context::isFieldMapped);
                    if (nestedHelper.mightMatchNestedDocs(roleQuery)) {
                        roleQuery = new BooleanQuery.Builder().add(roleQuery, FILTER)
                            .add(Queries.newNonNestedFilter(), FILTER).build();
                    }
                    // If access is allowed on root doc then also access is allowed on all nested docs of that root document:
                    BitSetProducer rootDocs = context
                        .bitsetFilter(Queries.newNonNestedFilter());
                    ToChildBlockJoinQuery includeNestedDocs = new ToChildBlockJoinQuery(roleQuery, rootDocs);
                    filter.add(includeNestedDocs, SHOULD);
                }
            }
        }
        // at least one of the queries should match
        filter.setMinimumNumberShouldMatch(1);
    }

    /**
     * Fall back validation that verifies that queries during rewrite don't use
     * the client to make remote calls. In the case of DLS this can cause a dead
     * lock if DLS is also applied on these remote calls. For example in the
     * case of terms query with lookup, this can cause recursive execution of
     * the DLS query until the get thread pool has been exhausted:
     * https://github.com/elastic/x-plugins/issues/3145
     */
    static void failIfQueryUsesClient(QueryBuilder queryBuilder, QueryRewriteContext original)
            throws IOException {
        QueryRewriteContext copy = new QueryRewriteContext(
                original.getXContentRegistry(), original.getWriteableRegistry(), null, original::nowInMillis);
        Rewriteable.rewrite(queryBuilder, copy);
        if (copy.hasAsyncActions()) {
            throw new IllegalStateException("role queries are not allowed to execute additional requests");
        }
    }

    /**
     * Create {@link DocumentPermissions} for given set of queries
     * @param queries set of queries
     * @return {@link DocumentPermissions}
     */
    public static DocumentPermissions filteredBy(Set<BytesReference> queries) {
        if (queries == null || queries.isEmpty()) {
            throw new IllegalArgumentException("null or empty queries not permitted");
        }
        return new DocumentPermissions(queries);
    }

    /**
     * Create {@link DocumentPermissions} with no restriction. The {@link #getQueries()}
     * will return {@code null} in this case and {@link #hasDocumentLevelPermissions()}
     * will be {@code false}
     * @return {@link DocumentPermissions}
     */
    public static DocumentPermissions allowAll() {
        return ALLOW_ALL;
    }

    /**
     * Create a document permissions, where the permissions for {@code this} are
     * limited by the queries from other document permissions.<br>
     *
     * @param limitedByDocumentPermissions {@link DocumentPermissions} used to limit the document level access
     * @return instance of {@link DocumentPermissions}
     */
    public DocumentPermissions limitDocumentPermissions(
            DocumentPermissions limitedByDocumentPermissions) {
        assert limitedByQueries == null
                && limitedByDocumentPermissions.limitedByQueries == null : "nested scoping for document permissions is not permitted";
        if (queries == null && limitedByDocumentPermissions.queries == null) {
            return DocumentPermissions.allowAll();
        }
        // TODO: should we apply the same logic here as FieldPermissions#limitFieldPermissions,
        //       i.e. treat limited-by as queries if original queries is null?
        return new DocumentPermissions(queries, limitedByDocumentPermissions.queries);
    }

    @Override
    public String toString() {
        return "DocumentPermissions [queries=" + queries + ", scopedByQueries=" + limitedByQueries + "]";
    }

    @Override
    public void buildCacheKey(StreamOutput out, DlsQueryEvaluationContext context) throws IOException {
        assert false == (queries == null && limitedByQueries == null) : "one of queries and limited-by queries must be non-null";
        evaluateQueries(context);
        if (evaluatedQueries != null) {
            out.writeBoolean(true);
            out.writeCollection(evaluatedQueries, StreamOutput::writeString);
        } else {
            out.writeBoolean(false);
        }
        if (evaluatedLimitedByQueries != null) {
            out.writeBoolean(true);
            out.writeCollection(evaluatedLimitedByQueries, StreamOutput::writeString);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DocumentPermissions that = (DocumentPermissions) o;
        return Objects.equals(queries, that.queries) && Objects.equals(limitedByQueries, that.limitedByQueries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queries, limitedByQueries);
    }
}
