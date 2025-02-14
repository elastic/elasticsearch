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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
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
import java.util.stream.Stream;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * Stores document level permissions in the form queries that match all the accessible documents.<br>
 * The document level permissions may be limited by another set of queries in that case the limited
 * queries are used as an additional filter.
 */
public final class DocumentPermissions implements CacheKey {

    @Nullable
    private final List<Set<BytesReference>> listOfQueries;
    @Nullable
    private List<List<String>> listOfEvaluatedQueries;

    private static final DocumentPermissions ALLOW_ALL = new DocumentPermissions();

    private DocumentPermissions() {
        this.listOfQueries = null;
    }

    private DocumentPermissions(Set<BytesReference> queries) {
        assert queries != null && false == queries.isEmpty() : "null or empty queries not permitted";
        this.listOfQueries = List.of(new TreeSet<>(queries));
    }

    private DocumentPermissions(List<Set<BytesReference>> listOfQueries) {
        assert listOfQueries != null && false == listOfQueries.isEmpty() : "null or empty list of queries not permitted";
        assert listOfQueries.stream().allMatch(queries -> queries != null && false == queries.isEmpty())
            : "null or empty queries not permitted";
        // SortedSet because orders are important when they get serialised for request cache key
        this.listOfQueries = listOfQueries.stream()
            .map(queries -> queries instanceof SortedSet<BytesReference> ? queries : new TreeSet<>(queries))
            .toList();
    }

    public List<Set<BytesReference>> getListOfQueries() {
        return listOfQueries;
    }

    public Set<BytesReference> getSingleSetOfQueries() {
        assert listOfQueries != null && listOfQueries.size() == 1 : "the list of queries does not have a single member";
        return listOfQueries.get(0);
    }

    /**
     * @return {@code true} if either queries or scoped queries are present for document level security else returns {@code false}
     */
    public boolean hasDocumentLevelPermissions() {
        return listOfQueries != null;
    }

    public boolean hasStoredScript() throws IOException {
        if (listOfQueries != null) {
            for (Set<BytesReference> queries : listOfQueries) {
                for (BytesReference q : queries) {
                    if (DLSRoleQueryValidator.hasStoredScript(q, NamedXContentRegistry.EMPTY)) {
                        return true;
                    }
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
    public BooleanQuery filter(
        User user,
        ScriptService scriptService,
        ShardId shardId,
        Function<ShardId, SearchExecutionContext> searchExecutionContextProvider
    ) throws IOException {
        if (hasDocumentLevelPermissions()) {
            evaluateQueries(SecurityQueryTemplateEvaluator.wrap(user, scriptService));
            assert listOfEvaluatedQueries != null : "evaluated queries must not be null";
            assert false == listOfEvaluatedQueries.isEmpty() : "evaluated queries must not be empty";

            BooleanQuery.Builder filter = new BooleanQuery.Builder();
            for (int i = listOfEvaluatedQueries.size() - 1; i > 0; i--) {
                final BooleanQuery.Builder scopedFilter = new BooleanQuery.Builder();
                buildRoleQuery(shardId, searchExecutionContextProvider, listOfEvaluatedQueries.get(i), scopedFilter);
                filter.add(scopedFilter.build(), FILTER);
            }
            // TODO: All role queries can be filters
            buildRoleQuery(shardId, searchExecutionContextProvider, listOfEvaluatedQueries.get(0), filter);
            return filter.build();
        }
        return null;
    }

    private void evaluateQueries(DlsQueryEvaluationContext context) {
        if (listOfQueries != null && listOfEvaluatedQueries == null) {
            listOfEvaluatedQueries = listOfQueries.stream().map(queries -> queries.stream().map(context::evaluate).toList()).toList();
        }
    }

    private static void buildRoleQuery(
        ShardId shardId,
        Function<ShardId, SearchExecutionContext> searchExecutionContextProvider,
        List<String> queries,
        BooleanQuery.Builder filter
    ) throws IOException {
        for (String query : queries) {
            SearchExecutionContext context = searchExecutionContextProvider.apply(shardId);
            QueryBuilder queryBuilder = DLSRoleQueryValidator.evaluateAndVerifyRoleQuery(query, context.getParserConfig().registry());
            if (queryBuilder != null) {
                failIfQueryUsesClient(queryBuilder, context);
                Query roleQuery = context.toQuery(queryBuilder).query();
                if (context.nestedLookup() == NestedLookup.EMPTY) {
                    filter.add(roleQuery, SHOULD);
                } else {
                    if (NestedHelper.mightMatchNestedDocs(roleQuery, context)) {
                        roleQuery = new BooleanQuery.Builder().add(roleQuery, FILTER)
                            .add(Queries.newNonNestedFilter(context.indexVersionCreated()), FILTER)
                            .build();
                    }
                    filter.add(roleQuery, SHOULD);
                    // If access is allowed on root doc then also access is allowed on all nested docs of that root document:
                    BitSetProducer rootDocs = context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated()));
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
    static void failIfQueryUsesClient(QueryBuilder queryBuilder, QueryRewriteContext original) throws IOException {
        QueryRewriteContext copy = new QueryRewriteContext(original.getParserConfig(), null, original::nowInMillis);
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
        return new DocumentPermissions(queries);
    }

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
    public DocumentPermissions limitDocumentPermissions(DocumentPermissions limitedByDocumentPermissions) {
        if (hasDocumentLevelPermissions() && limitedByDocumentPermissions.hasDocumentLevelPermissions()) {
            return new DocumentPermissions(
                Stream.concat(getListOfQueries().stream(), limitedByDocumentPermissions.getListOfQueries().stream()).toList()
            );
        } else if (hasDocumentLevelPermissions()) {
            return new DocumentPermissions(getListOfQueries());
        } else if (limitedByDocumentPermissions.hasDocumentLevelPermissions()) {
            return new DocumentPermissions(limitedByDocumentPermissions.getListOfQueries());
        } else {
            return DocumentPermissions.allowAll();
        }
    }

    @Override
    public String toString() {
        return "DocumentPermissions [listOfQueries=" + listOfQueries + "]";
    }

    @Override
    public void buildCacheKey(StreamOutput out, DlsQueryEvaluationContext context) throws IOException {
        assert hasDocumentLevelPermissions() : "document permissions should not contribute to cache key when there is no DLS query";
        evaluateQueries(context);
        out.writeCollection(listOfEvaluatedQueries, StreamOutput::writeStringCollection);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocumentPermissions that = (DocumentPermissions) o;
        return Objects.equals(listOfQueries, that.listOfQueries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(listOfQueries);
    }
}
