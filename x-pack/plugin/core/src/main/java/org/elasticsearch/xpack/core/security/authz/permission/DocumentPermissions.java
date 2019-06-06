/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.authz.support.SecurityQueryTemplateEvaluator;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * Stores document level permissions in the form queries that match all the accessible documents.<br>
 * The document level permissions may be limited by another set of queries in that case the limited
 * queries are used as an additional filter.
 */
public final class DocumentPermissions {
    private final Set<BytesReference> queries;
    private final Set<BytesReference> limitedByQueries;

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
        this.queries = (queries != null) ? Collections.unmodifiableSet(queries) : queries;
        this.limitedByQueries = (scopedByQueries != null) ? Collections.unmodifiableSet(scopedByQueries) : scopedByQueries;
    }

    public Set<BytesReference> getQueries() {
        return queries;
    }

    public Set<BytesReference> getLimitedByQueries() {
        return limitedByQueries;
    }

    /**
     * @return {@code true} if either queries or scoped queries are present for document level security else returns {@code false}
     */
    public boolean hasDocumentLevelPermissions() {
        return queries != null || limitedByQueries != null;
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
     * @param queryShardContextProvider {@link QueryShardContext}
     * @return {@link BooleanQuery} for the filter
     * @throws IOException thrown if there is an exception during parsing
     */
    public BooleanQuery filter(User user, ScriptService scriptService, ShardId shardId,
                                      Function<ShardId, QueryShardContext> queryShardContextProvider) throws IOException {
        if (hasDocumentLevelPermissions()) {
            BooleanQuery.Builder filter;
            if (queries != null && limitedByQueries != null) {
                filter = new BooleanQuery.Builder();
                BooleanQuery.Builder scopedFilter = new BooleanQuery.Builder();
                buildRoleQuery(user, scriptService, shardId, queryShardContextProvider, limitedByQueries, scopedFilter);
                filter.add(scopedFilter.build(), FILTER);

                buildRoleQuery(user, scriptService, shardId, queryShardContextProvider, queries, filter);
            } else if (queries != null) {
                filter = new BooleanQuery.Builder();
                buildRoleQuery(user, scriptService, shardId, queryShardContextProvider, queries, filter);
            } else if (limitedByQueries != null) {
                filter = new BooleanQuery.Builder();
                buildRoleQuery(user, scriptService, shardId, queryShardContextProvider, limitedByQueries, filter);
            } else {
                return null;
            }
            return filter.build();
        }
        return null;
    }

    private static void buildRoleQuery(User user, ScriptService scriptService, ShardId shardId,
                                       Function<ShardId, QueryShardContext> queryShardContextProvider, Set<BytesReference> queries,
                                       BooleanQuery.Builder filter) throws IOException {
        for (BytesReference bytesReference : queries) {
            QueryShardContext queryShardContext = queryShardContextProvider.apply(shardId);
            String templateResult = SecurityQueryTemplateEvaluator.evaluateTemplate(bytesReference.utf8ToString(), scriptService, user);
            try (XContentParser parser = XContentFactory.xContent(templateResult).createParser(queryShardContext.getXContentRegistry(),
                    LoggingDeprecationHandler.INSTANCE, templateResult)) {
                QueryBuilder queryBuilder = queryShardContext.parseInnerQueryBuilder(parser);
                verifyRoleQuery(queryBuilder);
                failIfQueryUsesClient(queryBuilder, queryShardContext);
                Query roleQuery = queryShardContext.toQuery(queryBuilder).query();
                filter.add(roleQuery, SHOULD);
                if (queryShardContext.getMapperService().hasNested()) {
                    NestedHelper nestedHelper = new NestedHelper(queryShardContext.getMapperService());
                    if (nestedHelper.mightMatchNestedDocs(roleQuery)) {
                        roleQuery = new BooleanQuery.Builder().add(roleQuery, FILTER)
                                .add(Queries.newNonNestedFilter(), FILTER).build();
                    }
                    // If access is allowed on root doc then also access is allowed on all nested docs of that root document:
                    BitSetProducer rootDocs = queryShardContext
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
     * Checks whether the role query contains queries we know can't be used as DLS role query.
     */
    static void verifyRoleQuery(QueryBuilder queryBuilder) throws IOException {
        if (queryBuilder instanceof TermsQueryBuilder) {
            TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) queryBuilder;
            if (termsQueryBuilder.termsLookup() != null) {
                throw new IllegalArgumentException("terms query with terms lookup isn't supported as part of a role query");
            }
        } else if (queryBuilder instanceof GeoShapeQueryBuilder) {
            GeoShapeQueryBuilder geoShapeQueryBuilder = (GeoShapeQueryBuilder) queryBuilder;
            if (geoShapeQueryBuilder.shape() == null) {
                throw new IllegalArgumentException("geoshape query referring to indexed shapes isn't support as part of a role query");
            }
        } else if (queryBuilder.getName().equals("percolate")) {
            // actually only if percolate query is referring to an existing document then this is problematic,
            // a normal percolate query does work. However we can't check that here as this query builder is inside
            // another module. So we don't allow the entire percolate query. I don't think users would ever use
            // a percolate query as role query, so this restriction shouldn't prohibit anyone from using dls.
            throw new IllegalArgumentException("percolate query isn't support as part of a role query");
        } else if (queryBuilder.getName().equals("has_child")) {
            throw new IllegalArgumentException("has_child query isn't support as part of a role query");
        } else if (queryBuilder.getName().equals("has_parent")) {
            throw new IllegalArgumentException("has_parent query isn't support as part of a role query");
        } else if (queryBuilder instanceof BoolQueryBuilder) {
            BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
            List<QueryBuilder> clauses = new ArrayList<>();
            clauses.addAll(boolQueryBuilder.filter());
            clauses.addAll(boolQueryBuilder.must());
            clauses.addAll(boolQueryBuilder.mustNot());
            clauses.addAll(boolQueryBuilder.should());
            for (QueryBuilder clause : clauses) {
                verifyRoleQuery(clause);
            }
        } else if (queryBuilder instanceof ConstantScoreQueryBuilder) {
            verifyRoleQuery(((ConstantScoreQueryBuilder) queryBuilder).innerQuery());
        } else if (queryBuilder instanceof FunctionScoreQueryBuilder) {
            verifyRoleQuery(((FunctionScoreQueryBuilder) queryBuilder).query());
        } else if (queryBuilder instanceof BoostingQueryBuilder) {
            verifyRoleQuery(((BoostingQueryBuilder) queryBuilder).negativeQuery());
            verifyRoleQuery(((BoostingQueryBuilder) queryBuilder).positiveQuery());
        }
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
        return new DocumentPermissions(queries, limitedByDocumentPermissions.queries);
    }

    @Override
    public String toString() {
        return "DocumentPermissions [queries=" + queries + ", scopedByQueries=" + limitedByQueries + "]";
    }

}
