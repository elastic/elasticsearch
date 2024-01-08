/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.lucene.search.Query;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.FilteredQueryRewriteContext;
import org.elasticsearch.index.query.FilteredSearchExecutionContext;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.IOException;
import java.util.Set;

public class ApiKeyBoolQueryBuilder extends BoolQueryBuilder {

    private ApiKeyBoolQueryBuilder(BoolQueryBuilder originalQueryBuilder) {
        originalQueryBuilder.must().forEach(this::must);
        originalQueryBuilder.mustNot().forEach(this::must);
        originalQueryBuilder.should().forEach(this::should);
        originalQueryBuilder.filter().forEach(this::filter);
        queryName(originalQueryBuilder.queryName());
        adjustPureNegative(originalQueryBuilder.adjustPureNegative());
        minimumShouldMatch(originalQueryBuilder.minimumShouldMatch());
        boost(originalQueryBuilder.boost());
    }

    /**
     * Build a bool query that is specialised for query API keys information from the security index.
     * The method processes the given QueryBuilder to ensure:
     *   * Only fields from an allowlist are queried
     *   * Only query types from an allowlist are used
     *   * Field names used in the Query DSL get translated into corresponding names used at the index level.
     *     This helps decouple the user facing and implementation level changes.
     *   * User's security context gets applied when necessary
     *   * Not exposing any other types of documents stored in the same security index
     *
     * @param queryBuilder This represents the query parsed directly from the user input. It is validated
     *                     and transformed (see above).
     * @param authentication The user's authentication object. If present, it will be used to filter the results
     *                       to only include API keys owned by the user.
     * @return A specialised query builder for API keys that is safe to run on the security index.
     */
    public static ApiKeyBoolQueryBuilder build(@Nullable QueryBuilder queryBuilder, @Nullable Authentication authentication) {
        final BoolQueryBuilder apiKeyBoolQueryBuilder = new BoolQueryBuilder();
        if (queryBuilder != null) {
            QueryBuilder processedQuery = doProcess(queryBuilder);
            apiKeyBoolQueryBuilder.must(processedQuery);
        }
        apiKeyBoolQueryBuilder.filter(QueryBuilders.termQuery("doc_type", "api_key"));
        if (authentication != null) {
            if (authentication.isApiKey()) {
                final String apiKeyId = (String) authentication.getAuthenticatingSubject()
                    .getMetadata()
                    .get(AuthenticationField.API_KEY_ID_KEY);
                assert apiKeyId != null : "api key id must be present in the metadata";
                apiKeyBoolQueryBuilder.filter(QueryBuilders.idsQuery().addIds(apiKeyId));
            } else {
                apiKeyBoolQueryBuilder.filter(
                    QueryBuilders.termQuery("username", authentication.getEffectiveSubject().getUser().principal())
                );
                final String[] realms = ApiKeyService.getOwnersRealmNames(authentication);
                final QueryBuilder realmsQuery = filterForRealmNames(realms);
                assert realmsQuery != null;
                apiKeyBoolQueryBuilder.filter(realmsQuery);
            }
        }
        return new ApiKeyBoolQueryBuilder(apiKeyBoolQueryBuilder);
    }

    private static QueryBuilder doProcess(QueryBuilder qb) {
        if (qb instanceof final BoolQueryBuilder query) {
            final BoolQueryBuilder newQuery = QueryBuilders.boolQuery()
                .minimumShouldMatch(query.minimumShouldMatch())
                .adjustPureNegative(query.adjustPureNegative());
            query.must().stream().map(ApiKeyBoolQueryBuilder::doProcess).forEach(newQuery::must);
            query.should().stream().map(ApiKeyBoolQueryBuilder::doProcess).forEach(newQuery::should);
            query.mustNot().stream().map(ApiKeyBoolQueryBuilder::doProcess).forEach(newQuery::mustNot);
            query.filter().stream().map(ApiKeyBoolQueryBuilder::doProcess).forEach(newQuery::filter);
            return newQuery;
        } else if (qb instanceof MatchAllQueryBuilder) {
            return qb;
        } else if (qb instanceof IdsQueryBuilder) {
            return qb;
        } else if (qb instanceof TermQueryBuilder) {
            return qb;
        } else if (qb instanceof ExistsQueryBuilder) {
            return qb;
        } else if (qb instanceof final TermsQueryBuilder query) {
            if (query.termsLookup() != null) {
                throw new IllegalArgumentException("terms query with terms lookup is not supported for API Key query");
            }
            return qb;
        } else if (qb instanceof PrefixQueryBuilder) {
            return qb;
        } else if (qb instanceof WildcardQueryBuilder) {
            return qb;
        } else if (qb instanceof final RangeQueryBuilder query) {
            if (query.relation() != null) {
                throw new IllegalArgumentException("range query with relation is not supported for API Key query");
            }
            return qb;
        } else if (qb instanceof QueryStringQueryBuilder) {
            return qb;
        } else if (qb instanceof SimpleQueryStringBuilder) {
            return qb;
        } else {
            throw new IllegalArgumentException("Query type [" + qb.getName() + "] is not supported for API Key query");
        }
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return super.doToQuery(wrapSearchExecutionContext(context));
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewritten = super.doRewrite(wrapQueryRewriteContext(queryRewriteContext));
        if (rewritten == this) {
            return rewritten;
        } else if (rewritten instanceof BoolQueryBuilder rewrittenBoolQueryBuilder) {
            return new ApiKeyBoolQueryBuilder(rewrittenBoolQueryBuilder);
        } else if (rewritten instanceof MatchNoneQueryBuilder) {
            return rewritten;
        } else {
            throw new IllegalStateException("Unexpected rewritten");
        }
    }

    static SearchExecutionContext wrapSearchExecutionContext(SearchExecutionContext context) {
        return new FilteredSearchExecutionContext(context) {
            @Override
            public Set<String> getMatchingFieldNames(String pattern) {
                return ApiKeyFieldNameTranslators.matchPattern(pattern);
            }

            @Override
            protected MappedFieldType fieldType(String name) {
                if (Set.of("_id", "doc_type").contains(name)) {
                    return super.fieldType(name);
                } else {
                    return super.fieldType(ApiKeyFieldNameTranslators.translate(name));
                }
            }
        };
    }

    static QueryRewriteContext wrapQueryRewriteContext(QueryRewriteContext context) {
        return new FilteredQueryRewriteContext(context) {
            @Override
            public Set<String> getMatchingFieldNames(String pattern) {
                return ApiKeyFieldNameTranslators.matchPattern(pattern);
            }

            @Override
            public Iterable<String> getAllFieldNames() {
                return ApiKeyFieldNameTranslators.matchPattern("*");
            }

            @Override
            protected MappedFieldType fieldType(String name) {
                if (Set.of("_id", "doc_type").contains(name)) {
                    return super.fieldType(name);
                } else {
                    return super.fieldType(ApiKeyFieldNameTranslators.translate(name));
                }
            }
        };
    }

    private static QueryBuilder filterForRealmNames(String[] realmNames) {
        if (realmNames == null || realmNames.length == 0) {
            return null;
        }
        if (realmNames.length == 1) {
            return QueryBuilders.termQuery("realm_name", realmNames[0]);
        } else {
            final BoolQueryBuilder realmsQuery = QueryBuilders.boolQuery();
            for (String realmName : realmNames) {
                realmsQuery.should(QueryBuilders.termQuery("realm_name", realmName));
            }
            realmsQuery.minimumShouldMatch(1);
            return realmsQuery;
        }
    }
}
