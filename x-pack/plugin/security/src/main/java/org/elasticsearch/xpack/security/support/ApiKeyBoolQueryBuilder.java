/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.lucene.search.Query;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.IOException;
import java.util.Set;

public class ApiKeyBoolQueryBuilder extends BoolQueryBuilder {

    // Field names allowed at the index level
    private static final Set<String> ALLOWED_EXACT_INDEX_FIELD_NAMES = Set.of(
        "_id",
        "doc_type",
        "name",
        "api_key_invalidated",
        "creation_time",
        "expiration_time"
    );

    private ApiKeyBoolQueryBuilder() {}

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
    public static ApiKeyBoolQueryBuilder build(QueryBuilder queryBuilder, @Nullable Authentication authentication) {
        final ApiKeyBoolQueryBuilder finalQuery = new ApiKeyBoolQueryBuilder();
        if (queryBuilder != null) {
            QueryBuilder processedQuery = doProcess(queryBuilder);
            finalQuery.must(processedQuery);
        }
        finalQuery.filter(QueryBuilders.termQuery("doc_type", "api_key"));

        if (authentication != null) {
            if (authentication.isApiKey()) {
                final String apiKeyId = (String) authentication.getAuthenticatingSubject()
                    .getMetadata()
                    .get(AuthenticationField.API_KEY_ID_KEY);
                assert apiKeyId != null : "api key id must be present in the metadata";
                finalQuery.filter(QueryBuilders.idsQuery().addIds(apiKeyId));
            } else {
                finalQuery.filter(QueryBuilders.termQuery("creator.principal", authentication.getEffectiveSubject().getUser().principal()));
                final String[] realms = ApiKeyService.getOwnersRealmNames(authentication);
                final QueryBuilder realmsQuery = ApiKeyService.filterForRealmNames(realms);
                assert realmsQuery != null;
                finalQuery.filter(realmsQuery);
            }
        }
        return finalQuery;
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
        } else if (qb instanceof final TermQueryBuilder query) {
            final String translatedFieldName = ApiKeyFieldNameTranslators.translate(query.fieldName());
            return QueryBuilders.termQuery(translatedFieldName, query.value()).caseInsensitive(query.caseInsensitive());
        } else if (qb instanceof final ExistsQueryBuilder query) {
            final String translatedFieldName = ApiKeyFieldNameTranslators.translate(query.fieldName());
            return QueryBuilders.existsQuery(translatedFieldName);
        } else if (qb instanceof final TermsQueryBuilder query) {
            if (query.termsLookup() != null) {
                throw new IllegalArgumentException("terms query with terms lookup is not supported for API Key query");
            }
            final String translatedFieldName = ApiKeyFieldNameTranslators.translate(query.fieldName());
            return QueryBuilders.termsQuery(translatedFieldName, query.getValues());
        } else if (qb instanceof final PrefixQueryBuilder query) {
            final String translatedFieldName = ApiKeyFieldNameTranslators.translate(query.fieldName());
            return QueryBuilders.prefixQuery(translatedFieldName, query.value()).caseInsensitive(query.caseInsensitive());
        } else if (qb instanceof final WildcardQueryBuilder query) {
            final String translatedFieldName = ApiKeyFieldNameTranslators.translate(query.fieldName());
            return QueryBuilders.wildcardQuery(translatedFieldName, query.value())
                .caseInsensitive(query.caseInsensitive())
                .rewrite(query.rewrite());
        } else if (qb instanceof final RangeQueryBuilder query) {
            final String translatedFieldName = ApiKeyFieldNameTranslators.translate(query.fieldName());
            if (query.relation() != null) {
                throw new IllegalArgumentException("range query with relation is not supported for API Key query");
            }
            final RangeQueryBuilder newQuery = QueryBuilders.rangeQuery(translatedFieldName);
            if (query.format() != null) {
                newQuery.format(query.format());
            }
            if (query.timeZone() != null) {
                newQuery.timeZone(query.timeZone());
            }
            if (query.from() != null) {
                newQuery.from(query.from()).includeLower(query.includeLower());
            }
            if (query.to() != null) {
                newQuery.to(query.to()).includeUpper(query.includeUpper());
            }
            return newQuery.boost(query.boost());
        } else {
            throw new IllegalArgumentException("Query type [" + qb.getName() + "] is not supported for API Key query");
        }
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        context.setAllowedFields(ApiKeyBoolQueryBuilder::isIndexFieldNameAllowed);
        return super.doToQuery(context);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (queryRewriteContext instanceof SearchExecutionContext) {
            ((SearchExecutionContext) queryRewriteContext).setAllowedFields(ApiKeyBoolQueryBuilder::isIndexFieldNameAllowed);
        }
        return super.doRewrite(queryRewriteContext);
    }

    static boolean isIndexFieldNameAllowed(String fieldName) {
        return ALLOWED_EXACT_INDEX_FIELD_NAMES.contains(fieldName)
            || fieldName.startsWith("metadata_flattened.")
            || fieldName.startsWith("creator.");
    }

}
