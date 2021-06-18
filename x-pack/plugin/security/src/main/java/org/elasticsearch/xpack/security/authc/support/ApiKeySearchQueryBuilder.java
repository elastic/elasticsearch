/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.IOException;
import java.util.Set;

public class ApiKeySearchQueryBuilder {

    private ApiKeySearchQueryBuilder() {}

    public static BoolQueryBuilder build(GetApiKeyRequest getApiKeyRequest, Authentication authentication) {
        BoolQueryBuilder finalQuery;
        if (getApiKeyRequest.getQuery() != null) {
            QueryBuilder processedQuery = doProcess(getApiKeyRequest.getQuery());
            if (false == processedQuery instanceof BoolQueryBuilder) {
                processedQuery = QueryBuilders.boolQuery().must(processedQuery);
            }
            finalQuery = (BoolQueryBuilder) processedQuery;
        } else {
            finalQuery = QueryBuilders.boolQuery();
        }
        finalQuery.filter(QueryBuilders.termQuery("doc_type", "api_key"));
        if (Strings.hasText(getApiKeyRequest.getApiKeyId())) {
            finalQuery.filter(QueryBuilders.idsQuery().addIds(getApiKeyRequest.getApiKeyId()));
        }
        if (Strings.hasText(getApiKeyRequest.getUserName())) {
            finalQuery.filter(QueryBuilders.termQuery("creator.principal", getApiKeyRequest.getUserName()));
        }
        if (Strings.hasText(getApiKeyRequest.getRealmName())) {
            finalQuery.filter(QueryBuilders.termQuery("creator.realm", getApiKeyRequest.getRealmName()));
        }
        if (getApiKeyRequest.ownedByAuthenticatedUser()) {
            finalQuery.filter(QueryBuilders.termQuery("creator.principal", authentication.getUser().principal()));
            finalQuery.filter(QueryBuilders.termQuery("creator.realm", ApiKeyService.getCreatorRealmName(authentication)));
        }
        return new ApiKeyBoolQueryBuilder(finalQuery);
    }

    private static QueryBuilder doProcess(QueryBuilder qb) {
        if (qb instanceof BoolQueryBuilder) {
            final BoolQueryBuilder query = (BoolQueryBuilder) qb;
            final BoolQueryBuilder newQuery = QueryBuilders.boolQuery()
                .minimumShouldMatch(query.minimumShouldMatch())
                .adjustPureNegative(query.adjustPureNegative());
            query.must().stream().map(ApiKeySearchQueryBuilder::doProcess).forEach(newQuery::must);
            query.should().stream().map(ApiKeySearchQueryBuilder::doProcess).forEach(newQuery::should);
            query.mustNot().stream().map(ApiKeySearchQueryBuilder::doProcess).forEach(newQuery::mustNot);
            query.filter().stream().map(ApiKeySearchQueryBuilder::doProcess).forEach(newQuery::filter);
            return newQuery;
        } else if (qb instanceof TermQueryBuilder) {
            final TermQueryBuilder query = (TermQueryBuilder) qb;
            final String translatedFieldName = FieldNameTranslators.translate(query.fieldName());
            return QueryBuilders.termQuery(translatedFieldName, query.value()).caseInsensitive(query.caseInsensitive());
        } else if (qb instanceof TermsQueryBuilder) {
            final TermsQueryBuilder query = (TermsQueryBuilder) qb;
            if (query.termsLookup() != null) {
                throw new IllegalArgumentException("terms query with terms lookup is not supported for search");
            }
            final String translatedFieldName = FieldNameTranslators.translate(query.fieldName());
            return QueryBuilders.termsQuery(translatedFieldName, query.getValues());
        } else if (qb instanceof PrefixQueryBuilder) {
            final PrefixQueryBuilder query = (PrefixQueryBuilder) qb;
            final String translatedFieldName = FieldNameTranslators.translate(query.fieldName());
            return QueryBuilders.prefixQuery(translatedFieldName, query.value()).caseInsensitive(query.caseInsensitive());
        } else if (qb instanceof WildcardQueryBuilder) {
            final WildcardQueryBuilder query = (WildcardQueryBuilder) qb;
            final String translatedFieldName = FieldNameTranslators.translate(query.fieldName());
            return QueryBuilders.wildcardQuery(translatedFieldName, query.value())
                .caseInsensitive(query.caseInsensitive())
                .rewrite(query.rewrite());
        } else if (qb instanceof RangeQueryBuilder) {
            final RangeQueryBuilder query = (RangeQueryBuilder) qb;
            final String translatedFieldName = FieldNameTranslators.translate(query.fieldName());
            if (query.relation() != null) {
                throw new IllegalArgumentException("range query with relation is not supported for search");
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
            throw new IllegalArgumentException("Query type [" + qb.getClass() + "] is not supported for search");
        }
    }

    private static class ApiKeyBoolQueryBuilder extends BoolQueryBuilder {

        ApiKeyBoolQueryBuilder(BoolQueryBuilder boolQueryBuilder) {
            super();
            minimumShouldMatch(boolQueryBuilder.minimumShouldMatch());
            adjustPureNegative(boolQueryBuilder.adjustPureNegative());
            boolQueryBuilder.must().forEach(this::must);
            boolQueryBuilder.should().forEach(this::should);
            boolQueryBuilder.mustNot().forEach(this::mustNot);
            boolQueryBuilder.filter().forEach(this::filter);
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) throws IOException {
            return super.doToQuery(context.withAllowedFieldNames(ApiKeyBoolQueryBuilder::isFieldNameAllowed));
        }

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
            if (queryRewriteContext instanceof SearchExecutionContext) {
                return super.doRewrite(
                    ((SearchExecutionContext) queryRewriteContext).withAllowedFieldNames(ApiKeyBoolQueryBuilder::isFieldNameAllowed)
                );
            } else {
                return super.doRewrite(queryRewriteContext);
            }
        }

        static boolean isFieldNameAllowed(String fieldName) {
            if (Set.of("doc_type", "name", "api_key_invalidated", "creation_time", "expiration_time").contains(fieldName)) {
                return true;
            } else if (fieldName.startsWith("metadata_flattened.") || fieldName.startsWith("creator.")) {
                return true;
            } else {
                return false;
            }
        }
    }

}
