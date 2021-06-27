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

public class ApiKeyBoolQueryBuilder extends BoolQueryBuilder {

    private static final Set<String> ALLOWED_INDEX_FIELD_NAMES =
        Set.of("doc_type", "name", "api_key_invalidated", "creation_time", "expiration_time");

    private ApiKeyBoolQueryBuilder() {}

    public static ApiKeyBoolQueryBuilder build(GetApiKeyRequest getApiKeyRequest, Authentication authentication) {
        final ApiKeyBoolQueryBuilder finalQuery = new ApiKeyBoolQueryBuilder();
        if (getApiKeyRequest.getQuery() != null) {
            QueryBuilder processedQuery = doProcess(getApiKeyRequest.getQuery());
            if (false == processedQuery instanceof BoolQueryBuilder) {
                finalQuery.must(processedQuery);
            } else {
                final BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) processedQuery;
                finalQuery.minimumShouldMatch(boolQueryBuilder.minimumShouldMatch());
                finalQuery.adjustPureNegative(boolQueryBuilder.adjustPureNegative());
                boolQueryBuilder.must().forEach(finalQuery::must);
                boolQueryBuilder.should().forEach(finalQuery::should);
                boolQueryBuilder.mustNot().forEach(finalQuery::mustNot);
                boolQueryBuilder.filter().forEach(finalQuery::filter);
            }
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
        return finalQuery;
    }

    private static QueryBuilder doProcess(QueryBuilder qb) {
        if (qb instanceof BoolQueryBuilder) {
            final BoolQueryBuilder query = (BoolQueryBuilder) qb;
            final BoolQueryBuilder newQuery = QueryBuilders.boolQuery()
                .minimumShouldMatch(query.minimumShouldMatch())
                .adjustPureNegative(query.adjustPureNegative());
            query.must().stream().map(ApiKeyBoolQueryBuilder::doProcess).forEach(newQuery::must);
            query.should().stream().map(ApiKeyBoolQueryBuilder::doProcess).forEach(newQuery::should);
            query.mustNot().stream().map(ApiKeyBoolQueryBuilder::doProcess).forEach(newQuery::mustNot);
            query.filter().stream().map(ApiKeyBoolQueryBuilder::doProcess).forEach(newQuery::filter);
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


    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        context.setAllowedFieldNames(ApiKeyBoolQueryBuilder::isIndexFieldNameAllowed);
        return super.doToQuery(context);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (queryRewriteContext instanceof SearchExecutionContext) {
            ((SearchExecutionContext) queryRewriteContext).setAllowedFieldNames(ApiKeyBoolQueryBuilder::isIndexFieldNameAllowed);
        }
        return super.doRewrite(queryRewriteContext);
    }

    static boolean isIndexFieldNameAllowed(String fieldName) {
        if (ALLOWED_INDEX_FIELD_NAMES.contains(fieldName)) {
            return true;
        } else if (fieldName.startsWith("metadata_flattened.") || fieldName.startsWith("creator.")) {
            return true;
        } else {
            return false;
        }
    }
}
