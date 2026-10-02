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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.xpack.security.authc.service.UserManagedServiceAccountStore;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.xpack.security.support.FieldNameTranslators.SERVICE_ACCOUNT_FIELD_NAME_TRANSLATORS;

/**
 * Wraps a caller's query so that it can only ever match {@code service_account} documents, and only through the
 * fields the query API allows. The security index holds users, roles, API keys and tokens alongside the accounts, so
 * both halves matter: the {@code doc_type} filter keeps other documents out of the result, and the allowed-fields
 * check keeps a query from probing their fields.
 */
public final class ServiceAccountBoolQueryBuilder extends BoolQueryBuilder {

    // Field names allowed at the index level
    private static final Set<String> FIELDS_ALLOWED_TO_QUERY = Set.of("_id", "doc_type");

    private ServiceAccountBoolQueryBuilder() {}

    /**
     * Builds the query the search actually runs from the one the caller sent. The caller's query has its field names
     * translated and checked against the allowlist, is restricted to the query types the translation supports, and is
     * then combined with the filter that selects service-account documents. A {@code null} query selects every account.
     */
    public static ServiceAccountBoolQueryBuilder build(@Nullable QueryBuilder queryBuilder) {
        final ServiceAccountBoolQueryBuilder finalQuery = new ServiceAccountBoolQueryBuilder();
        if (queryBuilder != null) {
            finalQuery.must(SERVICE_ACCOUNT_FIELD_NAME_TRANSLATORS.translateQueryBuilderFields(queryBuilder, null));
        }
        finalQuery.filter(QueryBuilders.termQuery("doc_type", UserManagedServiceAccountStore.SERVICE_ACCOUNT_DOC_TYPE));
        return finalQuery;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor queryVisitor) throws IOException {
        context.setAllowedFields(ServiceAccountBoolQueryBuilder::isIndexFieldNameAllowed);
        return super.doToQuery(context, queryVisitor);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (queryRewriteContext instanceof SearchExecutionContext searchExecutionContext) {
            searchExecutionContext.setAllowedFields(ServiceAccountBoolQueryBuilder::isIndexFieldNameAllowed);
        }
        return super.doRewrite(queryRewriteContext);
    }

    static boolean isIndexFieldNameAllowed(String fieldName) {
        return FIELDS_ALLOWED_TO_QUERY.contains(fieldName) || SERVICE_ACCOUNT_FIELD_NAME_TRANSLATORS.isIndexFieldSupported(fieldName);
    }
}
