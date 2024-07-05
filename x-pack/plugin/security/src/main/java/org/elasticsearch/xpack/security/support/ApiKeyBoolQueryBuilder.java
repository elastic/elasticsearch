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
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.IOException;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.security.support.FieldNameTranslators.API_KEY_FIELD_NAME_TRANSLATORS;

public final class ApiKeyBoolQueryBuilder extends BoolQueryBuilder {

    private static final Set<String> FIELDS_ALLOWED_TO_QUERY = Set.of("_id", "doc_type", "type");

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
     * @param fieldNameVisitor This {@code Consumer} is invoked with all the (index-level) field names referred to in the passed-in query.
     * @param authentication The user's authentication object. If present, it will be used to filter the results
     *                       to only include API keys owned by the user.
     * @return A specialised query builder for API keys that is safe to run on the security index.
     */
    public static ApiKeyBoolQueryBuilder build(
        QueryBuilder queryBuilder,
        Consumer<String> fieldNameVisitor,
        @Nullable Authentication authentication
    ) {
        final ApiKeyBoolQueryBuilder finalQuery = new ApiKeyBoolQueryBuilder();
        if (queryBuilder != null) {
            QueryBuilder processedQuery = API_KEY_FIELD_NAME_TRANSLATORS.translateQueryBuilderFields(queryBuilder, fieldNameVisitor);
            finalQuery.must(processedQuery);
        }
        finalQuery.filter(QueryBuilders.termQuery("doc_type", "api_key"));
        fieldNameVisitor.accept("doc_type");

        if (authentication != null) {
            if (authentication.isApiKey()) {
                final String apiKeyId = (String) authentication.getAuthenticatingSubject()
                    .getMetadata()
                    .get(AuthenticationField.API_KEY_ID_KEY);
                assert apiKeyId != null : "api key id must be present in the metadata";
                finalQuery.filter(QueryBuilders.idsQuery().addIds(apiKeyId));
            } else {
                finalQuery.filter(QueryBuilders.termQuery("creator.principal", authentication.getEffectiveSubject().getUser().principal()));
                fieldNameVisitor.accept("creator.principal");
                final String[] realms = ApiKeyService.getOwnersRealmNames(authentication);
                final QueryBuilder realmsQuery = ApiKeyService.filterForRealmNames(realms);
                fieldNameVisitor.accept("creator.realm");
                assert realmsQuery != null;
                finalQuery.filter(realmsQuery);
            }
        }
        return finalQuery;
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
        return FIELDS_ALLOWED_TO_QUERY.contains(fieldName) || API_KEY_FIELD_NAME_TRANSLATORS.isIndexFieldSupported(fieldName);
    }
}
