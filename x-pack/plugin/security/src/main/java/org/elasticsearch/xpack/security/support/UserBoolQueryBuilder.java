/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.xpack.security.support.FieldNameTranslators.USER_FIELD_NAME_TRANSLATORS;

public final class UserBoolQueryBuilder extends BoolQueryBuilder {

    private static final Set<String> FIELDS_ALLOWED_TO_QUERY = Set.of("_id", User.Fields.TYPE.getPreferredName());

    private UserBoolQueryBuilder() {}

    public static UserBoolQueryBuilder build(QueryBuilder queryBuilder) {
        final UserBoolQueryBuilder finalQuery = new UserBoolQueryBuilder();
        if (queryBuilder != null) {
            QueryBuilder processedQuery = USER_FIELD_NAME_TRANSLATORS.translateQueryBuilderFields(queryBuilder, null);
            finalQuery.must(processedQuery);
        }
        finalQuery.filter(QueryBuilders.termQuery(User.Fields.TYPE.getPreferredName(), NativeUsersStore.USER_DOC_TYPE));

        return finalQuery;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        context.setAllowedFields(this::isIndexFieldNameAllowed);
        return super.doToQuery(context);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (queryRewriteContext instanceof SearchExecutionContext) {
            ((SearchExecutionContext) queryRewriteContext).setAllowedFields(this::isIndexFieldNameAllowed);
        }
        return super.doRewrite(queryRewriteContext);
    }

    boolean isIndexFieldNameAllowed(String fieldName) {
        return FIELDS_ALLOWED_TO_QUERY.contains(fieldName) || USER_FIELD_NAME_TRANSLATORS.isIndexFieldSupported(fieldName);
    }
}
