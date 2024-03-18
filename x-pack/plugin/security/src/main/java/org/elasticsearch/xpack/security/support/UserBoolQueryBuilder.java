/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.LegacyTypeFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.security.support.SecurityIndexFieldNameTranslator.exact;

public class UserBoolQueryBuilder extends BoolQueryBuilder {
    public static final SecurityIndexFieldNameTranslator USER_FIELD_NAME_TRANSLATOR = new SecurityIndexFieldNameTranslator(
        List.of(exact("username"), exact("roles"), exact("full_name"), exact("email"), exact("enabled"))
    );

    private static final List<String> ALLOWED_FIELD_NAMES = List.of(
        RoutingFieldMapper.NAME,
        IgnoredFieldMapper.NAME,
        LegacyTypeFieldMapper.NAME,
        "type"
    );

    private UserBoolQueryBuilder() {}

    public static UserBoolQueryBuilder build(QueryBuilder queryBuilder) {
        UserBoolQueryBuilder userQueryBuilder = new UserBoolQueryBuilder();
        if (queryBuilder != null) {
            QueryBuilder translaterdQueryBuilder = translateToUserQueryBuilder(queryBuilder);
            userQueryBuilder.must(translaterdQueryBuilder);
        }
        userQueryBuilder.filter(QueryBuilders.termQuery("type", "user"));

        return userQueryBuilder;
    }

    private static QueryBuilder translateToUserQueryBuilder(QueryBuilder qb) {
        if (qb instanceof final BoolQueryBuilder query) {
            final BoolQueryBuilder newQuery = QueryBuilders.boolQuery()
                .minimumShouldMatch(query.minimumShouldMatch())
                .adjustPureNegative(query.adjustPureNegative());
            query.must().stream().map(UserBoolQueryBuilder::translateToUserQueryBuilder).forEach(newQuery::must);
            query.should().stream().map(UserBoolQueryBuilder::translateToUserQueryBuilder).forEach(newQuery::should);
            query.mustNot().stream().map(UserBoolQueryBuilder::translateToUserQueryBuilder).forEach(newQuery::mustNot);
            query.filter().stream().map(UserBoolQueryBuilder::translateToUserQueryBuilder).forEach(newQuery::filter);
            return newQuery;
        } else if (qb instanceof MatchAllQueryBuilder) {
            return qb;
        } else if (qb instanceof final TermQueryBuilder query) {
            final String translatedFieldName = USER_FIELD_NAME_TRANSLATOR.translate(query.fieldName());
            return QueryBuilders.termQuery(translatedFieldName, query.value()).caseInsensitive(query.caseInsensitive());
        } else if (qb instanceof final ExistsQueryBuilder query) {
            final String translatedFieldName = USER_FIELD_NAME_TRANSLATOR.translate(query.fieldName());
            return QueryBuilders.existsQuery(translatedFieldName);
        } else if (qb instanceof final TermsQueryBuilder query) {
            if (query.termsLookup() != null) {
                throw new IllegalArgumentException("Terms query with terms lookup is not supported for User query");
            }
            final String translatedFieldName = USER_FIELD_NAME_TRANSLATOR.translate(query.fieldName());
            return QueryBuilders.termsQuery(translatedFieldName, query.getValues());
        } else if (qb instanceof final PrefixQueryBuilder query) {
            final String translatedFieldName = USER_FIELD_NAME_TRANSLATOR.translate(query.fieldName());
            return QueryBuilders.prefixQuery(translatedFieldName, query.value()).caseInsensitive(query.caseInsensitive());
        } else if (qb instanceof final WildcardQueryBuilder query) {
            final String translatedFieldName = USER_FIELD_NAME_TRANSLATOR.translate(query.fieldName());
            return QueryBuilders.wildcardQuery(translatedFieldName, query.value())
                .caseInsensitive(query.caseInsensitive())
                .rewrite(query.rewrite());
        } else {
            throw new IllegalArgumentException("Query type [" + qb.getName() + "] is not supported for User query");
        }
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

    boolean isIndexFieldNameAllowed(String queryFieldName) {
        // Type is needed to filter on user doc type
        return ALLOWED_FIELD_NAMES.contains(queryFieldName) || USER_FIELD_NAME_TRANSLATOR.supportedIndexFieldName(queryFieldName);
    }
}
