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
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.security.support.FieldNameTranslators.ROLE_FIELD_NAME_TRANSLATORS;

public class RoleBoolQueryBuilder extends BoolQueryBuilder {

    // Field names allowed at the index level
    private static final Set<String> FIELDS_ALLOWED_TO_QUERY = Set.of("_id", "type");

    private RoleBoolQueryBuilder() {}

    /**
     * Build a bool query that is specialised for querying roles from the security index.
     * The method processes the given QueryBuilder to ensure:
     *   * Only fields from an allowlist are queried
     *   * Only query types from an allowlist are used
     *   * Field names used in the Query DSL get translated into corresponding names used at the index level.
     *   * Not exposing any other types of documents stored in the same security index
     *
     * @param queryBuilder This represents the query parsed directly from the user input. It is validated
     *                     and transformed (see above).
     * @param fieldNameVisitor This {@code Consumer} is invoked with all the (index-level) field names referred to in the passed-in query.
     * @return A specialised query builder for roles that is safe to run on the security index.
     */
    public static RoleBoolQueryBuilder build(QueryBuilder queryBuilder, @Nullable Consumer<String> fieldNameVisitor) {
        final RoleBoolQueryBuilder finalQuery = new RoleBoolQueryBuilder();
        if (queryBuilder != null) {
            QueryBuilder processedQuery = ROLE_FIELD_NAME_TRANSLATORS.translateQueryBuilderFields(queryBuilder, fieldNameVisitor);
            finalQuery.must(processedQuery);
        }
        finalQuery.filter(QueryBuilders.termQuery(RoleDescriptor.Fields.TYPE.getPreferredName(), RoleDescriptor.ROLE_TYPE));
        if (fieldNameVisitor != null) {
            fieldNameVisitor.accept(RoleDescriptor.Fields.TYPE.getPreferredName());
        }
        return finalQuery;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        context.setAllowedFields(RoleBoolQueryBuilder::isIndexFieldNameAllowed);
        return super.doToQuery(context);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (queryRewriteContext instanceof SearchExecutionContext) {
            ((SearchExecutionContext) queryRewriteContext).setAllowedFields(RoleBoolQueryBuilder::isIndexFieldNameAllowed);
        }
        return super.doRewrite(queryRewriteContext);
    }

    static boolean isIndexFieldNameAllowed(String fieldName) {
        return FIELDS_ALLOWED_TO_QUERY.contains(fieldName) || ROLE_FIELD_NAME_TRANSLATORS.isIndexFieldSupported(fieldName);
    }
}
