/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.SearchService;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

public class DerivedFieldResolverFactory {

    public static DerivedFieldResolver createResolver(
        QueryShardContext queryShardContext,
        Map<String, Object> derivedFieldsObject,
        List<DerivedField> derivedFields,
        boolean derivedFieldAllowed
    ) {
        boolean derivedFieldsPresent = derivedFieldsPresent(derivedFieldsObject, derivedFields);
        if (derivedFieldsPresent && !derivedFieldAllowed) {
            throw new ElasticsearchException(
                "[derived field] queries cannot be executed when '"
                    + IndexSettings.ALLOW_DERIVED_FIELDS.getKey()
                    + "' or '"
                    + SearchService.CLUSTER_ALLOW_DERIVED_FIELD_SETTING.getKey()
                    + "' is set to false."
            );
        }
        if (derivedFieldsPresent && queryShardContext.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[derived field] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }
        if (derivedFieldAllowed) {
            return new DefaultDerivedFieldResolver(queryShardContext, derivedFieldsObject, derivedFields);
        } else {
            return new NoOpDerivedFieldResolver();
        }
    }

    private static boolean derivedFieldsPresent(Map<String, Object> derivedFieldsObject, List<DerivedField> derivedFields) {
        return (derivedFieldsObject != null && !derivedFieldsObject.isEmpty()) || (derivedFields != null && !derivedFields.isEmpty());
    }
}
