/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.script.TemplateScript;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public final class MatchProcessor extends AbstractEnrichProcessor {

    MatchProcessor(
        String tag,
        String description,
        BiConsumer<SearchRequest, BiConsumer<List<Map<?, ?>>, Exception>> searchRunner,
        String policyName,
        TemplateScript.Factory field,
        TemplateScript.Factory targetField,
        boolean overrideEnabled,
        boolean ignoreMissing,
        String matchField,
        int maxMatches
    ) {
        super(tag, description, searchRunner, policyName, field, targetField, ignoreMissing, overrideEnabled, matchField, maxMatches);
    }

    @Override
    public QueryBuilder getQueryBuilder(Object fieldValue) {
        if (fieldValue instanceof List) {
            return new TermsQueryBuilder(matchField, (List<?>) fieldValue);
        } else {
            return new TermQueryBuilder(matchField, fieldValue);
        }
    }
}
