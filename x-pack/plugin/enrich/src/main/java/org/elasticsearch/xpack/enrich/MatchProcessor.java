/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.script.TemplateScript;

import java.util.List;
import java.util.function.BiConsumer;

public final class MatchProcessor extends AbstractEnrichProcessor {

    MatchProcessor(
        String tag,
        String description,
        Client client,
        String policyName,
        TemplateScript.Factory field,
        TemplateScript.Factory targetField,
        boolean overrideEnabled,
        boolean ignoreMissing,
        String matchField,
        int maxMatches
    ) {
        super(tag, description, client, policyName, field, targetField, ignoreMissing, overrideEnabled, matchField, maxMatches);
    }

    /** used in tests **/
    MatchProcessor(
        String tag,
        String description,
        BiConsumer<SearchRequest, BiConsumer<SearchResponse, Exception>> searchRunner,
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
