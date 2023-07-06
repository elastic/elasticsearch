/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder;

import java.util.Objects;

public class QueryRuleValidator {

    public static boolean validRule(QueryRule rule) {
        if (Objects.requireNonNull(rule.type()) == QueryRule.QueryRuleType.PINNED) {
            return validPinnedRule(rule);
        }
        return false;
    }

    private static boolean validPinnedRule(QueryRule rule) {

        if (rule.type() != QueryRule.QueryRuleType.PINNED) {
            return false;
        }

        if (rule.actions().containsKey(PinnedQueryBuilder.IDS_FIELD.getPreferredName()) == false
            && rule.actions().containsKey(PinnedQueryBuilder.DOCS_FIELD.getPreferredName()) == false) {
            throw new UnsupportedOperationException("Pinned rules must specify id or docs");
        }

        // Pinned queries may have ids or docs, but not both
        if (rule.actions().containsKey(PinnedQueryBuilder.IDS_FIELD.getPreferredName())
            && rule.actions().containsKey(PinnedQueryBuilder.DOCS_FIELD.getPreferredName())) {
            throw new UnsupportedOperationException("Pinned rules must specify id or docs");
        }

        return true;
    }

}
