/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.Item;

public class AppliedQueryRules {

    private final List<String> pinnedIds;
    private final List<Item> pinnedDocs;

    public AppliedQueryRules() {
        this(new ArrayList<>(0), new ArrayList<>(0));
    }

    public AppliedQueryRules(List<String> pinnedIds, List<Item> pinnedDocs) {
        this.pinnedIds = pinnedIds;
        this.pinnedDocs = pinnedDocs;
    }

    public List<String> pinnedIds() {
        return pinnedIds;
    }

    public List<Item> pinnedDocs() {
        return pinnedDocs;
    }

    public void applyRule(QueryRule queryRule, Map<String, Object> matchCriteria) {
        if (queryRule.type() == QueryRule.QueryRuleType.PINNED) {
            applyPinnedRule(queryRule, matchCriteria);
        } else {
            throw new UnsupportedOperationException("Unsupported QueryRule type: " + queryRule.type());
        }
    }

    @SuppressWarnings("unchecked")
    public void applyPinnedRule(QueryRule rule, Map<String, Object> matchCriteria) {

        List<String> matchingPinnedIds = new ArrayList<>();
        List<Item> matchingPinnedDocs = new ArrayList<>();

        for (QueryRuleCriteria criterion : rule.criteria()) {
            for (String match : matchCriteria.keySet()) {
                final String matchValue = matchCriteria.get(match).toString();
                if (criterion.criteriaMetadata().equals(match) && criterion.isMatch(matchValue)) {
                    if (rule.actions().containsKey(PinnedQueryBuilder.IDS_FIELD.getPreferredName())) {
                        matchingPinnedIds.addAll((List<String>) rule.actions().get(PinnedQueryBuilder.IDS_FIELD.getPreferredName()));
                    } else if (rule.actions().containsKey(PinnedQueryBuilder.DOCS_FIELD.getPreferredName())) {
                        List<Map<String, String>> docsToPin = (List<Map<String, String>>) rule.actions()
                            .get(PinnedQueryBuilder.DOCS_FIELD.getPreferredName());
                        List<Item> items = docsToPin.stream()
                            .map(map -> new Item(map.get(Item.INDEX_FIELD.getPreferredName()), map.get(Item.ID_FIELD.getPreferredName())))
                            .toList();
                        matchingPinnedDocs.addAll(items);
                    }
                }
            }
        }

        pinnedIds.addAll(matchingPinnedIds);
        pinnedDocs.addAll(matchingPinnedDocs);
    }

}
