/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType;
import org.elasticsearch.xpack.application.rules.QueryRuleset;
import org.elasticsearch.xpack.application.rules.QueryRulesetListItem;
import org.elasticsearch.xpack.application.search.SearchApplicationTestUtils;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListQueryRulesetsActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    ListQueryRulesetsAction.Response> {

    @Override
    protected Writeable.Reader<ListQueryRulesetsAction.Response> instanceReader() {
        return ListQueryRulesetsAction.Response::new;
    }

    private static ListQueryRulesetsAction.Response randomQueryRulesetListItem() {
        return new ListQueryRulesetsAction.Response(randomList(10, () -> {
            QueryRuleset queryRuleset = SearchApplicationTestUtils.randomQueryRuleset();
            Map<QueryRuleCriteriaType, Integer> criteriaTypeToCountMap = Map.of(
                randomFrom(QueryRuleCriteriaType.values()),
                randomIntBetween(0, 10)
            );
            return new QueryRulesetListItem(queryRuleset.id(), queryRuleset.rules().size(), criteriaTypeToCountMap);
        }), randomLongBetween(0, 1000));
    }

    @Override
    protected ListQueryRulesetsAction.Response mutateInstance(ListQueryRulesetsAction.Response instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ListQueryRulesetsAction.Response createTestInstance() {
        return randomQueryRulesetListItem();
    }

    @Override
    protected ListQueryRulesetsAction.Response mutateInstanceForVersion(
        ListQueryRulesetsAction.Response instance,
        TransportVersion version
    ) {
        if (version.onOrAfter(QueryRulesetListItem.EXPANDED_RULESET_COUNT_TRANSPORT_VERSION)) {
            return instance;
        } else {
            List<QueryRulesetListItem> updatedResults = new ArrayList<>();
            for (QueryRulesetListItem listItem : instance.queryPage.results()) {
                updatedResults.add(new QueryRulesetListItem(listItem.rulesetId(), listItem.ruleTotalCount(), Map.of()));
            }
            return new ListQueryRulesetsAction.Response(updatedResults, instance.queryPage.count());
        }
    }
}
