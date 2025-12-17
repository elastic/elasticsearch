/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;
import org.elasticsearch.xpack.application.rules.QueryRule;
import org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType;
import org.elasticsearch.xpack.application.rules.QueryRuleset;
import org.elasticsearch.xpack.application.rules.QueryRulesetListItem;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.util.List;
import java.util.Map;

public class ListQueryRulesetsActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    ListQueryRulesetsAction.Response> {

    @Override
    protected Writeable.Reader<ListQueryRulesetsAction.Response> instanceReader() {
        return ListQueryRulesetsAction.Response::new;
    }

    private static List<QueryRulesetListItem> randomQueryRulesetList() {
        return randomList(10, () -> {
            QueryRuleset queryRuleset = EnterpriseSearchModuleTestUtils.randomQueryRuleset();
            Map<QueryRuleCriteriaType, Integer> criteriaTypeToCountMap = Map.of(
                randomFrom(QueryRuleCriteriaType.values()),
                randomIntBetween(1, 10)
            );
            Map<QueryRule.QueryRuleType, Integer> ruleTypeToCountMap = Map.of(
                randomFrom(QueryRule.QueryRuleType.values()),
                randomIntBetween(1, 10)
            );
            return new QueryRulesetListItem(queryRuleset.id(), queryRuleset.rules().size(), criteriaTypeToCountMap, ruleTypeToCountMap);
        });
    }

    @Override
    protected ListQueryRulesetsAction.Response mutateInstance(ListQueryRulesetsAction.Response instance) {
        QueryPage<QueryRulesetListItem> originalQueryPage = instance.queryPage();
        QueryPage<QueryRulesetListItem> mutatedQueryPage = randomValueOtherThan(
            originalQueryPage,
            () -> new QueryPage<>(randomQueryRulesetList(), randomLongBetween(0, 1000), ListQueryRulesetsAction.Response.RESULT_FIELD)
        );
        return new ListQueryRulesetsAction.Response(mutatedQueryPage.results(), mutatedQueryPage.count());
    }

    @Override
    protected ListQueryRulesetsAction.Response createTestInstance() {
        return new ListQueryRulesetsAction.Response(randomQueryRulesetList(), randomLongBetween(0, 1000));
    }

    @Override
    protected ListQueryRulesetsAction.Response mutateInstanceForVersion(
        ListQueryRulesetsAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }
}
