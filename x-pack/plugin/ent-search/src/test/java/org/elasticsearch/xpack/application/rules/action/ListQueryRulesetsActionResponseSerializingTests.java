/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.application.rules.QueryRuleset;
import org.elasticsearch.xpack.application.rules.QueryRulesetListItem;
import org.elasticsearch.xpack.application.search.SearchApplicationTestUtils;

public class ListQueryRulesetsActionResponseSerializingTests extends AbstractWireSerializingTestCase<ListQueryRulesetsAction.Response> {

    @Override
    protected Writeable.Reader<ListQueryRulesetsAction.Response> instanceReader() {
        return ListQueryRulesetsAction.Response::new;
    }

    private static ListQueryRulesetsAction.Response randomQueryRulesetListItem() {
        return new ListQueryRulesetsAction.Response(randomList(10, () -> {
            QueryRuleset queryRuleset = SearchApplicationTestUtils.randomQueryRuleset();
            return new QueryRulesetListItem(queryRuleset.id(), queryRuleset.rules().size());
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
}
