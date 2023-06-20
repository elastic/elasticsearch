/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.application.search.SearchApplicationTestUtils;
import org.elasticsearch.xpack.core.action.util.PageParams;

public class ListQueryRulesetsActionRequestSerializingTests extends AbstractWireSerializingTestCase<
    ListQueryRulesetsAction.Request> {

    @Override
    protected Writeable.Reader<ListQueryRulesetsAction.Request> instanceReader() {
        return ListQueryRulesetsAction.Request::new;
    }

    @Override
    protected ListQueryRulesetsAction.Request createTestInstance() {

        PageParams pageParams = SearchApplicationTestUtils.randomPageParams();
        return new ListQueryRulesetsAction.Request(pageParams);
    }

    @Override
    protected ListQueryRulesetsAction.Request mutateInstance(ListQueryRulesetsAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
