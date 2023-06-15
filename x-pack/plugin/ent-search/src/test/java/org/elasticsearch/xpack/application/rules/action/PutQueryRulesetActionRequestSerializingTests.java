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

public class PutQueryRulesetActionRequestSerializingTests extends AbstractWireSerializingTestCase<PutQueryRulesetAction.Request> {

    @Override
    protected Writeable.Reader<PutQueryRulesetAction.Request> instanceReader() {
        return PutQueryRulesetAction.Request::new;
    }

    @Override
    protected PutQueryRulesetAction.Request createTestInstance() {
        return new PutQueryRulesetAction.Request(SearchApplicationTestUtils.randomQueryRuleset(), randomBoolean());
    }

    @Override
    protected PutQueryRulesetAction.Request mutateInstance(PutQueryRulesetAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
