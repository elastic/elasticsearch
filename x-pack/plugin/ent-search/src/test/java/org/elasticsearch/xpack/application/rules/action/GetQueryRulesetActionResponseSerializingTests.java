/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.application.search.SearchApplicationTestUtils.randomQueryRuleset;

public class GetQueryRulesetActionResponseSerializingTests extends AbstractWireSerializingTestCase<GetQueryRulesetAction.Response> {

    @Override
    protected Writeable.Reader<GetQueryRulesetAction.Response> instanceReader() {
        return GetQueryRulesetAction.Response::new;
    }

    @Override
    protected GetQueryRulesetAction.Response createTestInstance() {
        return new GetQueryRulesetAction.Response(randomQueryRuleset());
    }

    @Override
    protected GetQueryRulesetAction.Response mutateInstance(GetQueryRulesetAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
