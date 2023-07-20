/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DeleteQueryRulesetActionRequestSerializingTests extends AbstractWireSerializingTestCase<DeleteQueryRulesetAction.Request> {

    @Override
    protected Writeable.Reader<DeleteQueryRulesetAction.Request> instanceReader() {
        return DeleteQueryRulesetAction.Request::new;
    }

    @Override
    protected DeleteQueryRulesetAction.Request createTestInstance() {
        return new DeleteQueryRulesetAction.Request(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected DeleteQueryRulesetAction.Request mutateInstance(DeleteQueryRulesetAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
