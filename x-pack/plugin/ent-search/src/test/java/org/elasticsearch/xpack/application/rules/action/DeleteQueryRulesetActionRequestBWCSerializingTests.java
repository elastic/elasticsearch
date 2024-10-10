/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DeleteQueryRulesetActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<DeleteQueryRulesetAction.Request> {

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

    @Override
    protected DeleteQueryRulesetAction.Request doParseInstance(XContentParser parser) throws IOException {
        return DeleteQueryRulesetAction.Request.parse(parser);
    }

    @Override
    protected DeleteQueryRulesetAction.Request mutateInstanceForVersion(
        DeleteQueryRulesetAction.Request instance,
        TransportVersion version
    ) {
        return new DeleteQueryRulesetAction.Request(instance.rulesetId());
    }
}
