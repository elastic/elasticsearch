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

public class GetQueryRulesetActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<GetQueryRulesetAction.Request> {

    @Override
    protected Writeable.Reader<GetQueryRulesetAction.Request> instanceReader() {
        return GetQueryRulesetAction.Request::new;
    }

    @Override
    protected GetQueryRulesetAction.Request createTestInstance() {
        return new GetQueryRulesetAction.Request(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected GetQueryRulesetAction.Request mutateInstance(GetQueryRulesetAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetQueryRulesetAction.Request doParseInstance(XContentParser parser) throws IOException {
        return GetQueryRulesetAction.Request.parse(parser, null);
    }

    @Override
    protected GetQueryRulesetAction.Request mutateInstanceForVersion(GetQueryRulesetAction.Request instance, TransportVersion version) {
        return new GetQueryRulesetAction.Request(instance.rulesetId());
    }
}
