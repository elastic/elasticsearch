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

public class GetQueryRuleActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<GetQueryRuleAction.Request> {

    @Override
    protected Writeable.Reader<GetQueryRuleAction.Request> instanceReader() {
        return GetQueryRuleAction.Request::new;
    }

    @Override
    protected GetQueryRuleAction.Request createTestInstance() {
        return new GetQueryRuleAction.Request(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected GetQueryRuleAction.Request mutateInstance(GetQueryRuleAction.Request instance) {
        String rulesetId = instance.rulesetId();
        String ruleId = instance.ruleId();
        switch (randomIntBetween(0, 1)) {
            case 0 -> rulesetId = randomValueOtherThan(rulesetId, () -> randomAlphaOfLengthBetween(1, 10));
            case 1 -> ruleId = randomValueOtherThan(ruleId, () -> randomAlphaOfLengthBetween(1, 10));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new GetQueryRuleAction.Request(rulesetId, ruleId);
    }

    @Override
    protected GetQueryRuleAction.Request doParseInstance(XContentParser parser) throws IOException {
        return GetQueryRuleAction.Request.parse(parser, null);
    }

    @Override
    protected GetQueryRuleAction.Request mutateInstanceForVersion(GetQueryRuleAction.Request instance, TransportVersion version) {
        return instance;
    }
}
