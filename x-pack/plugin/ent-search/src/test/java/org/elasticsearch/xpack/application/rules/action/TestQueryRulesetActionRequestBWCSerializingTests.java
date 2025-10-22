/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.BWCVersions.getAllBWCVersions;

public class TestQueryRulesetActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<TestQueryRulesetAction.Request> {

    private final String RULESET_NAME = "my-ruleset";

    @Override
    protected Writeable.Reader<TestQueryRulesetAction.Request> instanceReader() {
        return TestQueryRulesetAction.Request::new;
    }

    @Override
    protected TestQueryRulesetAction.Request createTestInstance() {
        return new TestQueryRulesetAction.Request(RULESET_NAME, EnterpriseSearchModuleTestUtils.randomMatchCriteria());
    }

    @Override
    protected TestQueryRulesetAction.Request mutateInstance(TestQueryRulesetAction.Request instance) {
        String rulesetId = instance.rulesetId();
        Map<String, Object> matchCriteria = instance.matchCriteria();
        switch (randomIntBetween(0, 1)) {
            case 0 -> rulesetId = randomValueOtherThan(rulesetId, () -> randomAlphaOfLength(10));
            case 1 -> matchCriteria = randomValueOtherThan(matchCriteria, () -> EnterpriseSearchModuleTestUtils.randomMatchCriteria());
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new TestQueryRulesetAction.Request(rulesetId, matchCriteria);
    }

    @Override
    protected TestQueryRulesetAction.Request doParseInstance(XContentParser parser) throws IOException {
        return TestQueryRulesetAction.Request.parse(parser, RULESET_NAME);
    }

    @Override
    protected TestQueryRulesetAction.Request mutateInstanceForVersion(TestQueryRulesetAction.Request instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected List<TransportVersion> bwcVersions() {
        return getAllBWCVersions().stream().filter(v -> v.onOrAfter(TransportVersions.V_8_16_0)).collect(Collectors.toList());
    }
}
