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
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.BWCVersions.getAllBWCVersions;

public class TestQueryRulesetActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    TestQueryRulesetAction.Response> {

    @Override
    protected Writeable.Reader<TestQueryRulesetAction.Response> instanceReader() {
        return TestQueryRulesetAction.Response::new;
    }

    @Override
    protected TestQueryRulesetAction.Response mutateInstance(TestQueryRulesetAction.Response instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected TestQueryRulesetAction.Response createTestInstance() {
        int totalMatchedRules = randomIntBetween(0, 10);
        List<TestQueryRulesetAction.MatchedRule> matchedRules = IntStream.range(0, totalMatchedRules)
            .mapToObj(i -> new TestQueryRulesetAction.MatchedRule(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10)))
            .toList();
        return new TestQueryRulesetAction.Response(totalMatchedRules, matchedRules);
    }

    @Override
    protected TestQueryRulesetAction.Response mutateInstanceForVersion(TestQueryRulesetAction.Response instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected List<TransportVersion> bwcVersions() {
        return getAllBWCVersions().stream().filter(v -> v.onOrAfter(TransportVersions.V_8_16_0)).collect(Collectors.toList());
    }
}
