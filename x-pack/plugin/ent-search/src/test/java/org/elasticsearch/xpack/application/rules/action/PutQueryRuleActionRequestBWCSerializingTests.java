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
import org.elasticsearch.xpack.application.rules.QueryRule;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.test.BWCVersions.getAllBWCVersions;

public class PutQueryRuleActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<PutQueryRuleAction.Request> {

    private String queryRuleId;

    @Override
    protected Writeable.Reader<PutQueryRuleAction.Request> instanceReader() {
        return PutQueryRuleAction.Request::new;
    }

    @Override
    protected PutQueryRuleAction.Request createTestInstance() {
        String queryRulesetId = randomAlphaOfLengthBetween(5, 10);
        QueryRule queryRule = EnterpriseSearchModuleTestUtils.randomQueryRule();
        this.queryRuleId = queryRule.id();
        return new PutQueryRuleAction.Request(queryRulesetId, queryRule);
    }

    @Override
    protected PutQueryRuleAction.Request mutateInstance(PutQueryRuleAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutQueryRuleAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PutQueryRuleAction.Request.parse(parser, this.queryRuleId);
    }

    @Override
    protected PutQueryRuleAction.Request mutateInstanceForVersion(PutQueryRuleAction.Request instance, TransportVersion version) {
        return new PutQueryRuleAction.Request(instance.queryRulesetId(), instance.queryRule());
    }

    @Override
    protected List<TransportVersion> bwcVersions() {
        return getAllBWCVersions().stream().filter(v -> v.onOrAfter(TransportVersions.V_8_15_0)).collect(Collectors.toList());
    }
}
