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
import org.elasticsearch.xpack.application.rules.QueryRule;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.test.BWCVersions.getAllBWCVersions;
import static org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils.randomQueryRule;

public class GetQueryRuleActionResponseBWCSerializingTests extends AbstractBWCSerializationTestCase<GetQueryRuleAction.Response> {
    public QueryRule queryRule;

    @Override
    protected Writeable.Reader<GetQueryRuleAction.Response> instanceReader() {
        return GetQueryRuleAction.Response::new;
    }

    @Override
    protected GetQueryRuleAction.Response createTestInstance() {
        this.queryRule = randomQueryRule();
        return new GetQueryRuleAction.Response(this.queryRule);
    }

    @Override
    protected GetQueryRuleAction.Response mutateInstance(GetQueryRuleAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetQueryRuleAction.Response doParseInstance(XContentParser parser) throws IOException {
        return GetQueryRuleAction.Response.fromXContent(parser);
    }

    @Override
    protected GetQueryRuleAction.Response mutateInstanceForVersion(GetQueryRuleAction.Response instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected List<TransportVersion> bwcVersions() {
        return getAllBWCVersions().stream().filter(v -> v.onOrAfter(TransportVersions.V_8_15_0)).collect(Collectors.toList());
    }
}
