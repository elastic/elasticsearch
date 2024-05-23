/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.rules.QueryRule;
import org.elasticsearch.xpack.application.rules.QueryRuleCriteria;
import org.elasticsearch.xpack.application.rules.QueryRuleset;
import org.elasticsearch.xpack.application.search.SearchApplicationTestUtils;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CRITERIA_METADATA_VALUES_TRANSPORT_VERSION;

public class PutQueryRuleActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<PutQueryRuleAction.Request> {

    private String queryRulesetId;
    private QueryRule queryRule;

    @Override
    protected Writeable.Reader<PutQueryRuleAction.Request> instanceReader() {
        return PutQueryRuleAction.Request::new;
    }

    @Override
    protected PutQueryRuleAction.Request createTestInstance() {
        this.queryRulesetId = randomAlphaOfLengthBetween(5, 10);
        this.queryRule = SearchApplicationTestUtils.randomQueryRule();
        return new PutQueryRuleAction.Request(queryRulesetId, queryRule);
    }

    @Override
    protected PutQueryRuleAction.Request mutateInstance(PutQueryRuleAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutQueryRuleAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PutQueryRuleAction.Request.fromXContent(this.queryRulesetId, this.queryRule.id(), parser);
    }

    @Override
    protected PutQueryRuleAction.Request mutateInstanceForVersion(PutQueryRuleAction.Request instance, TransportVersion version) {
        return instance;
    }
}
