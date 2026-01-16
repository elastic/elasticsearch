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
import org.elasticsearch.xpack.application.rules.QueryRuleset;

import java.io.IOException;

import static org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils.randomQueryRuleset;

public class GetQueryRulesetActionResponseBWCSerializingTests extends AbstractBWCSerializationTestCase<GetQueryRulesetAction.Response> {
    public QueryRuleset queryRuleset;

    @Override
    protected Writeable.Reader<GetQueryRulesetAction.Response> instanceReader() {
        return GetQueryRulesetAction.Response::new;
    }

    @Override
    protected GetQueryRulesetAction.Response createTestInstance() {
        this.queryRuleset = randomQueryRuleset();
        return new GetQueryRulesetAction.Response(this.queryRuleset);
    }

    @Override
    protected GetQueryRulesetAction.Response mutateInstance(GetQueryRulesetAction.Response instance) throws IOException {
        return new GetQueryRulesetAction.Response(randomValueOtherThan(instance.queryRuleset(), () -> randomQueryRuleset()));
    }

    @Override
    protected GetQueryRulesetAction.Response doParseInstance(XContentParser parser) throws IOException {
        return GetQueryRulesetAction.Response.fromXContent(this.queryRuleset.id(), parser);
    }

    @Override
    protected GetQueryRulesetAction.Response mutateInstanceForVersion(GetQueryRulesetAction.Response instance, TransportVersion version) {
        return instance;
    }
}
