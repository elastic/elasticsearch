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
import org.elasticsearch.xpack.application.rules.QueryRuleCriteria;
import org.elasticsearch.xpack.application.rules.QueryRuleset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils.randomQueryRuleset;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CRITERIA_METADATA_VALUES_TRANSPORT_VERSION;

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
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetQueryRulesetAction.Response doParseInstance(XContentParser parser) throws IOException {
        return GetQueryRulesetAction.Response.fromXContent(this.queryRuleset.id(), parser);
    }

    @Override
    protected GetQueryRulesetAction.Response mutateInstanceForVersion(GetQueryRulesetAction.Response instance, TransportVersion version) {
        if (version.before(CRITERIA_METADATA_VALUES_TRANSPORT_VERSION)) {
            List<QueryRule> rules = new ArrayList<>();
            for (QueryRule rule : instance.queryRuleset().rules()) {
                List<QueryRuleCriteria> newCriteria = new ArrayList<>();
                for (QueryRuleCriteria criteria : rule.criteria()) {
                    newCriteria.add(
                        new QueryRuleCriteria(criteria.criteriaType(), criteria.criteriaMetadata(), criteria.criteriaValues().subList(0, 1))
                    );
                }
                rules.add(new QueryRule(rule.id(), rule.type(), newCriteria, rule.actions(), null));
            }
            return new GetQueryRulesetAction.Response(new QueryRuleset(instance.queryRuleset().id(), rules));
        } else if (version.before(TransportVersions.V_8_15_0)) {
            List<QueryRule> rules = new ArrayList<>();
            for (QueryRule rule : instance.queryRuleset().rules()) {
                rules.add(new QueryRule(rule.id(), rule.type(), rule.criteria(), rule.actions(), null));
            }
            return new GetQueryRulesetAction.Response(new QueryRuleset(instance.queryRuleset().id(), rules));
        }

        return instance;
    }
}
