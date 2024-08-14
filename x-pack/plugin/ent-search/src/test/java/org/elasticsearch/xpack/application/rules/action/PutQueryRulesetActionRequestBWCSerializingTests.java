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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;
import org.elasticsearch.xpack.application.rules.QueryRule;
import org.elasticsearch.xpack.application.rules.QueryRuleCriteria;
import org.elasticsearch.xpack.application.rules.QueryRuleset;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CRITERIA_METADATA_VALUES_TRANSPORT_VERSION;

public class PutQueryRulesetActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<PutQueryRulesetAction.Request> {

    private QueryRuleset queryRulesSet;

    @Override
    protected Writeable.Reader<PutQueryRulesetAction.Request> instanceReader() {
        return PutQueryRulesetAction.Request::new;
    }

    @Override
    protected PutQueryRulesetAction.Request createTestInstance() {
        this.queryRulesSet = EnterpriseSearchModuleTestUtils.randomQueryRuleset();
        return new PutQueryRulesetAction.Request(this.queryRulesSet);
    }

    @Override
    protected PutQueryRulesetAction.Request mutateInstance(PutQueryRulesetAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutQueryRulesetAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PutQueryRulesetAction.Request.fromXContent(this.queryRulesSet.id(), parser);
    }

    @Override
    protected PutQueryRulesetAction.Request mutateInstanceForVersion(PutQueryRulesetAction.Request instance, TransportVersion version) {

        if (version.before(CRITERIA_METADATA_VALUES_TRANSPORT_VERSION)) {
            List<QueryRule> rules = new ArrayList<>();
            for (QueryRule rule : instance.queryRuleset().rules()) {
                rules.add(new QueryRule(rule.id(), rule.type(), mutateQueryRuleCriteria(rule.criteria(), version), rule.actions(), null));
            }
            return new PutQueryRulesetAction.Request(new QueryRuleset(instance.queryRuleset().id(), rules));
        } else if (version.before(TransportVersions.QUERY_RULE_CRUD_API_PUT)) {
            List<QueryRule> rules = new ArrayList<>();
            for (QueryRule rule : instance.queryRuleset().rules()) {
                rules.add(new QueryRule(rule.id(), rule.type(), mutateQueryRuleCriteria(rule.criteria(), version), rule.actions(), null));
            }
            return new PutQueryRulesetAction.Request(new QueryRuleset(instance.queryRuleset().id(), rules));
        } else if (version.before(TransportVersions.QUERY_RULES_ANALYZER_SUPPORT_ADDED)) {
            List<QueryRule> rules = new ArrayList<>();
            for (QueryRule rule : instance.queryRuleset().rules()) {
                rules.add(
                    new QueryRule(
                        rule.id(),
                        rule.type(),
                        mutateQueryRuleCriteria(rule.criteria(), version),
                        rule.actions(),
                        rule.priority()
                    )
                );
            }
            return new PutQueryRulesetAction.Request(new QueryRuleset(instance.queryRuleset().id(), rules));
        }

        // Default to current instance
        return instance;
    }

    private List<QueryRuleCriteria> mutateQueryRuleCriteria(List<QueryRuleCriteria> originalCriteria, TransportVersion version) {
        List<QueryRuleCriteria> newCriteria = new ArrayList<>();
        for (QueryRuleCriteria criteria : originalCriteria) {
            List<Object> values = version.before(CRITERIA_METADATA_VALUES_TRANSPORT_VERSION)
                ? criteria.criteriaValues().subList(0, 1)
                : criteria.criteriaValues();
            Map<String, Object> properties = version.before(TransportVersions.QUERY_RULES_ANALYZER_SUPPORT_ADDED)
                ? null
                : criteria.criteriaProperties();

            newCriteria.add(new QueryRuleCriteria(criteria.criteriaType(), criteria.criteriaMetadata(), values, properties));
        }
        return newCriteria;
    }
}
