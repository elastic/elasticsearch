/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DetectionRuleTests extends AbstractXContentTestCase<DetectionRule> {

    @Override
    protected DetectionRule createTestInstance() {
        DetectionRule.Builder builder = new DetectionRule.Builder();

        if (randomBoolean()) {
            EnumSet<RuleAction> actions = EnumSet.noneOf(RuleAction.class);
            int actionsCount = randomIntBetween(1, RuleAction.values().length);
            for (int i = 0; i < actionsCount; ++i) {
                actions.add(randomFrom(RuleAction.values()));
            }
            builder.setActions(actions);
        }

        boolean hasScope = randomBoolean();
        boolean hasConditions = randomBoolean();

        if (hasScope == false && hasConditions == false) {
            // at least one of the two should be present
            if (randomBoolean()) {
                hasScope = true;
            } else {
                hasConditions = true;
            }
        }

        if (hasScope) {
            Map<String, FilterRef> scope = new HashMap<>();
            int scopeSize = randomIntBetween(1, 3);
            for (int i = 0; i < scopeSize; i++) {
                scope.put(randomAlphaOfLength(20), new FilterRef(randomAlphaOfLength(20), randomFrom(FilterRef.FilterType.values())));
            }
            builder.setScope(new RuleScope(scope));
        }

        if (hasConditions) {
            int size = randomIntBetween(1, 5);
            List<RuleCondition> ruleConditions = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                // no need for random condition (it is already tested)
                ruleConditions.addAll(createCondition(randomDouble()));
            }
            builder.setConditions(ruleConditions);
        }

        return builder.build();
    }

    @Override
    protected DetectionRule doParseInstance(XContentParser parser) {
        return DetectionRule.PARSER.apply(parser, null).build();
    }

    private static List<RuleCondition> createCondition(double value) {
        return Collections.singletonList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, value));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
