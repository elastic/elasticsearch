/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DetectionRuleTests extends AbstractXContentSerializingTestCase<DetectionRule> {

    public void testBuildWithNeitherScopeNorCondition() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> new DetectionRule.Builder().build());
        assertThat(e.getMessage(), equalTo("Invalid detector rule: at least scope or a condition is required"));
    }

    public void testExtractReferencedLists() {
        DetectionRule rule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "filter1").include("bar", "filter2")).build();

        assertEquals(new HashSet<>(Arrays.asList("filter1", "filter2")), rule.extractReferencedFilters());
    }

    @Override
    protected DetectionRule createTestInstance() {
        DetectionRule.Builder builder = new DetectionRule.Builder();
        boolean hasForceTimeShiftAction = false;
        if (randomBoolean()) {
            EnumSet<RuleAction> actions = EnumSet.noneOf(RuleAction.class);
            int actionsCount = randomIntBetween(1, RuleAction.values().length);
            for (int i = 0; i < actionsCount; ++i) {
                actions.add(randomFrom(RuleAction.values()));
            }
            builder.setActions(actions);
            hasForceTimeShiftAction = actions.contains(RuleAction.FORCE_TIME_SHIFT);
        }

        boolean hasScope = randomBoolean();
        boolean hasConditions = randomBoolean();
        boolean hasParams = randomBoolean() || hasForceTimeShiftAction;

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

        if (hasParams) {
            if (hasForceTimeShiftAction) {
                long timeShiftAmount = randomLong();
                builder.setParams(new RuleParams(new RuleParamsForForceTimeShift(timeShiftAmount)));
            } else {
                builder.setParams(new RuleParams());
            }
        }

        return builder.build();
    }

    @Override
    protected Reader<DetectionRule> instanceReader() {
        return DetectionRule::new;
    }

    @Override
    protected DetectionRule doParseInstance(XContentParser parser) {
        return DetectionRule.STRICT_PARSER.apply(parser, null).build();
    }

    @Override
    protected DetectionRule mutateInstance(DetectionRule instance) {
        List<RuleCondition> conditions = instance.getConditions();
        RuleScope scope = instance.getScope();
        EnumSet<RuleAction> actions = instance.getActions();
        RuleParams params = instance.getParams();

        switch (between(0, 3)) {
            case 0:
                if (actions.size() == RuleAction.values().length) {
                    actions = EnumSet.of(randomFrom(RuleAction.values()));
                } else {
                    actions = EnumSet.allOf(RuleAction.class);
                }
                break;
            case 1:
                conditions = new ArrayList<>(conditions);
                conditions.addAll(createCondition(randomDouble()));
                break;
            case 2:
                scope = new RuleScope.Builder(scope).include("another_field", "another_filter").build();
                break;
            case 3:
                if (params.getForceTimeShift() != null) {
                    params = new RuleParams(new RuleParamsForForceTimeShift(randomLong()));
                } else {
                    params = new RuleParams(new RuleParamsForForceTimeShift(randomLong()));
                    actions.add(RuleAction.FORCE_TIME_SHIFT);
                }
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }

        if (actions.contains(RuleAction.FORCE_TIME_SHIFT) && params.getForceTimeShift() == null) {
            params = new RuleParams(new RuleParamsForForceTimeShift(randomLong()));
        } else if (actions.contains(RuleAction.FORCE_TIME_SHIFT) == false && params.getForceTimeShift() != null) {
            params = new RuleParams();
        }

        return new DetectionRule.Builder(conditions).setActions(actions).setScope(scope).setParams(params).build();
    }

    private static List<RuleCondition> createCondition(double value) {
        return Collections.singletonList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, value));
    }
}
