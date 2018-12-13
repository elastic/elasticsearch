/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

        if (!hasScope && !hasConditions) {
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
