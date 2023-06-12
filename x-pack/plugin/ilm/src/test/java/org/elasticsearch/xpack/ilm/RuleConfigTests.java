/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.mockito.Mockito.mock;

public class RuleConfigTests extends ESTestCase {
    public void testActionRuleConfig() {
        var actionName = randomAlphaOfLength(30);
        assertTrue(
            new IlmHealthIndicatorService.ActionRule(actionName, null).test(
                randomNonNegativeLong(),
                metadataAction(actionName, randomNonNegativeLong())
            )
        );
        assertFalse(
            new IlmHealthIndicatorService.ActionRule(actionName, null).test(
                randomNonNegativeLong(),
                metadataAction(randomAlphaOfLength(30), randomNonNegativeLong())
            )
        );

        var maxTimeOn = TimeValue.parseTimeValue(randomTimeValue(), "");
        var rule = new IlmHealthIndicatorService.ActionRule(actionName, maxTimeOn);
        var now = System.currentTimeMillis();

        assertFalse(rule.test(now, metadataAction(randomAlphaOfLength(30), now)));
        assertFalse(rule.test(now, metadataAction(actionName, now)));
        assertTrue(rule.test(now, metadataAction(actionName, now - maxTimeOn.millis() - randomIntBetween(1000, 100000))));
    }

    public void testStepRuleConfig() {
        var stepName = randomAlphaOfLength(30);
        var maxTimeOn = TimeValue.parseTimeValue(randomTimeValue(), "");
        var maxRetries = randomIntBetween(11, 100);
        var rule = new IlmHealthIndicatorService.StepRule(stepName, maxTimeOn, maxRetries);
        var now = System.currentTimeMillis();

        // rule is not for this step
        assertFalse(rule.test(now, metadataStep(randomAlphaOfLength(30), now, maxRetries + 1)));

        // step still has time to run && can continue retrying
        assertFalse(rule.test(now, metadataStep(stepName, now, maxRetries - randomIntBetween(0, 10))));

        // step still has run longer than expected
        assertTrue(
            rule.test(
                now,
                metadataStep(stepName, now - maxTimeOn.millis() - randomIntBetween(1000, 100000), maxRetries - randomIntBetween(0, 10))
            )
        );

        // step still has time to run but have retried more than expected
        assertTrue(rule.test(now, metadataStep(stepName, now, maxRetries + randomIntBetween(1, 10))));
    }

    public void testRuleChaining() {
        var mockedMd = mock(IndexMetadata.class);
        var someLong = randomLong();

        assertTrue(ruleAlwaysReturn(true).test(someLong, mockedMd));
        assertFalse(ruleAlwaysReturn(false).test(someLong, mockedMd));

        // and
        assertTrue(ruleAlwaysReturn(true).and(ruleAlwaysReturn(true)).test(someLong, mockedMd));
        assertFalse(ruleAlwaysReturn(true).and(ruleAlwaysReturn(false)).test(someLong, mockedMd));
        assertFalse(ruleAlwaysReturn(false).and(ruleAlwaysReturn(false)).test(someLong, mockedMd));
        assertFalse(ruleAlwaysReturn(false).and(ruleAlwaysReturn(true)).test(someLong, mockedMd));

        // or
        assertTrue(ruleAlwaysReturn(true).or(ruleAlwaysReturn(true)).test(someLong, mockedMd));
        assertTrue(ruleAlwaysReturn(true).or(ruleAlwaysReturn(false)).test(someLong, mockedMd));
        assertFalse(ruleAlwaysReturn(false).or(ruleAlwaysReturn(false)).test(someLong, mockedMd));
        assertTrue(ruleAlwaysReturn(false).or(ruleAlwaysReturn(true)).test(someLong, mockedMd));
    }

    public void testGetElapsedTime() {
        var a = randomLongBetween(1000, 2000);
        var b = a - randomLongBetween(0, 500);
        assertEquals(IlmHealthIndicatorService.RuleConfig.getElapsedTime(a, b), TimeValue.timeValueMillis(a - b));
        assertEquals(IlmHealthIndicatorService.RuleConfig.getElapsedTime(a, null), TimeValue.ZERO);
    }

    public void testRuleConfigBuilder() {
        var now = System.currentTimeMillis();
        var lastExecutionTime = System.currentTimeMillis() - TimeValue.timeValueDays(2).millis();
        var maxTimeOnStep = TimeValue.timeValueDays(1);
        var expectedAction = "some-action";
        var rules = IlmHealthIndicatorService.RuleConfig.Builder.actionRule(expectedAction)
            .maxTimeOnAction(TimeValue.timeValueDays(1))
            .stepRules(
                IlmHealthIndicatorService.StepRule.stepRule("step-1", maxTimeOnStep),
                IlmHealthIndicatorService.StepRule.stepRule("step-2", maxTimeOnStep),
                IlmHealthIndicatorService.StepRule.stepRule("step-3", maxTimeOnStep)
            );

        // An unknown action should not satisfy the conditions
        assertFalse(rules.test(now, metadataAlwaysOutOfRule(lastExecutionTime, randomAlphaOfLength(30), "step-1")));
        // The following 3 steps should satisfy the conditions
        assertTrue(rules.test(now, metadataAlwaysOutOfRule(lastExecutionTime, expectedAction, "step-1")));
        assertTrue(rules.test(now, metadataAlwaysOutOfRule(lastExecutionTime, expectedAction, "step-2")));
        assertTrue(rules.test(now, metadataAlwaysOutOfRule(lastExecutionTime, expectedAction, "step-3")));
        // An unknown step should not satisfy the conditions
        assertFalse(rules.test(now, metadataAlwaysOutOfRule(lastExecutionTime, expectedAction, randomAlphaOfLength(30))));

        rules = IlmHealthIndicatorService.RuleConfig.Builder.actionRule(expectedAction)
            .maxTimeOnAction(TimeValue.timeValueDays(1))
            .noStepRules();

        assertTrue(rules.test(now, metadataAlwaysOutOfRule(lastExecutionTime, expectedAction, randomAlphaOfLength(30))));
        // An unknown action and step should satisfy the conditions
        assertFalse(rules.test(now, metadataAlwaysOutOfRule(lastExecutionTime, randomAlphaOfLength(30), randomAlphaOfLength(30))));
    }

    private IlmHealthIndicatorService.RuleConfig ruleAlwaysReturn(boolean shouldReturn) {
        return (now, indexMetadata) -> shouldReturn;
    }

    private IndexMetadata metadataAlwaysOutOfRule(long lastExecutionTime, String action, String step) {
        return baseBuilder().putCustom(
            ILM_CUSTOM_METADATA_KEY,
            Map.of(
                "action",
                action,
                "action_time",
                String.valueOf(lastExecutionTime),
                "step",
                step,
                "step_time",
                String.valueOf(lastExecutionTime),
                "failed_step_retry_count",
                "200"
            )
        ).build();
    }

    private IndexMetadata metadataStep(String currentStep, long stepTime, long stepRetries) {
        return baseBuilder().putCustom(
            ILM_CUSTOM_METADATA_KEY,
            Map.of("step", currentStep, "step_time", String.valueOf(stepTime), "failed_step_retry_count", String.valueOf(stepRetries))
        ).build();
    }

    private IndexMetadata metadataAction(String currentAction, long actionTime) {
        return baseBuilder().putCustom(ILM_CUSTOM_METADATA_KEY, Map.of("action", currentAction, "action_time", String.valueOf(actionTime)))
            .build();
    }

    private IndexMetadata.Builder baseBuilder() {
        return IndexMetadata.builder("some-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));
    }
}
