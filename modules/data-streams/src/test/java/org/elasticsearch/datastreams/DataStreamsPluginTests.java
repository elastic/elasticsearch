/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmAction;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmActionContext;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;

public class DataStreamsPluginTests extends ESTestCase {

    public void testVerifyActionsWithEmptyList() {
        DataStreamsPlugin.verifyActions(List.of());
    }

    public void testVerifyActionsWithDefaultOutputPatterns() {
        TestDlmStep step = new TestDlmStep("step-1");
        DlmAction action = new TestDlmAction("valid-action", List.of(step));
        DataStreamsPlugin.verifyActions(List.of(action));
    }

    public void testVerifyActionsWithSingleCustomOutputPattern() {
        TestDlmStep step = new TestDlmStep("step-1");
        step.outputIndexNamePatternFunctions = List.of(name -> "cloned-" + name);
        DlmAction action = new TestDlmAction("valid-action", List.of(step));
        DataStreamsPlugin.verifyActions(List.of(action));
    }

    public void testVerifyActionsWithMultipleCustomOutputPatterns() {
        TestDlmStep step = new TestDlmStep("step-1");
        step.outputIndexNamePatternFunctions = List.of(name -> "partial-" + name, name -> "full-" + name);
        DlmAction action = new TestDlmAction("valid-action", List.of(step));
        DataStreamsPlugin.verifyActions(List.of(action));
    }

    public void testVerifyActionsWithMixOfDefaultAndCustomOutputPatterns() {
        TestDlmStep defaultStep = new TestDlmStep("default-step");
        TestDlmStep customStep = new TestDlmStep("custom-step");
        customStep.outputIndexNamePatternFunctions = List.of(name -> "cloned-" + name);
        DlmAction action = new TestDlmAction("valid-action", List.of(defaultStep, customStep));
        DataStreamsPlugin.verifyActions(List.of(action));
    }

    public void testVerifyActionsWithMultipleValidActions() {
        TestDlmStep step1 = new TestDlmStep("step-1");
        step1.outputIndexNamePatternFunctions = List.of(name -> "cloned-" + name);
        DlmAction action1 = new TestDlmAction("action-1", List.of(step1, new TestDlmStep("step-2")));
        DlmAction action2 = new TestDlmAction("action-2", List.of(new TestDlmStep("step-3")));
        DataStreamsPlugin.verifyActions(List.of(action1, action2));
    }

    public void testVerifyActionsThrowsOnActionWithNoSteps() {
        DlmAction action = new TestDlmAction("empty-action", List.of());
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> DataStreamsPlugin.verifyActions(List.of(action)));
        assertThat(e.getMessage(), containsString("DLM action [empty-action] must have at least one step"));
    }

    public void testVerifyActionsThrowsOnStepWithEmptyOutputPatterns() {
        TestDlmStep step = new TestDlmStep("bad-step");
        step.outputIndexNamePatternFunctions = List.of();
        DlmAction action = new TestDlmAction("my-action", List.of(step));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> DataStreamsPlugin.verifyActions(List.of(action)));
        assertThat(e.getMessage(), containsString("DLM step [bad-step] in action [my-action]"));
        assertThat(e.getMessage(), containsString("must have at least one possible output index name pattern"));
    }

    public void testVerifyActionsThrowsOnSecondActionWithNoSteps() {
        DlmAction validAction = new TestDlmAction("valid-action", List.of(new TestDlmStep("step-1")));
        DlmAction emptyAction = new TestDlmAction("broken-action", List.of());
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> DataStreamsPlugin.verifyActions(List.of(validAction, emptyAction))
        );
        assertThat(e.getMessage(), containsString("DLM action [broken-action] must have at least one step"));
    }

    public void testVerifyActionsThrowsOnSecondStepWithEmptyOutputPatterns() {
        TestDlmStep goodStep = new TestDlmStep("good-step");
        TestDlmStep badStep = new TestDlmStep("bad-step");
        badStep.outputIndexNamePatternFunctions = List.of();
        DlmAction action = new TestDlmAction("my-action", List.of(goodStep, badStep));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> DataStreamsPlugin.verifyActions(List.of(action)));
        assertThat(e.getMessage(), containsString("DLM step [bad-step] in action [my-action]"));
    }

    private static class TestDlmStep implements DlmStep {
        private final String name;
        List<Function<String, String>> outputIndexNamePatternFunctions = null;

        TestDlmStep(String name) {
            this.name = name;
        }

        @Override
        public boolean stepCompleted(Index index, ProjectState projectState) {
            return false;
        }

        @Override
        public void execute(DlmStepContext dlmStepContext) {}

        @Override
        public String stepName() {
            return name;
        }

        @Override
        public List<String> possibleOutputIndexNamePatterns(String indexName) {
            if (outputIndexNamePatternFunctions != null) {
                return outputIndexNamePatternFunctions.stream().map(func -> func.apply(indexName)).toList();
            }
            return DlmStep.super.possibleOutputIndexNamePatterns(indexName);
        }
    }

    private static class TestDlmAction implements DlmAction {
        private final String name;
        private final List<DlmStep> steps;

        TestDlmAction(String name, List<DlmStep> steps) {
            this.name = name;
            this.steps = steps;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Function<DataStreamLifecycle, TimeValue> applyAfterTime() {
            return dsl -> null;
        }

        @Override
        public List<DlmStep> steps() {
            return steps;
        }

        @Override
        public boolean canRunOnProject(DlmActionContext dlmActionContext) {
            return true;
        }
    }
}
