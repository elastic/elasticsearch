/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.function.LongSupplier;

public class StepsGenerator {
    private static final Logger LOGGER = ESLoggerFactory.getLogger(StepsGenerator.class);

    private final Map<String, ActionStepsGenerator<?>> actionStepGenerators;

    public StepsGenerator(Map<String, ActionStepsGenerator<?>> actionStepGenerators) {
        this.actionStepGenerators = actionStepGenerators;
    }

    public List<Step> createSteps(LifecyclePolicy policy, Client client, LongSupplier nowSupplier) {
        LifecycleType type = policy.getType();
        List<Step> steps = new ArrayList<>();
        List<Phase> orderedPhases = type.getOrderedPhases(policy.getPhases());
        ListIterator<Phase> phaseIterator = orderedPhases.listIterator(orderedPhases.size());

        // final step so that policy can properly update cluster-state with last
        // action completed
        steps.add(TerminalPolicyStep.INSTANCE);
        Step.StepKey lastStepKey = TerminalPolicyStep.KEY;

        Phase phase = null;
        // add steps for each phase, in reverse
        while (phaseIterator.hasPrevious()) {

            Phase previousPhase = phaseIterator.previous();

            // add `after` step for phase before next
            if (phase != null) {
                // after step should have the name of the previous phase since
                // the index is still in the
                // previous phase until the after condition is reached
                Step.StepKey afterStepKey = new Step.StepKey(previousPhase.getName(), PhaseAfterStep.NAME, PhaseAfterStep.NAME);
                Step phaseAfterStep = new PhaseAfterStep(nowSupplier, phase.getAfter(), afterStepKey, lastStepKey);
                steps.add(phaseAfterStep);
                lastStepKey = phaseAfterStep.getKey();
            }

            phase = previousPhase;
            List<LifecycleAction> orderedActions = type.getOrderedActions(phase);
            ListIterator<LifecycleAction> actionIterator = orderedActions.listIterator(orderedActions.size());
            // add steps for each action, in reverse
            while (actionIterator.hasPrevious()) {
                LifecycleAction action = actionIterator.previous();
                ActionStepsGenerator<?> actionStepsGenerator = actionStepGenerators.get(action.getWriteableName());
                List<Step> actionSteps = actionStepsGenerator.createSteps(action, client, phase.getName(), lastStepKey);
                ListIterator<Step> actionStepsIterator = actionSteps.listIterator(actionSteps.size());
                while (actionStepsIterator.hasPrevious()) {
                    Step step = actionStepsIterator.previous();
                    steps.add(step);
                    lastStepKey = step.getKey();
                }
            }
        }

        if (phase != null) {
            // The very first after step is in a phase before the hot phase so
            // call this "new"
            Step.StepKey afterStepKey = new Step.StepKey("new", PhaseAfterStep.NAME, PhaseAfterStep.NAME);
            Step phaseAfterStep = new PhaseAfterStep(nowSupplier, phase.getAfter(), afterStepKey, lastStepKey);
            steps.add(phaseAfterStep);
            lastStepKey = phaseAfterStep.getKey();
        }

        // init step so that policy is guaranteed to have
        steps.add(new InitializePolicyContextStep(InitializePolicyContextStep.KEY, lastStepKey));

        Collections.reverse(steps);
        LOGGER.debug("STEP COUNT: " + steps.size());
        for (Step step : steps) {
            LOGGER.debug(step.getKey() + " -> " + step.getNextStepKey());
        }

        return steps;
    }

    public static abstract class ActionStepsGenerator<A extends LifecycleAction> {

        protected abstract List<Step> doCreateSteps(A action, Client client, String phaseName, StepKey nextStepKey);

        @SuppressWarnings("unchecked")
        public final List<Step> createSteps(LifecycleAction action, Client client, String phaseName, StepKey nextStepKey) {
            return doCreateSteps((A) action, client, phaseName, nextStepKey);
        }
    }
}
