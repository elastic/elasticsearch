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

public class StepsFactory {
    private static final Logger LOGGER = ESLoggerFactory.getLogger(StepsFactory.class);

    private final Map<String, ActionStepsCreator<?>> actionStepsCreators;

    public StepsFactory(Map<String, ActionStepsCreator<?>> actionStepsCreators) {
        this.actionStepsCreators = actionStepsCreators;
    }

    /**
     * This method is used to compile this policy into its execution plan built out
     * of {@link Step} instances. The order of the {@link Phase}s and {@link LifecycleAction}s is
     * determined by the {@link LifecycleType} associated with this policy.
     *
     * The order of the policy will have this structure:
     *
     * - initialize policy context step
     * - phase-1 phase-after-step
     * - ... phase-1 action steps
     * - phase-2 phase-after-step
     * - ...
     * - terminal policy step
     *
     * We first initialize the policy's context and ensure that the index has proper settings set.
     * Then we begin each phase's after-step along with all its actions as steps. Finally, we have
     * a terminal step to inform us that this policy's steps are all complete. Each phase's `after`
     * step is associated with the previous phase's phase. For example, the warm phase's `after` is
     * associated with the hot phase so that it is clear that we haven't stepped into the warm phase
     * just yet (until this step is complete).
     *
     * @param client The Elasticsearch Client to use during execution of {@link AsyncActionStep}
     *               and {@link AsyncWaitStep} steps.
     * @param nowSupplier The supplier of the current time for {@link PhaseAfterStep} steps.
     * @return The list of {@link Step} objects in order of their execution.
     */
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
                ActionStepsCreator<?> actionStepsGenerator = actionStepsCreators.get(action.getWriteableName());
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

    @FunctionalInterface
    public interface ActionStepsCreator<A extends LifecycleAction> {

        List<Step> doCreateSteps(A action, Client client, String phaseName, StepKey nextStepKey);

        @SuppressWarnings("unchecked")
        default List<Step> createSteps(LifecycleAction action, Client client, String phaseName, StepKey nextStepKey) {
            return doCreateSteps((A) action, client, phaseName, nextStepKey);
        }
    }
}
