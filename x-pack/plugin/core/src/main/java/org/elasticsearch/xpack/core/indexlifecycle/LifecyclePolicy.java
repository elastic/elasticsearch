/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Represents the lifecycle of an index from creation to deletion. A
 * {@link LifecyclePolicy} is made up of a set of {@link Phase}s which it will
 * move through. Soon we will constrain the phases using some kinda of lifecycle
 * type which will allow only particular {@link Phase}s to be defined, will
 * dictate the order in which the {@link Phase}s are executed and will define
 * which {@link LifecycleAction}s are allowed in each phase.
 */
public class LifecyclePolicy extends AbstractDiffable<LifecyclePolicy>
        implements ToXContentObject, Diffable<LifecyclePolicy> {
    private static final Logger logger = ESLoggerFactory.getLogger(LifecyclePolicy.class);

    public static final ParseField PHASES_FIELD = new ParseField("phases");
    public static final ParseField TYPE_FIELD = new ParseField("type");

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<LifecyclePolicy, String> PARSER = new ConstructingObjectParser<>("lifecycle_policy", false,
            (a, name) -> {
                LifecycleType type = (LifecycleType) a[0];
                List<Phase> phases = (List<Phase>) a[1];
                Map<String, Phase> phaseMap = phases.stream().collect(Collectors.toMap(Phase::getName, Function.identity()));
                return new LifecyclePolicy(type, name, phaseMap);
            });
    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.namedObject(LifecycleType.class, p.text(), null),
                TYPE_FIELD, ValueType.STRING);
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> Phase.parse(p, n), v -> {
            throw new IllegalArgumentException("ordered " + PHASES_FIELD.getPreferredName() + " are not supported");
        }, PHASES_FIELD);
    }

    protected final String name;
    protected final LifecycleType type;
    protected final Map<String, Phase> phases;

    /**
     * @param name
     *            the name of this {@link LifecyclePolicy}
     * @param phases
     *            a {@link Map} of {@link Phase}s which make up this
     *            {@link LifecyclePolicy}.
     */
    public LifecyclePolicy(LifecycleType type, String name, Map<String, Phase> phases) {
        if (type == null) {
            this.type = TimeseriesLifecycleType.INSTANCE;
        } else {
            this.type = type;
        }
        this.name = name;
        this.phases = phases;
        this.type.validate(phases.values());
    }

    /**
     * For Serialization
     */
    public LifecyclePolicy(StreamInput in) throws IOException {
        type = in.readNamedWriteable(LifecycleType.class);
        name = in.readString();
        phases = Collections.unmodifiableMap(in.readMap(StreamInput::readString, Phase::new));
    }

    public static LifecyclePolicy parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(type);
        out.writeString(name);
        out.writeMap(phases, StreamOutput::writeString, (o, val) -> val.writeTo(o));
    }

    /**
     * @return the name of this {@link LifecyclePolicy}
     */
    public String getName() {
        return name;
    }

    /**
     * @return the type of this {@link LifecyclePolicy}
     */
    public LifecycleType getType() {
        return type;
    }

    /**
     * @return the {@link Phase}s for this {@link LifecyclePolicy} in the order
     *         in which they will be executed.
     */
    public Map<String, Phase> getPhases() {
        return phases;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD.getPreferredName(), type.getWriteableName());
            builder.startObject(PHASES_FIELD.getPreferredName());
                for (Phase phase : phases.values()) {
                    builder.field(phase.getName(), phase);
                }
            builder.endObject();
        builder.endObject();
        return builder;
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
    public List<Step> toSteps(Client client, LongSupplier nowSupplier) {
        List<Step> steps = new ArrayList<>();
        List<Phase> orderedPhases = type.getOrderedPhases(phases);
        ListIterator<Phase> phaseIterator = orderedPhases.listIterator(orderedPhases.size());

        // final step so that policy can properly update cluster-state with last action completed
        steps.add(TerminalPolicyStep.INSTANCE);
        Step.StepKey lastStepKey = TerminalPolicyStep.KEY;

        Phase phase = null;
        // add steps for each phase, in reverse
        while (phaseIterator.hasPrevious()) {

            Phase previousPhase = phaseIterator.previous();

            // add `after` step for phase before next
            if (phase != null) {
                // after step should have the name of the previous phase since the index is still in the 
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
                List<Step> actionSteps = action.toSteps(client, phase.getName(), lastStepKey);
                ListIterator<Step> actionStepsIterator = actionSteps.listIterator(actionSteps.size());
                while (actionStepsIterator.hasPrevious()) {
                    Step step = actionStepsIterator.previous();
                    steps.add(step);
                    lastStepKey = step.getKey();
                }
            }
        }

        if (phase != null) {
            // The very first after step is in a phase before the hot phase so call this "new"
            Step.StepKey afterStepKey = new Step.StepKey("new", PhaseAfterStep.NAME, PhaseAfterStep.NAME);
            Step phaseAfterStep = new PhaseAfterStep(nowSupplier, phase.getAfter(), afterStepKey, lastStepKey);
            steps.add(phaseAfterStep);
            lastStepKey = phaseAfterStep.getKey();
        }

        // init step so that policy is guaranteed to have
        steps.add(new InitializePolicyContextStep(InitializePolicyContextStep.KEY, lastStepKey));

        Collections.reverse(steps);
        logger.debug("STEP COUNT: " + steps.size());
        for (Step step : steps) {
            logger.debug(step.getKey() + " -> " + step.getNextStepKey());
        }

        return steps;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, phases);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        LifecyclePolicy other = (LifecyclePolicy) obj;
        return Objects.equals(name, other.name) && 
                Objects.equals(phases, other.phases);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
