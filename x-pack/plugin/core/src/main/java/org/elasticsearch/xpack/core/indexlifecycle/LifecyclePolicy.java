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
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

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

    public boolean isActionSafe(StepKey stepKey) {
        if ("new".equals(stepKey.getPhase())) {
            return true;
        }
        Phase phase = phases.get(stepKey.getPhase());
        if (phase != null) {
            LifecycleAction action = phase.getActions().get(stepKey.getAction());
            if (action != null) {
                return action.isSafeAction();
            } else {
                throw new IllegalArgumentException("Action [" + stepKey.getAction() + "] in phase [" + stepKey.getPhase()
                        + "]  does not exist in policy [" + name + "]");
            }
        } else {
            throw new IllegalArgumentException("Phase [" + stepKey.getPhase() + "]  does not exist in policy [" + name + "]");
        }
    }

    /**
     * Finds the next valid {@link StepKey} on or after the provided
     * {@link StepKey}. If the provided {@link StepKey} is valid in this policy
     * it will be returned. If its not valid the next available {@link StepKey}
     * will be returned.
     */
    public StepKey getNextValidStep(StepKey stepKey) {
        Phase phase = phases.get(stepKey.getPhase());
        if (phase == null) {
            // Phase doesn't exist so find the after step for the previous
            // available phase
            return getAfterStepBeforePhase(stepKey.getPhase());
        } else {
            // Phase exists so check if the action exists
            LifecycleAction action = phase.getActions().get(stepKey.getAction());
            if (action == null) {
                // if action doesn't exist find the first step in the next
                // available action
                return getFirstStepInNextAction(stepKey.getAction(), phase);
            } else {
                // if the action exists check if the step itself exists
                if (action.toStepKeys(phase.getName()).contains(stepKey)) {
                    // stepKey is valid still so return it
                    return stepKey;
                } else {
                    // stepKey no longer exists in the action so we need to move
                    // to the first step in the next action since skipping steps
                    // in an action is not safe
                    return getFirstStepInNextAction(stepKey.getAction(), phase);
                }
            }
        }
    }

    private StepKey getNextAfterStep(String currentPhaseName) {
        String nextPhaseName = type.getNextPhaseName(currentPhaseName, phases);
        if (nextPhaseName == null) {
            // We don't have a next phase after this one so there is no after
            // step to move to. Instead we need to go to the terminal step as
            // there are no more steps we should execute
            return TerminalPolicyStep.KEY;
        } else {
            return new StepKey(currentPhaseName, PhaseAfterStep.NAME, PhaseAfterStep.NAME);
        }
    }

    private StepKey getAfterStepBeforePhase(String currentPhaseName) {
        String nextPhaseName = type.getNextPhaseName(currentPhaseName, phases);
        if (nextPhaseName == null) {
            // We don't have a next phase after this one so the next step is the
            // TerminalPolicyStep
            return TerminalPolicyStep.KEY;
        } else {
            String prevPhaseName = type.getPreviousPhaseName(currentPhaseName, phases);
            if (prevPhaseName == null) {
                // no previous phase available so go to the
                // InitializePolicyContextStep
                return InitializePolicyContextStep.KEY;
            }
            return new StepKey(prevPhaseName, PhaseAfterStep.NAME, PhaseAfterStep.NAME);
        }
    }

    private StepKey getFirstStepInNextAction(String currentActionName, Phase phase) {
        String nextActionName = type.getNextActionName(currentActionName, phase);
        if (nextActionName == null) {
            // The current action is the last in this phase so we need to find
            // the next after step
            return getNextAfterStep(phase.getName());
        } else {
            LifecycleAction nextAction = phase.getActions().get(nextActionName);
            // Return the first stepKey for nextAction
            return nextAction.toStepKeys(phase.getName()).get(0);
        }
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
