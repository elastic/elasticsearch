/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the lifecycle of an index from creation to deletion. A
 * {@link LifecyclePolicy} is made up of a set of {@link Phase}s which it will
 * move through. Policies are constrained by a {@link LifecycleType} which governs which
 * {@link Phase}s and {@link LifecycleAction}s are allowed to be defined and in which order
 * they are executed.
 */
public class LifecyclePolicy implements SimpleDiffable<LifecyclePolicy>, ToXContentObject {
    private static final int MAX_INDEX_NAME_BYTES = 255;

    public static final ParseField PHASES_FIELD = new ParseField("phases");
    private static final ParseField METADATA = new ParseField("_meta");

    private static final StepKey NEW_STEP_KEY = new StepKey("new", PhaseCompleteStep.NAME, PhaseCompleteStep.NAME);

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<LifecyclePolicy, String> PARSER = new ConstructingObjectParser<>(
        "lifecycle_policy",
        false,
        (a, name) -> {
            List<Phase> phases = (List<Phase>) a[0];
            Map<String, Phase> phaseMap = phases.stream().collect(Collectors.toMap(Phase::getName, Function.identity()));
            return new LifecyclePolicy(TimeseriesLifecycleType.INSTANCE, name, phaseMap, (Map<String, Object>) a[1]);
        }
    );
    static {
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> Phase.parse(p, n),
            v -> { throw new IllegalArgumentException("ordered " + PHASES_FIELD.getPreferredName() + " are not supported"); },
            PHASES_FIELD
        );
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), METADATA);
    }

    private final String name;
    private final LifecycleType type;
    private final Map<String, Phase> phases;
    @Nullable
    private final Map<String, Object> metadata;

    /**
     * @param name
     *            the name of this {@link LifecyclePolicy}
     * @param phases
     *            a {@link Map} of {@link Phase}s which make up this
     *            {@link LifecyclePolicy}.
     *
     */
    public LifecyclePolicy(String name, Map<String, Phase> phases) {
        this(TimeseriesLifecycleType.INSTANCE, name, phases, null);
    }

    /**
     * @param name
     *            the name of this {@link LifecyclePolicy}
     * @param phases
     *            a {@link Map} of {@link Phase}s which make up this
     *            {@link LifecyclePolicy}.
     * @param metadata
     *            the custom metadata of this {@link LifecyclePolicy}
     */
    public LifecyclePolicy(String name, Map<String, Phase> phases, @Nullable Map<String, Object> metadata) {
        this(TimeseriesLifecycleType.INSTANCE, name, phases, metadata);
    }

    /**
     * For Serialization
     */
    public LifecyclePolicy(StreamInput in) throws IOException {
        type = in.readNamedWriteable(LifecycleType.class);
        name = in.readString();
        phases = Collections.unmodifiableMap(in.readMap(StreamInput::readString, Phase::new));
        this.metadata = in.readMap();
    }

    /**
     * @param type
     *            the {@link LifecycleType} of the policy
     * @param name
     *            the name of this {@link LifecyclePolicy}
     * @param phases
     *            a {@link Map} of {@link Phase}s which make up this
     *            {@link LifecyclePolicy}.
     * @param metadata
     *            the custom metadata of this {@link LifecyclePolicy}
     */
    public LifecyclePolicy(LifecycleType type, String name, Map<String, Phase> phases, @Nullable Map<String, Object> metadata) {
        this.name = name;
        this.phases = phases;
        this.type = type;
        this.metadata = metadata;
    }

    public void validate() {
        this.type.validate(phases.values());
    }

    public static LifecyclePolicy parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(type);
        out.writeString(name);
        out.writeMap(phases, StreamOutput::writeString, (o, val) -> val.writeTo(o));
        out.writeGenericMap(this.metadata);
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

    /**
     * @return the custom metadata of this {@link LifecyclePolicy}
     */
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(PHASES_FIELD.getPreferredName());
        for (Phase phase : phases.values()) {
            builder.field(phase.getName(), phase);
        }
        builder.endObject();
        if (this.metadata != null) {
            builder.field(METADATA.getPreferredName(), this.metadata);
        }
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
     * @param licenseState The license state to use in actions and steps
     * @return The list of {@link Step} objects in order of their execution.
     */
    public List<Step> toSteps(Client client, XPackLicenseState licenseState) {
        List<Step> steps = new ArrayList<>();
        List<Phase> orderedPhases = type.getOrderedPhases(phases);
        ListIterator<Phase> phaseIterator = orderedPhases.listIterator(orderedPhases.size());

        Step.StepKey lastStepKey = null;

        Phase phase = null;
        // add steps for each phase, in reverse
        while (phaseIterator.hasPrevious()) {

            Phase previousPhase = phaseIterator.previous();

            // add `after` step for phase before next
            if (previousPhase != null) {
                // after step should have the name of the previous phase since the index is still in the
                // previous phase until the after condition is reached
                Step.StepKey afterStepKey = new Step.StepKey(previousPhase.getName(), PhaseCompleteStep.NAME, PhaseCompleteStep.NAME);
                Step phaseAfterStep = new PhaseCompleteStep(afterStepKey, lastStepKey);
                steps.add(phaseAfterStep);
                lastStepKey = phaseAfterStep.getKey();
            }

            phase = previousPhase;
            List<LifecycleAction> orderedActions = type.getOrderedActions(phase);
            ListIterator<LifecycleAction> actionIterator = orderedActions.listIterator(orderedActions.size());
            // add steps for each action, in reverse
            while (actionIterator.hasPrevious()) {
                LifecycleAction action = actionIterator.previous();
                List<Step> actionSteps = action.toSteps(client, phase.getName(), lastStepKey, licenseState);
                ListIterator<Step> actionStepsIterator = actionSteps.listIterator(actionSteps.size());
                while (actionStepsIterator.hasPrevious()) {
                    Step step = actionStepsIterator.previous();
                    steps.add(step);
                    lastStepKey = step.getKey();
                }
            }
        }

        // The very first after step is in a phase before the hot phase so call this "new"
        Step phaseAfterStep = new PhaseCompleteStep(NEW_STEP_KEY, lastStepKey);
        steps.add(phaseAfterStep);
        lastStepKey = phaseAfterStep.getKey();

        // init step so that policy is guaranteed to have
        steps.add(new InitializePolicyContextStep(InitializePolicyContextStep.KEY, lastStepKey));

        Collections.reverse(steps);

        return steps;
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
                throw new IllegalArgumentException(
                    "Action [" + stepKey.getAction() + "] in phase [" + stepKey.getPhase() + "]  does not exist in policy [" + name + "]"
                );
            }
        } else {
            throw new IllegalArgumentException("Phase [" + stepKey.getPhase() + "]  does not exist in policy [" + name + "]");
        }
    }

    /**
     * Validate the name for an policy against some static rules. Intended to match
     * {@link org.elasticsearch.cluster.metadata.MetadataCreateIndexService#validateIndexOrAliasName(String, BiFunction)}
     * @param policy the policy name to validate
     * @throws IllegalArgumentException if the name is invalid
     */
    public static void validatePolicyName(String policy) {
        if (Strings.isNullOrEmpty(policy)) {
            throw new IllegalArgumentException("invalid policy name [" + policy + "]: must not be null or empty");
        }
        if (policy.contains(",")) {
            throw new IllegalArgumentException("invalid policy name [" + policy + "]: must not contain ','");
        }
        if (policy.contains(" ")) {
            throw new IllegalArgumentException("invalid policy name [" + policy + "]: must not contain spaces");
        }
        if (policy.charAt(0) == '_') {
            throw new IllegalArgumentException("invalid policy name [" + policy + "]: must not start with '_'");
        }
        int byteCount = 0;
        byteCount = policy.getBytes(StandardCharsets.UTF_8).length;
        if (byteCount > MAX_INDEX_NAME_BYTES) {
            throw new IllegalArgumentException(
                "invalid policy name [" + policy + "]: name is too long, (" + byteCount + " > " + MAX_INDEX_NAME_BYTES + ")"
            );
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, phases, metadata);
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
        return Objects.equals(name, other.name) && Objects.equals(phases, other.phases) && Objects.equals(metadata, other.metadata);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
