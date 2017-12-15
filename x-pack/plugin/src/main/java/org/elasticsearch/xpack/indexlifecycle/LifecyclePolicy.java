/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.NamedDiffable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleContext.Listener;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents the lifecycle of an index from creation to deletion. A
 * {@link LifecyclePolicy} is made up of a set of {@link Phase}s which it will
 * move through. Soon we will constrain the phases using some kinda of lifecycle
 * type which will allow only particular {@link Phase}s to be defined, will
 * dictate the order in which the {@link Phase}s are executed and will define
 * which {@link LifecycleAction}s are allowed in each phase.
 */
public abstract class LifecyclePolicy extends AbstractDiffable<LifecyclePolicy>
        implements ToXContentObject, NamedDiffable<LifecyclePolicy> {
    private static final Logger logger = ESLoggerFactory.getLogger(LifecyclePolicy.class);

    public static final ParseField PHASES_FIELD = new ParseField("phases");
    public static final ParseField TYPE_FIELD = new ParseField("type");

    protected final String name;
    protected final Map<String, Phase> phases;

    /**
     * @param name
     *            the name of this {@link LifecyclePolicy}
     * @param phases
     *            a {@link Map} of {@link Phase}s which make up this
     *            {@link LifecyclePolicy}.
     */
    public LifecyclePolicy(String name, Map<String, Phase> phases) {
        this.name = name;
        this.phases = phases;
        validate(phases.values());
    }

    /**
     * For Serialization
     */
    public LifecyclePolicy(StreamInput in) throws IOException {
        name = in.readString();
        phases = Collections.unmodifiableMap(in.readMap(StreamInput::readString, Phase::new));
    }

    public static LifecyclePolicy parse(XContentParser parser, Tuple<String, NamedXContentRegistry> context) {
        return ToXContentContext.PARSER.apply(parser, context);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeMap(phases, StreamOutput::writeString, (o, val) -> val.writeTo(o));
    }

    @Override
    public String getWriteableName() {
        return getType();
    }

    /**
     * @return the name of this {@link LifecyclePolicy}
     */
    public String getName() {
        return name;
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
            builder.field(TYPE_FIELD.getPreferredName(), getType());
            builder.startObject(PHASES_FIELD.getPreferredName());
                for (Phase phase : phases.values()) {
                    builder.field(phase.getName(), phase);
                }
            builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Checks the current state and executes the appropriate {@link Phase}.
     * 
     * @param context
     *            the {@link IndexLifecycleContext} to use to execute the
     *            {@link LifecyclePolicy}.
     */
    public void execute(IndexLifecycleContext context) {
        String currentPhaseName = context.getPhase();
        boolean currentPhaseActionsComplete = context.getAction().equals(Phase.PHASE_COMPLETED);
        String indexName = context.getLifecycleTarget();
        Phase currentPhase = phases.get(currentPhaseName);
        if (Strings.isNullOrEmpty(currentPhaseName) || currentPhaseActionsComplete) {
            Phase nextPhase = nextPhase(currentPhase);
            // We only want to execute the phase if the conditions for executing are met (e.g. the index is old enough)
            if (nextPhase != null && context.canExecute(nextPhase)) {
                String nextPhaseName = nextPhase.getName();
                // Set the phase on the context to this phase so we know where we are next time we execute
                context.setPhase(nextPhaseName, new Listener() {

                    @Override
                    public void onSuccess() {
                        logger.info("Successfully initialised phase [" + nextPhaseName + "] for index [" + indexName + "]");
                        // We might as well execute the phase now rather than waiting for execute to be called again
                        nextPhase.execute(context, getActionProvider(context, nextPhase));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Failed to initialised phase [" + nextPhaseName + "] for index [" + indexName + "]", e);
                    }
                });
            }
        } else {
            // If we have already seen this index and the action is not PHASE_COMPLETED then we just need to execute the current phase again
            if (currentPhase == null) {
                throw new IllegalStateException("Current phase [" + currentPhaseName + "] not found in lifecycle ["
                    + getName() + "] for index [" + indexName + "]");
            } else {
                currentPhase.execute(context, getActionProvider(context, currentPhase));
            }
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

    /**
     * @return the first phase of this policy to execute
     */
    protected abstract Phase getFirstPhase();

    /**
     * @param currentPhase the current phase that is or was just executed
     * @return the next phase after <code>currentPhase</code> to be execute. If it is `null`, the first
     *         phase to be executed is returned. If it is the last phase, then no next phase is to be
     *         executed and `null` is returned.
     */
    protected abstract Phase nextPhase(@Nullable Phase currentPhase);

    /**
     * validates whether the specified <code>phases</code> are valid for this policy instance.
     * @param phases the phases to verify validity against
     * @throws IllegalArgumentException if a specific phase or lack of a specific phase is invalid.
     */
    protected abstract void validate(Collection<Phase> phases);

    /**
     * Each {@link LifecyclePolicy} has a specific type to differentiate themselves. Every implementation
     * is responsible to providing its specific type.
     * @return the {@link LifecyclePolicy} type.
     */
    protected abstract String getType();

    /**
     * @param context the index lifecycle context for this phase at the time of execution
     * @param phase the current phase for which to provide an action provider
     * @return the action provider
     */
    protected abstract NextActionProvider getActionProvider(IndexLifecycleContext context, Phase phase);

    /**
     * Reference to a method that determines which {@link LifecycleAction} to execute next after
     * a specific action.
     *
     * <p>
     * Concrete {@link LifecyclePolicy} classes will implement this to help determine their specific
     * ordering of actions for the phases they allow.
     */
    @FunctionalInterface
    interface NextActionProvider {

        /**
         * @param current The current action which is being or was executed
         * @return the action following {@code current} to execute
         */
        LifecycleAction next(LifecycleAction current);

    }

    /**
     * This class is here to assist in creating a context from which the specific LifecyclePolicy sub-classes can inherit
     * all the previously parsed values
     */
    public static class ToXContentContext {
        private final Map<String, Phase> phases;
        private final String name;

        @SuppressWarnings("unchecked")
        public static ConstructingObjectParser<LifecyclePolicy, Tuple<String, NamedXContentRegistry>> PARSER =
            new ConstructingObjectParser<>("lifecycle_policy", false, (a, c) -> {
                String lifecycleType = (String) a[0];
                List<Phase> phases = (List<Phase>) a[1];
                Map<String, Phase> phaseMap = phases.stream().collect(Collectors.toMap(Phase::getName, Function.identity()));
                NamedXContentRegistry registry = c.v2();
                ToXContentContext factory = new ToXContentContext(c.v1(), phaseMap);
                try {
                    return registry.parseNamedObject(LifecyclePolicy.class, lifecycleType, null, factory);
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
        });
        static {
            PARSER.declareString(constructorArg(), TYPE_FIELD);
            PARSER.declareNamedObjects(constructorArg(), (p, c, n) -> Phase.parse(p, new Tuple<>(n, c.v2())),
                v -> {
                    throw new IllegalArgumentException("ordered " + PHASES_FIELD.getPreferredName() + " are not supported");
                }, PHASES_FIELD);
        }

        ToXContentContext(String name, Map<String, Phase> phases) {
            this.name = name;
            this.phases = phases;
        }

        public String getName() {
            return name;
        }

        public Map<String, Phase> getPhases() {
            return phases;
        }
    }

}
