/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleContext.Listener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
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
        builder.field(TYPE_FIELD.getPreferredName(), type.getType());
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
            Phase nextPhase = type.nextPhase(phases, currentPhase);
            // We only want to execute the phase if the conditions for executing are met (e.g. the index is old enough)
            if (nextPhase != null && context.canExecute(nextPhase)) {
                String nextPhaseName = nextPhase.getName();
                // Set the phase on the context to this phase so we know where we are next time we execute
                context.setPhase(nextPhaseName, new Listener() {

                    @Override
                    public void onSuccess() {
                        logger.info("Successfully initialised phase [" + nextPhaseName + "] for index [" + indexName + "]");
                        // We might as well execute the phase now rather than waiting for execute to be called again
                        nextPhase.execute(context, type.getActionProvider(context, nextPhase));
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
                currentPhase.execute(context, type.getActionProvider(context, currentPhase));
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
     * Reference to a method that determines which {@link LifecycleAction} to
     * execute next after a specific action.
     *
     * <p>
     * Concrete {@link LifecyclePolicy} classes will implement this to help
     * determine their specific ordering of actions for the phases they allow.
     */
    @FunctionalInterface
    interface NextActionProvider {

        /**
         * @param current
         *            The current action which is being or was executed
         * @return the action following {@code current} to execute
         */
        LifecycleAction next(LifecycleAction current);

    }

}
