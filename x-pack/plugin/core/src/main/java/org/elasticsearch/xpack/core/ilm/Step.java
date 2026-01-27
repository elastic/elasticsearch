/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents one part of the execution of a {@link LifecycleAction}.
 */
public abstract class Step {

    /**
     * Retrieves the current {@link Step.StepKey} from the lifecycle state. Note that
     * it is illegal for the step to be set with the phase and/or action unset,
     * or for the step to be unset with the phase and/or action set. All three
     * settings must be either present or missing.
     *
     * @param lifecycleState the index custom data to extract the {@link Step.StepKey} from.
     */
    @Nullable
    public static Step.StepKey getCurrentStepKey(LifecycleExecutionState lifecycleState) {
        Objects.requireNonNull(lifecycleState, "cannot determine current step key as lifecycle state is null");
        String currentPhase = lifecycleState.phase();
        String currentAction = lifecycleState.action();
        String currentStep = lifecycleState.step();
        if (Strings.isNullOrEmpty(currentStep)) {
            assert Strings.isNullOrEmpty(currentPhase) : "Current phase is not empty: " + currentPhase;
            assert Strings.isNullOrEmpty(currentAction) : "Current action is not empty: " + currentAction;
            return null;
        } else {
            assert Strings.isNullOrEmpty(currentPhase) == false;
            assert Strings.isNullOrEmpty(currentAction) == false;
            return new Step.StepKey(currentPhase, currentAction, currentStep);
        }
    }

    private final StepKey key;
    private final StepKey nextStepKey;

    public Step(StepKey key, StepKey nextStepKey) {
        this.key = key;
        this.nextStepKey = nextStepKey;
    }

    public final StepKey getKey() {
        return key;
    }

    public StepKey getNextStepKey() {
        return nextStepKey;
    }

    /**
     * Indicates if the step can be automatically retried when it encounters an execution error.
     */
    public abstract boolean isRetryable();

    @Override
    public int hashCode() {
        return Objects.hash(key, nextStepKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Step other = (Step) obj;
        return Objects.equals(key, other.key) && Objects.equals(nextStepKey, other.nextStepKey);
    }

    @Override
    public String toString() {
        return key + " => " + nextStepKey;
    }

    public record StepKey(String phase, String action, String name) implements Writeable, ToXContentObject {

        public static final ParseField PHASE_FIELD = new ParseField("phase");
        public static final ParseField ACTION_FIELD = new ParseField("action");
        public static final ParseField NAME_FIELD = new ParseField("name");
        private static final ConstructingObjectParser<StepKey, Void> PARSER = new ConstructingObjectParser<>(
            "stepkey",
            a -> new StepKey((String) a[0], (String) a[1], (String) a[2])
        );
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), PHASE_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), ACTION_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        }

        public static StepKey readFrom(StreamInput in) throws IOException {
            return new StepKey(in.readString(), in.readString(), in.readString());
        }

        public static StepKey parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(phase);
            out.writeString(action);
            out.writeString(name);
        }

        @Override
        public String toString() {
            return "{\"phase\":\"" + phase + "\",\"action\":\"" + action + "\",\"name\":\"" + name + "\"}";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PHASE_FIELD.getPreferredName(), phase);
            builder.field(ACTION_FIELD.getPreferredName(), action);
            builder.field(NAME_FIELD.getPreferredName(), name);
            builder.endObject();
            return builder;
        }
    }
}
