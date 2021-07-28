/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents one part of the execution of a {@link LifecycleAction}.
 */
public abstract class Step {
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
        return Objects.equals(key, other.key) &&
                Objects.equals(nextStepKey, other.nextStepKey);
    }

    @Override
    public String toString() {
        return key + " => " + nextStepKey;
    }

    public static final class StepKey implements Writeable, ToXContentObject {
        private final String phase;
        private final String action;
        private final String name;

        public static final ParseField PHASE_FIELD = new ParseField("phase");
        public static final ParseField ACTION_FIELD = new ParseField("action");
        public static final ParseField NAME_FIELD = new ParseField("name");
        private static final ConstructingObjectParser<StepKey, Void> PARSER =
            new ConstructingObjectParser<>("stepkey", a -> new StepKey((String) a[0], (String) a[1], (String) a[2]));
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), PHASE_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), ACTION_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        }

        public StepKey(String phase, String action, String name) {
            this.phase = phase;
            this.action = action;
            this.name = name;
        }

        public StepKey(StreamInput in) throws IOException {
            this.phase = in.readString();
            this.action = in.readString();
            this.name = in.readString();
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

        public String getPhase() {
            return phase;
        }

        public String getAction() {
            return action;
        }

        public String getName() {
            return name;
        }

        @Override
        public int hashCode() {
            return Objects.hash(phase, action, name);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            StepKey other = (StepKey) obj;
            return Objects.equals(phase, other.phase) &&
                    Objects.equals(action, other.action) &&
                    Objects.equals(name, other.name);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
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
