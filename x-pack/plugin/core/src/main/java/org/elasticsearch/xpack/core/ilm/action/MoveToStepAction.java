/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 */
package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Objects;

public class MoveToStepAction extends ActionType<AcknowledgedResponse> {
    public static final MoveToStepAction INSTANCE = new MoveToStepAction();
    public static final String NAME = "cluster:admin/ilm/_move/post";

    protected MoveToStepAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {
        static final ParseField CURRENT_KEY_FIELD = new ParseField("current_step");
        static final ParseField NEXT_KEY_FIELD = new ParseField("next_step");
        private static final ConstructingObjectParser<Request, String> PARSER =
            new ConstructingObjectParser<>("move_to_step_request", false,
                (a, index) -> {
                    StepKey currentStepKey = (StepKey) a[0];
                    PartialStepKey nextStepKey = (PartialStepKey) a[1];
                    return new Request(index, currentStepKey, nextStepKey);
                });

        static {
            // The current step uses the strict parser (meaning it requires all three parts of a stepkey)
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, name) -> StepKey.parse(p), CURRENT_KEY_FIELD);
            // The target step uses the parser that allows specifying only the phase, or the phase and action
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, name) -> PartialStepKey.parse(p), NEXT_KEY_FIELD);
        }

        private String index;
        private StepKey currentStepKey;
        private PartialStepKey nextStepKey;

        public Request(String index, StepKey currentStepKey, PartialStepKey nextStepKey) {
            this.index = index;
            this.currentStepKey = currentStepKey;
            this.nextStepKey = nextStepKey;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.index = in.readString();
            this.currentStepKey = new StepKey(in);
            if (in.getVersion().onOrAfter(Version.V_7_15_0)) {
                this.nextStepKey = new PartialStepKey(in);
            } else {
                StepKey spec = new StepKey(in);
                this.nextStepKey = new PartialStepKey(spec.getPhase(), spec.getAction(), spec.getName());
            }
        }

        public Request() {
        }

        public String getIndex() {
            return index;
        }

        public StepKey getCurrentStepKey() {
            return currentStepKey;
        }

        public PartialStepKey getNextStepKey() {
            return nextStepKey;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public static Request parseRequest(String name, XContentParser parser) {
            return PARSER.apply(parser, name);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            currentStepKey.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_15_0)) {
                nextStepKey.writeTo(out);
            } else {
                String action = nextStepKey.getAction();
                String name = nextStepKey.getName();
                StepKey key = new StepKey(nextStepKey.getPhase(), action == null ? "" : action, name == null ? "" : name);
                key.writeTo(out);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, currentStepKey, nextStepKey);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(index, other.index) && Objects.equals(currentStepKey, other.currentStepKey)
                && Objects.equals(nextStepKey, other.nextStepKey);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field(CURRENT_KEY_FIELD.getPreferredName(), currentStepKey)
                .field(NEXT_KEY_FIELD.getPreferredName(), nextStepKey)
                .endObject();
        }

        /**
         * A PartialStepKey is like a {@link StepKey}, however, the action and step name are optional.
         */
        public static class PartialStepKey implements Writeable, ToXContentObject {
            private final String phase;
            private final String action;
            private final String name;

            public static final ParseField PHASE_FIELD = new ParseField("phase");
            public static final ParseField ACTION_FIELD = new ParseField("action");
            public static final ParseField NAME_FIELD = new ParseField("name");
            private static final ConstructingObjectParser<PartialStepKey, Void> PARSER =
                new ConstructingObjectParser<>("step_specification",
                    a -> new PartialStepKey((String) a[0], (String) a[1], (String) a[2]));
            static {
                PARSER.declareString(ConstructingObjectParser.constructorArg(), PHASE_FIELD);
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ACTION_FIELD);
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NAME_FIELD);
            }

            public PartialStepKey(String phase, @Nullable String action, @Nullable String name) {
                this.phase = phase;
                this.action = action;
                this.name = name;
                if (name != null && action == null) {
                    throw new IllegalArgumentException("phase; phase and action; or phase, action, and step must be provided, " +
                        "but a step name was specified without a corresponding action");
                }
            }

            public PartialStepKey(StreamInput in) throws IOException {
                this.phase = in.readString();
                this.action = in.readOptionalString();
                this.name = in.readOptionalString();
                if (name != null && action == null) {
                    throw new IllegalArgumentException("phase; phase and action; or phase, action, and step must be provided, " +
                        "but a step name was specified without a corresponding action");
                }
            }

            public static PartialStepKey parse(XContentParser parser) {
                return PARSER.apply(parser, null);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(phase);
                out.writeOptionalString(action);
                out.writeOptionalString(name);
            }

            @Nullable
            public String getPhase() {
                return phase;
            }

            @Nullable
            public String getAction() {
                return action;
            }

            @Nullable
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
                PartialStepKey other = (PartialStepKey) obj;
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
                if (action != null) {
                    builder.field(ACTION_FIELD.getPreferredName(), action);
                }
                if (name != null) {
                    builder.field(NAME_FIELD.getPreferredName(), name);
                }
                builder.endObject();
                return builder;
            }
        }
    }
}
