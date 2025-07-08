/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * This abstract class defining basic {@link Decision} used during shard
 * allocation process.
 *
 * @see AllocationDecider
 */
public sealed interface Decision extends ToXContent, Writeable permits Decision.Single, Decision.Multi {

    Single ALWAYS = new Single(Type.YES);
    Single YES = new Single(Type.YES);
    Single NO = new Single(Type.NO);
    Single THROTTLE = new Single(Type.THROTTLE);

    /**
     * Creates a simple decision
     * @param type {@link Type} of the decision
     * @param label label for the Decider that produced this decision
     * @param explanation explanation of the decision
     * @param explanationParams additional parameters for the decision
     * @return new {@link Decision} instance
     */
    static Decision single(Type type, @Nullable String label, @Nullable String explanation, @Nullable Object... explanationParams) {
        return new Single(type, label, explanation, explanationParams);
    }

    static Decision readFrom(StreamInput in) throws IOException {
        // Determine whether to read a Single or Multi Decision
        if (in.readBoolean()) {
            Multi result = new Multi();
            int decisionCount = in.readVInt();
            for (int i = 0; i < decisionCount; i++) {
                var flag = in.readBoolean();
                assert flag == false : "nested multi decision is not permitted";
                var single = readSingleFrom(in);
                result.decisions.add(single);
            }
            return result;
        } else {
            return readSingleFrom(in);
        }
    }

    private static Single readSingleFrom(StreamInput in) throws IOException {
        final Type type = Type.readFrom(in);
        final String label = in.readOptionalString();
        final String explanation = in.readOptionalString();
        if (label == null && explanation == null) {
            return switch (type) {
                case YES -> YES;
                case THROTTLE -> THROTTLE;
                case NO -> NO;
            };
        }
        return new Single(type, label, explanation);
    }

    /**
     * Get the {@link Type} of this decision
     * @return {@link Type} of this decision
     */
    Type type();

    /**
     * Get the description label for this decision.
     */
    @Nullable
    String label();

    /**
     * Get the explanation for this decision.
     */
    @Nullable
    String getExplanation();

    /**
     * Return the list of all decisions that make up this decision
     */
    List<Decision> getDecisions();

    /**
     * This enumeration defines the
     * possible types of decisions
     */
    enum Type implements Writeable {
        YES(1),
        THROTTLE(2),
        NO(0);

        private final int id;

        Type(int id) {
            this.id = id;
        }

        public static Type readFrom(StreamInput in) throws IOException {
            int i = in.readVInt();
            return switch (i) {
                case 0 -> NO;
                case 1 -> YES;
                case 2 -> THROTTLE;
                default -> throw new IllegalArgumentException("No Type for integer [" + i + "]");
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(id);
        }

        public boolean higherThan(Type other) {
            if (this == NO) {
                return false;
            } else if (other == NO) {
                return true;
            } else return other == THROTTLE && this == YES;
        }

        /**
         * @return lowest decision by precedence NO->THROTTLE->YES
         */
        public static Type min(Type a, Type b) {
            return a.higherThan(b) ? b : a;
        }

    }

    /**
     * Simple class representing a single decision
     */
    record Single(Type type, String label, String explanationString) implements Decision, ToXContentObject {
        /**
         * Creates a new {@link Single} decision of a given type
         * @param type {@link Type} of the decision
         */
        private Single(Type type) {
            this(type, null, null, (Object[]) null);
        }

        /**
         * Creates a new {@link Single} decision of a given type
         *
         * @param type {@link Type} of the decision
         * @param explanation An explanation of this {@link Decision}
         * @param explanationParams A set of additional parameters
         */
        public Single(Type type, @Nullable String label, @Nullable String explanation, @Nullable Object... explanationParams) {
            this(
                type,
                label,
                explanationParams != null && explanationParams.length > 0
                    ? String.format(Locale.ROOT, explanation, explanationParams)
                    : explanation
            );
        }

        @Override
        public List<Decision> getDecisions() {
            return Collections.singletonList(this);
        }

        /**
         * Returns the explanation string, fully formatted.  Only formats the string once.
         */
        @Override
        @Nullable
        public String getExplanation() {
            return this.explanationString;
        }

        @Override
        public String toString() {
            if (explanationString != null) {
                return type + "(" + explanationString + ")";
            }
            return type + "()";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("decider", label);
            builder.field("decision", type);
            builder.field("explanation", explanationString != null ? explanationString : "none");
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(false); // flag specifying its a single decision
            type.writeTo(out);
            out.writeOptionalString(label);
            // Flatten explanation on serialization, so that explanationParams
            // do not need to be serialized
            out.writeOptionalString(explanationString);
        }
    }

    /**
     * Simple class representing a list of decisions
     */
    record Multi(List<Single> decisions) implements Decision, ToXContentFragment {

        public Multi() {
            this(new ArrayList<>());
        }

        /**
         * Add a decision to this {@link Multi}decision instance
         * @param decision {@link Decision} to add
         * @return {@link Multi}decision instance with the given decision added
         */
        public Multi add(Decision decision) {
            assert decision instanceof Single;
            decisions.add((Single) decision);
            return this;
        }

        @Override
        public Type type() {
            Type ret = Type.YES;
            for (int i = 0; i < decisions.size(); i++) {
                Type type = decisions.get(i).type();
                if (type == Type.NO) {
                    return type;
                } else if (type == Type.THROTTLE) {
                    ret = type;
                }
            }
            return ret;
        }

        @Override
        @Nullable
        public String label() {
            // Multi decisions have no labels
            return null;
        }

        @Override
        @Nullable
        public String getExplanation() {
            throw new UnsupportedOperationException("multi-level decisions do not have an explanation");
        }

        @Override
        public List<Decision> getDecisions() {
            return Collections.unmodifiableList(this.decisions);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (Decision decision : decisions) {
                sb.append("[").append(decision.toString()).append("]");
            }
            return sb.toString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            for (Decision d : decisions) {
                d.toXContent(builder, params);
            }
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(true); // flag indicating it is a multi decision
            out.writeCollection(getDecisions());
        }
    }
}
