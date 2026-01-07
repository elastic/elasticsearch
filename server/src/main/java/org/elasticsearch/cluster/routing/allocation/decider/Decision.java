/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
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
 * A {@link Decision} used during shard allocation process.
 *
 * @see AllocationDecider
 */
public sealed interface Decision extends ToXContent, Writeable permits Decision.Single, Decision.Multi {

    Single ALWAYS = new Single(Type.YES);
    Single YES = new Single(Type.YES);
    Single NOT_PREFERRED = new Single(Type.NOT_PREFERRED);
    Single NO = new Single(Type.NO);
    Single THROTTLE = new Single(Type.THROTTLE);

    /**
     * Creates a new {@link Decision} instance including some (optional) extra details to explain it. Do not call this method to create a
     * new {@link Decision} instance in an {@link AllocationDecider} implementation unless the explanation is required, because allocation
     * decision-making is a hot path and constructing fresh instances for each decision can be very expensive. Instead, check
     * {@link RoutingAllocation#debugDecision()} before doing nontrivial explanation work, and use one of the constants above such as
     * {@link #YES} or {@link #NO} if no explanation is required. See also {@link RoutingAllocation#decision} for a utility method to
     * perform this check automatically.
     *
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
                case NOT_PREFERRED -> NOT_PREFERRED;
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
     * This enumeration defines the possible types of decisions
     */
    enum Type implements Writeable {
        // ordered by positiveness; order matters for serialization and comparison
        NO,
        NOT_PREFERRED,
        // Temporarily throttled is a better choice than choosing a not-preferred node,
        // but NOT_PREFERRED and THROTTLED are generally not comparable.
        THROTTLE,
        YES;

        // visible for testing
        static final TransportVersion ALLOCATION_DECISION_NOT_PREFERRED = TransportVersion.fromName("allocation_decision_not_preferred");

        public static Type readFrom(StreamInput in) throws IOException {
            if (in.getTransportVersion().supports(AllocationDecision.ADD_NOT_PREFERRED_ALLOCATION_DECISION)) {
                return in.readEnum(Type.class);
            } else if (in.getTransportVersion().supports(ALLOCATION_DECISION_NOT_PREFERRED)) {
                int i = in.readVInt();
                // the order of THROTTLE and NOT_PREFERRED was swapped
                return switch (i) {
                    case 0 -> NO;
                    case 1 -> THROTTLE;
                    case 2 -> NOT_PREFERRED;
                    case 3 -> YES;
                    default -> throw new IllegalArgumentException("No type for integer [" + i + "]");
                };
            } else {
                int i = in.readVInt();
                return switch (i) {
                    case 0 -> NO;
                    case 1 -> YES;
                    case 2 -> THROTTLE;
                    default -> throw new IllegalArgumentException("No Type for integer [" + i + "]");
                };
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(AllocationDecision.ADD_NOT_PREFERRED_ALLOCATION_DECISION)) {
                out.writeEnum(this);
            } else if (out.getTransportVersion().supports(ALLOCATION_DECISION_NOT_PREFERRED)) {
                // the order of THROTTLE and NOT_PREFERRED was swapped
                out.writeVInt(switch (this) {
                    case NOT_PREFERRED -> 2;
                    case THROTTLE -> 1;
                    default -> this.ordinal();
                });
            } else {
                out.writeVInt(switch (this) {
                    case NO -> 0;
                    case NOT_PREFERRED, YES -> 1;
                    case THROTTLE -> 2;
                });
            }
        }

        /**
         * Compares this decision against a new decision. A temporarily THROTTLE'ed node is a better choice than a NOT_PREFERRED node.
         * <p>
         * This is used in simulation, which does not encounter THROTTLE; and allocation explain, which will choose a target node that is
         * THROTTLED temporarily (which simulation ignores and will assign to) over one that is NOT_PREFERRED. Simulation
         * and explain both use the same code paths in the Allocator code, so this helper is used in the Allocator code.
         */
        public boolean isBetterAcrossNodes(Type newDecision) {
            return this.compareTo(newDecision) > 0;
        }

        /**
         * Compares this decision against a new decision. THROTTLE is considered a worse decision than NOT_PREFERRED.
         * <p>
         * A node that has both NOT_PREFERRED and THROTTLE decider results must surface THROTTLE so that the shard does not get moved.
         * THROTTLE will go away eventually and NOT_PREFERRED will surface, and then different action can be taken. This is important for
         * the Reconciler executing real shard movement decisions. Allocation explain will also use it for individual node explanations.
         */
        public boolean isWorseForTheSameNode(Type decision) {
            return switch (decision) {
                case NO -> {
                    yield false;
                }
                case NOT_PREFERRED -> {
                    yield (this == YES) ? false /* this=YES is not worse than NOT_PREFERRED */ : true /* this=THROTTLE is worse */;
                }
                case THROTTLE -> {
                    yield (this == NO) ? true : false /* all Types other than NO are better than THROTTLE */ ;
                }
                case YES -> {
                    yield true;
                }
            };
        }

        /**
         * @return true if Type is one of {NOT_PREFERRED, YES}
         */
        public boolean assignmentAllowed() {
            return this == NOT_PREFERRED || this == YES;
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
            // returns most negative decision
            Decision.Type worst = Type.YES;
            for (Single decision : decisions) {
                final var next = decision.type();
                if (next.isWorseForTheSameNode(worst)) {
                    worst = next;
                }
            }
            return worst;
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
