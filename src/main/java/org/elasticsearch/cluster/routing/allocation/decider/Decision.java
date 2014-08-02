/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * This abstract class defining basic {@link Decision} used during shard
 * allocation process.
 * 
 * @see AllocationDecider
 */
public abstract class Decision implements ToXContent {

    public static final Decision ALWAYS = new Single(Type.YES);
    public static final Decision YES = new Single(Type.YES);
    public static final Decision NO = new Single(Type.NO);
    public static final Decision THROTTLE = new Single(Type.THROTTLE);

    /**
     * Creates a simple decision 
     * @param type {@link Type} of the decision
     * @param label label for the Decider that produced this decision
     * @param explanation explanation of the decision
     * @param explanationParams additional parameters for the decision
     * @return new {@link Decision} instance
     */
    public static Decision single(Type type, String label, String explanation, Object... explanationParams) {
        return new Single(type, label, explanation, explanationParams);
    }

    public static void writeTo(Decision decision, StreamOutput out) throws IOException {
        if (decision instanceof Multi) {
            // Flag specifying whether it is a Multi or Single Decision
            out.writeBoolean(true);
            out.writeVInt(((Multi) decision).decisions.size());
            for (Decision d : ((Multi) decision).decisions) {
                writeTo(d, out);
            }
        } else {
            // Flag specifying whether it is a Multi or Single Decision
            out.writeBoolean(false);
            Single d = ((Single) decision);
            Type.writeTo(d.type, out);
            out.writeOptionalString(d.label);
            // Flatten explanation on serialization, so that explanationParams
            // do not need to be serialized
            out.writeOptionalString(d.getExplanation());
        }
    }

    public static Decision readFrom(StreamInput in) throws IOException {
        // Determine whether to read a Single or Multi Decision
        if (in.readBoolean()) {
            Multi result = new Multi();
            int decisionCount = in.readVInt();
            for (int i = 0; i < decisionCount; i++) {
                Decision s = readFrom(in);
                result.decisions.add(s);
            }
            return result;
        } else {
            Single result = new Single();
            result.type = Type.readFrom(in);
            result.label = in.readOptionalString();
            result.explanationString = in.readOptionalString();
            return result;
        }
    }

    /**
     * This enumeration defines the 
     * possible types of decisions 
     */
    public static enum Type {
        YES,
        NO,
        THROTTLE;

        public static Type resolve(String s) {
            return Type.valueOf(s.toUpperCase(Locale.ROOT));
        }

        public static Type readFrom(StreamInput in) throws IOException {
            int i = in.readVInt();
            switch (i) {
                case 0:
                    return NO;
                case 1:
                    return YES;
                case 2:
                    return THROTTLE;
                default:
                    throw new ElasticsearchIllegalArgumentException("No Type for integer [" + i + "]");
            }
        }

        public static void writeTo(Type type, StreamOutput out) throws IOException {
            switch (type) {
                case NO:
                    out.writeVInt(0);
                    break;
                case YES:
                    out.writeVInt(1);
                    break;
                case THROTTLE:
                    out.writeVInt(2);
                    break;
                default:
                    throw new ElasticsearchIllegalArgumentException("Invalid Type [" + type + "]");
            }
        }
    }

    /**
     * Get the {@link Type} of this decision
     * @return {@link Type} of this decision
     */
    public abstract Type type();

    public abstract String label();

    /**
     * Simple class representing a single decision
     */
    public static class Single extends Decision {
        private Type type;
        private String label;
        private String explanation;
        private String explanationString;
        private Object[] explanationParams;

        public Single() {

        }

        /**
         * Creates a new {@link Single} decision of a given type 
         * @param type {@link Type} of the decision
         */
        public Single(Type type) {
            this(type, null, null, (Object[]) null);
        }

        /**
         * Creates a new {@link Single} decision of a given type
         *  
         * @param type {@link Type} of the decision
         * @param explanation An explanation of this {@link Decision}
         * @param explanationParams A set of additional parameters
         */
        public Single(Type type, String label, String explanation, Object... explanationParams) {
            this.type = type;
            this.label = label;
            this.explanation = explanation;
            this.explanationParams = explanationParams;
        }

        @Override
        public Type type() {
            return this.type;
        }

        @Override
        public String label() {
            return this.label;
        }

        /**
         * Returns the explanation string, fully formatted. Only formats the string once
         */
        public String getExplanation() {
            if (explanationString == null && explanation != null) {
                explanationString = String.format(Locale.ROOT, explanation, explanationParams);
            }
            return this.explanationString;
        }

        @Override
        public String toString() {
            if (explanation == null) {
                return type + "()";
            }
            return type + "(" + getExplanation() + ")";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("decider", label);
            builder.field("decision", type);
            String explanation = getExplanation();
            builder.field("explanation", explanation != null ? explanation : "none");
            builder.endObject();
            return builder;
        }
    }

    /**
     * Simple class representing a list of decisions
     */
    public static class Multi extends Decision {

        private final List<Decision> decisions = Lists.newArrayList();

        /**
         * Add a decision to this {@link Multi}decision instance
         * @param decision {@link Decision} to add
         * @return {@link Multi}decision instance with the given decision added
         */
        public Multi add(Decision decision) {
            decisions.add(decision);
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
        public String label() {
            // Multi decisions have no labels
            return null;
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
            builder.startArray("decisions");
            for (Decision d : decisions) {
                d.toXContent(builder, params);
            }
            builder.endArray();
            return builder;
        }
    }
}
