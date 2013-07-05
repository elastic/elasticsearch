/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.util.List;
import java.util.Locale;

/**
 * This abstract class defining basic {@link Decision} used during shard
 * allocation process.
 * 
 * @see AllocationDecider
 */
public abstract class Decision {

    public static final Decision ALWAYS = new Single(Type.YES);
    public static final Decision YES = new Single(Type.YES);
    public static final Decision NO = new Single(Type.NO);
    public static final Decision THROTTLE = new Single(Type.THROTTLE);

    /**
     * Creates a simple decision 
     * @param type {@link Type} of the decision
     * @param explanation explanation of the decision
     * @param explanationParams additional parameters for the decision
     * @return new {@link Decision} instance
     */
    public static Decision single(Type type, String explanation, Object... explanationParams) {
        return new Single(type, explanation, explanationParams);
    }

    /**
     * This enumeration defines the 
     * possible types of decisions 
     */
    public static enum Type {
        YES,
        NO,
        THROTTLE
    }

    /**
     * Get the {@link Type} of this decision
     * @return {@link Type} of this decision
     */
    public abstract Type type();

    /**
     * Simple class representing a single decision
     */
    public static class Single extends Decision {
        private final Type type;
        private final String explanation;
        private final Object[] explanationParams;

        /**
         * Creates a new {@link Single} decision of a given type 
         * @param type {@link Type} of the decision
         */
        public Single(Type type) {
            this(type, null, (Object[]) null);
        }

        /**
         * Creates a new {@link Single} decision of a given type
         *  
         * @param type {@link Type} of the decision
         * @param explanation An explanation of this {@link Decision}
         * @param explanationParams A set of additional parameters
         */
        public Single(Type type, String explanation, Object... explanationParams) {
            this.type = type;
            this.explanation = explanation;
            this.explanationParams = explanationParams;
        }

        @Override
        public Type type() {
            return this.type;
        }

        @Override
        public String toString() {
            if (explanation == null) {
                return type + "()";
            }
            return type + "(" + String.format(Locale.ROOT, explanation, explanationParams) + ")";
        }
    }

    /**
     * Simple class representing a list of decisions
     */
    public static class Multi extends Decision {

        private final List<Decision> decisions = Lists.newArrayList();

        /**
         * Add a decission to this {@link Multi}decision instance
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
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (Decision decision : decisions) {
                sb.append("[").append(decision.toString()).append("]");
            }
            return sb.toString();
        }
    }
}
