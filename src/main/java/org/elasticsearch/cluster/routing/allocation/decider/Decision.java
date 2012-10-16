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

/**
 */
public abstract class Decision {

    public static final Decision ALWAYS = new Single(Type.YES);
    public static final Decision YES = new Single(Type.YES);
    public static final Decision NO = new Single(Type.NO);
    public static final Decision THROTTLE = new Single(Type.THROTTLE);

    public static Decision single(Type type, String explanation, Object... explanationParams) {
        return new Single(type, explanation, explanationParams);
    }

    public static enum Type {
        YES,
        NO,
        THROTTLE
    }

    public abstract Type type();

    public static class Single extends Decision {
        private final Type type;
        private final String explanation;
        private final Object[] explanationParams;

        public Single(Type type) {
            this(type, null, (Object[]) null);
        }

        public Single(Type type, String explanation, Object... explanationParams) {
            this.type = type;
            this.explanation = explanation;
            this.explanationParams = explanationParams;
        }

        public Type type() {
            return this.type;
        }

        @Override
        public String toString() {
            if (explanation == null) {
                return type + "()";
            }
            return type + "(" + String.format(explanation, explanationParams) + ")";
        }
    }

    public static class Multi extends Decision {

        private final List<Decision> decisions = Lists.newArrayList();

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
