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
package org.elasticsearch.persistent.decider;

import java.util.Locale;
import java.util.Objects;

/**
 * {@link AssignmentDecision} represents the decision made during the process of
 * assigning a persistent task to a node of the cluster.
 *
 * @see EnableAssignmentDecider
 */
public final class AssignmentDecision {

    public static final AssignmentDecision YES = new AssignmentDecision(Type.YES, "");

    private final Type type;
    private final String reason;

    public AssignmentDecision(final Type type, final String reason) {
        this.type = Objects.requireNonNull(type);
        this.reason = Objects.requireNonNull(reason);
    }

    public Type getType() {
        return type;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "assignment decision [type=" + type + ", reason=" + reason + "]";
    }

    public enum Type {
        NO(0), YES(1);

        private final int id;

        Type(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public static Type resolve(final String s) {
            return Type.valueOf(s.toUpperCase(Locale.ROOT));
        }
    }
}
