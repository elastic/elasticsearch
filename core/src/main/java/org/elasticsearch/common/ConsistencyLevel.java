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

package org.elasticsearch.common;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Consistency level that allows to define when out of total elements and active elements
 * the consistency is met.
 */
public enum ConsistencyLevel {

    DEFAULT() {
        @Override
        public boolean isMet(int total, int active) {
            return QUORUM.isMet(total, active);
        }
    },
    ONE() {
        @Override
        public boolean isMet(int total, int active) {
            return active >= 1;
        }
    },
    QUORUM() {
        @Override
        public boolean isMet(int total, int active) {
            if (total > 2) {
                return active >= ((total / 2) + 1);
            } else {
                return active >= 1;
            }
        }
    },
    ALL_MINUS_1() {
        @Override
        public boolean isMet(int total, int active) {
            if (total > 1) {
                return active >= (total - 1);
            } else {
                return active == 1;
            }
        }
    },
    ALL() {
        @Override
        public boolean isMet(int total, int active) {
            return active >= total;
        }
    };

    /**
     * Is the consistency level met from the total elements and the current "active" elements.
     */
    public abstract boolean isMet(int total, int active);

    public static ConsistencyLevel readFrom(StreamInput in) throws IOException {
        return ConsistencyLevel.values()[(int) in.readByte()];
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) ordinal());
    }

    public static ConsistencyLevel fromString(String value) {
        if (value.equals("default")) {
            return DEFAULT;
        } else if (value.equals("one")) {
            return ONE;
        } else if (value.equals("quorum")) {
            return QUORUM;
        } else if (value.equals("all-1") || value.equals("full-1")) {
            return ALL_MINUS_1;
        } else if (value.equals("all") || value.equals("full")) {
            return ALL;
        }
        throw new IllegalArgumentException("No write consistency match [" + value + "]");
    }
}
