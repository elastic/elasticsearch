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

package org.elasticsearch.common.breaker;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Locale;

/**
 * Interface for an object that can be incremented, breaking after some
 * configured limit has been reached.
 */
public interface CircuitBreaker {

    /**
     * Enum used for specifying different types of circuit breakers
     */
    public static enum Name {
        PARENT(0),
        FIELDDATA(1),
        REQUEST(2);

        private int ordinal;

        Name(int ordinal) {
            this.ordinal = ordinal;
        }

        public int getSerializableValue() {
            return this.ordinal;
        }

        public static Name readFrom(StreamInput in) throws IOException {
            int value = in.readVInt();
            switch (value) {
                case 0:
                    return Name.PARENT;
                case 1:
                    return Name.FIELDDATA;
                case 2:
                    return Name.REQUEST;
                default:
                    throw new ElasticsearchIllegalArgumentException("No CircuitBreaker with ordinal: " + value);
            }
        }

        public static void writeTo(Name name, StreamOutput out) throws IOException {
            out.writeVInt(name.getSerializableValue());
        }
    }

    public static enum Type {
        // A regular or child MemoryCircuitBreaker
        MEMORY,
        // A special parent-type for the hierarchy breaker service
        PARENT,
        // A breaker where every action is a noop, it never breaks
        NOOP;

        public static Type parseValue(String value) {
            switch(value.toLowerCase(Locale.ROOT)) {
                case "noop":
                    return Type.NOOP;
                case "parent":
                    return Type.PARENT;
                case "memory":
                    return Type.MEMORY;
                default:
                    throw new ElasticsearchIllegalArgumentException("No CircuitBreaker with type: " + value);
            }
        }
    }

    /**
     * Trip the circuit breaker
     * @param fieldName name of the field responsible for tripping the breaker
     * @param bytesNeeded bytes asked for but unable to be allocated
     */
    public void circuitBreak(String fieldName, long bytesNeeded);

    /**
     * add bytes to the breaker and maybe trip
     * @param bytes number of bytes to add
     * @param label string label describing the bytes being added
     * @return the number of "used" bytes for the circuit breaker
     * @throws CircuitBreakingException
     */
    public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException;

    /**
     * Adjust the circuit breaker without tripping
     */
    public long addWithoutBreaking(long bytes);

    /**
     * @return the currently used bytes the breaker is tracking
     */
    public long getUsed();

    /**
     * @return maximum number of bytes the circuit breaker can track before tripping
     */
    public long getLimit();

    /**
     * @return overhead of circuit breaker
     */
    public double getOverhead();

    /**
     * @return the number of times the circuit breaker has been tripped
     */
    public long getTrippedCount();

    /**
     * @return the name of the breaker
     */
    public Name getName();
}
