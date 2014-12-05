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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Interface for an object that can be incremented, breaking after some
 * configured limit has been reached.
 */
public interface CircuitBreaker {

    /**
     * Enum used for specifying different types of circuit breakers
     */
    public static class Name {

        private static Map<Integer, Name> names = new HashMap<>();

        public static final Name PARENT = register(0, "parent");
        public static final Name FIELDDATA = register(1, "fielddata");
        public static final Name REQUEST = register(2, "request");

        private final int id;
        private final String label;

        Name(int ordinal, String label) {
            this.id = ordinal;
            this.label = label;
        }

        public int getSerializableValue() {
            return this.id;
        }

        public String toString() {
            return label.toUpperCase(Locale.ENGLISH);
        }

        public static Name register(int id, String label) {
            if (names.containsKey(id)) {
                throw new ElasticsearchIllegalArgumentException(
                        String.format(Locale.ENGLISH,
                                "CircuitBreaker.Name with id %d already registered", id));
            }
            Name name = new Name(id, label);
            names.put(id, name);
            return name;
        }

        public static Name readFrom(StreamInput in) throws IOException {
            int value = in.readVInt();
            Name name = names.get(value);
            if (name == null) {
                throw new ElasticsearchIllegalArgumentException("No CircuitBreaker.Name with id: " + value);
            }
            return name;
        }

        public static void writeTo(Name name, StreamOutput out) throws IOException {
            out.writeVInt(name.getSerializableValue());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Name name = (Name) o;

            if (id != name.id) return false;
            if (label != null ? !label.equals(name.label) : name.label != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + (label != null ? label.hashCode() : 0);
            return result;
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
