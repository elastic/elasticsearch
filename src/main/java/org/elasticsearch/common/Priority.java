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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public final class Priority implements Comparable<Priority> {

    public static Priority readFrom(StreamInput input) throws IOException {
        return fromByte(input.readByte());
    }

    public static void writeTo(Priority priority, StreamOutput output) throws IOException {
        byte b = priority.value;
        if (output.getVersion().before(Version.V_1_1_0)) {
            b = (byte) Math.max(URGENT.value, b);
        }
        output.writeByte(b);
    }

    public static Priority fromByte(byte b) {
        switch (b) {
            case -1: return IMMEDIATE;
            case 0: return URGENT;
            case 1: return HIGH;
            case 2: return NORMAL;
            case 3: return LOW;
            case 4: return LANGUID;
            default:
                throw new ElasticsearchIllegalArgumentException("can't find priority for [" + b + "]");
        }
    }

    public static final Priority IMMEDIATE = new Priority((byte) -1);
    public static final Priority URGENT = new Priority((byte) 0);
    public static final Priority HIGH = new Priority((byte) 1);
    public static final Priority NORMAL = new Priority((byte) 2);
    public static final Priority LOW = new Priority((byte) 3);
    public static final Priority LANGUID = new Priority((byte) 4);
    private static final Priority[] values = new Priority[] { IMMEDIATE, URGENT, HIGH, NORMAL, LOW, LANGUID };

    private final byte value;

    private Priority(byte value) {
        this.value = value;
    }

    /**
     * @return an array of all available priorities, sorted from the highest to the lowest.
     */
    public static Priority[] values() {
        return values;
    }

    @Override
    public int compareTo(Priority p) {
        return (this.value < p.value) ? -1 : ((this.value > p.value) ? 1 : 0);
    }

    public boolean after(Priority p) {
        return value > p.value;
    }

    public boolean sameOrAfter(Priority p) {
        return value >= p.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || Priority.class != o.getClass()) return false;

        Priority priority = (Priority) o;

        if (value != priority.value) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) value;
    }

    @Override
    public String toString() {
        switch (value) {
            case (byte) -1: return "IMMEDIATE";
            case (byte) 0: return "URGENT";
            case (byte) 1: return "HIGH";
            case (byte) 2: return "NORMAL";
            case (byte) 3: return "LOW";
            default:
                return "LANGUID";
        }
    }
}
