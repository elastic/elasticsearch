/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import org.elasticsearch.ElasticSearchIllegalArgumentException;

/**
 *
 */
public final class Priority implements Comparable<Priority> {

    public static Priority fromByte(byte b) {
        switch (b) {
            case 0:
                return URGENT;
            case 1:
                return HIGH;
            case 2:
                return NORMAL;
            case 3:
                return LOW;
            case 4:
                return LANGUID;
            default:
                throw new ElasticSearchIllegalArgumentException("can't find priority for [" + b + "]");
        }
    }

    public static Priority URGENT = new Priority((byte) 0);
    public static Priority HIGH = new Priority((byte) 1);
    public static Priority NORMAL = new Priority((byte) 2);
    public static Priority LOW = new Priority((byte) 3);
    public static Priority LANGUID = new Priority((byte) 4);

    private final byte value;

    private Priority(byte value) {
        this.value = value;
    }

    public byte value() {
        return this.value;
    }

    public int compareTo(Priority p) {
        return this.value - p.value;
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
            case (byte) 0:
                return "URGENT";
            case (byte) 1:
                return "HIGH";
            case (byte) 2:
                return "NORMAL";
            case (byte) 3:
                return "LOW";
            default:
                return "LANGUID";
        }
    }
}
