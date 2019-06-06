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

public enum Priority {

    IMMEDIATE((byte) 0),
    URGENT((byte) 1),
    HIGH((byte) 2),
    NORMAL((byte) 3),
    LOW((byte) 4),
    LANGUID((byte) 5);

    public static Priority readFrom(StreamInput input) throws IOException {
        return fromByte(input.readByte());
    }

    public static void writeTo(Priority priority, StreamOutput output) throws IOException {
        output.writeByte(priority.value);
    }

    public static Priority fromByte(byte b) {
        switch (b) {
            case 0: return IMMEDIATE;
            case 1: return URGENT;
            case 2: return HIGH;
            case 3: return NORMAL;
            case 4: return LOW;
            case 5: return LANGUID;
            default:
                throw new IllegalArgumentException("can't find priority for [" + b + "]");
        }
    }

    private final byte value;

    Priority(byte value) {
        this.value = value;
    }

    public boolean after(Priority p) {
        return this.compareTo(p) > 0;
    }

    public boolean sameOrAfter(Priority p) {
        return this.compareTo(p) >= 0;
    }

}
