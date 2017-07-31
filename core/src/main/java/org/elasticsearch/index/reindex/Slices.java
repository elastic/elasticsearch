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

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents the setting for the number of slices used by a BulkByScrollRequest. Valid values are positive integers and "auto". The
 * "auto" setting defers choosing the number of slices to the request handler.
 */
public final class Slices implements ToXContent, Writeable {

    private static final int AUTO_COUNT = -1;

    public static final String FIELD_NAME = "slices";
    public static final String AUTO_VALUE = "auto";

    public static final Slices AUTO = new Slices(AUTO_COUNT);
    public static final Slices ONE = of(1);
    public static final Slices DEFAULT = ONE;

    private final int count;

    private Slices(int count) {
        this.count = count;
    }

    public Slices(StreamInput stream) throws IOException {
        count = stream.readVInt();
    }

    /**
     * Creates a new {@link Slices} as a concrete number. The given number must be a positive integer.
     */
    public static Slices of(int count) {
        if (count < 1) {
            throw new IllegalArgumentException("[slices] must be at least 1");
        }
        return new Slices(count);
    }

    /**
     * Parse a string to a valid {@link Slices} value
     */
    public static Slices parse(String slicesString) {
        Objects.requireNonNull(slicesString);

        if (AUTO_VALUE.equals(slicesString)) {
            return AUTO;
        }

        try {
            int slicesNumber = Integer.parseInt(slicesString);
            return of(slicesNumber);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[slices] must be a positive integer or the string \"auto\"");
        }
    }

    /**
     * Returns true if this value is set as "auto", false otherwise
     */
    public boolean isAuto() {
        return count == AUTO_COUNT;
    }

    /**
     * Returns true if this value is set as a number, false otherwise
     */
    public boolean isNumber() {
        return !isAuto();
    }

    /**
     * Returns the number value this {@link Slices} has, if it is set as a number. Otherwise throws IllegalStateException
     */
    public int number() {
        if (isAuto()) {
            throw new IllegalStateException("Slice count is set as \"auto\", not a number");
        }
        return count;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(count);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (isAuto()) {
            builder.field(FIELD_NAME, AUTO_VALUE);
        } else {
            builder.field(FIELD_NAME, number());
        }
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }

        Slices other = (Slices) obj;
        return other.count == count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count);
    }

    @Override
    public String toString() {
        if (isAuto()) {
            return "auto";
        } else {
            return Integer.toString(count);
        }
    }
}
