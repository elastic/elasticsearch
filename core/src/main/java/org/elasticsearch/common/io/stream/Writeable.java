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

package org.elasticsearch.common.io.stream;

import java.io.IOException;

/**
 * Implementers can be written to a {@linkplain StreamOutput} and read from a {@linkplain StreamInput}. This allows them to be "thrown
 * across the wire" using Elasticsearch's internal protocol. If the implementer also implements equals and hashCode then a copy made by
 * serializing and deserializing must be equal and have the same hashCode. It isn't required that such a copy be entirely unchanged. For
 * example, {@link org.elasticsearch.common.unit.TimeValue} converts the time to nanoseconds for serialization.
 * {@linkplain org.elasticsearch.common.unit.TimeValue} actually implements {@linkplain Streamable} not {@linkplain Writeable} but it has
 * the same contract.
 *
 * Prefer implementing this interface over implementing {@link Streamable} where possible. Lots of code depends on {@linkplain Streamable}
 * so this isn't always possible.
 *
 * The fact that this interface extends {@link StreamableReader} should be consider vestigial. Instead of using its
 * {@link #readFrom(StreamInput)} method you should prefer using the Reader interface as a reference to a constructor that takes
 * {@link StreamInput}. The reasoning behind this is that most "good" readFrom implementations just delegated to such a constructor anyway
 * and they required an unsightly PROTOTYPE object.
 */
public interface Writeable<T> extends StreamableReader<T> { // TODO remove extends StreamableReader<T> from this interface, and remove <T>
    /**
     * Write this into the {@linkplain StreamOutput}.
     */
    void writeTo(StreamOutput out) throws IOException;

    @Override
    default T readFrom(StreamInput in) throws IOException {
        // See class javadoc for reasoning
        throw new UnsupportedOperationException("Prefer calling a constructor that takes a StreamInput to calling readFrom.");
    }

    /**
     * Reference to a method that can read some object from a stream. By convention this is a constructor that takes
     * {@linkplain StreamInput} as an argument for most classes and a static method for things like enums.
     */
    @FunctionalInterface
    interface Reader<R> {
        R read(StreamInput t) throws IOException;
    }
}
