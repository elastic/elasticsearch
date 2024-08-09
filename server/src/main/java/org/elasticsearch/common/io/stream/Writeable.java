/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementers can be written to a {@linkplain StreamOutput} and read from a {@linkplain StreamInput}. This allows them to be "thrown
 * across the wire" using Elasticsearch's internal protocol. If the implementer also implements equals and hashCode then a copy made by
 * serializing and deserializing must be equal and have the same hashCode. It isn't required that such a copy be entirely unchanged.
 */
public interface Writeable {

    /**
     * Write this into the {@linkplain StreamOutput}.
     */
    void writeTo(StreamOutput out) throws IOException;

    default boolean supportsZeroCopy() {
        return false;
    }

    default void serialize(SerializationContext result) throws IOException {
        var e = new UnsupportedOperationException("[" + this.getClass() + "] does not support zero copy serialization");
        assert false : e;
        throw e;
    }

    final class SerializationContext {

        private final List<BytesReference> bytesReferences = new ArrayList<>();

        public final BytesStream out;

        private int startingOffset;

        public SerializationContext(BytesStream out) throws IOException {
            this.out = out;
            startingOffset = Math.toIntExact(out.position());
        }

        public void insertBytesReference(BytesReference bytes) throws IOException {
            out.writeVInt(bytes.length());
            int currentPos = Math.toIntExact(out.position());
            bytesReferences.add(out.bytes().slice(startingOffset, currentPos - startingOffset));
            bytesReferences.add(bytes);
            startingOffset = currentPos;
        }

        public BytesReference finish() throws IOException {
            bytesReferences.add(out.bytes().slice(startingOffset, Math.toIntExact(out.position()) - startingOffset));
            return CompositeBytesReference.of(bytesReferences.toArray(new BytesReference[0]));
        }

    }

    /**
     * Reference to a method that can write some object to a {@link StreamOutput}.
     * <p>
     * By convention this is a method from {@link StreamOutput} itself (e.g., {@link StreamOutput#writeString(String)}. If the value can be
     * {@code null}, then the "optional" variant of methods should be used!
     * <p>
     * Most classes should implement {@link Writeable} and the {@link Writeable#writeTo(StreamOutput)} method should <em>use</em>
     * {@link StreamOutput} methods directly or this indirectly:
     * <pre><code>
     * public void writeTo(StreamOutput out) throws IOException {
     *     out.writeVInt(someValue);
     *     out.writeMapOfLists(someMap, StreamOutput::writeString, StreamOutput::writeString);
     * }
     * </code></pre>
     */
    @FunctionalInterface
    interface Writer<V> {

        /**
         * Write {@code V}-type {@code value} to the {@code out}put stream.
         *
         * @param out Output to write the {@code value} too
         * @param value The value to add
         */
        void write(StreamOutput out, V value) throws IOException;

    }

    /**
     * Reference to a method that can read some object from a stream. By convention this is a constructor that takes
     * {@linkplain StreamInput} as an argument for most classes and a static method for things like enums. Returning null from one of these
     * is always wrong - for that we use methods like {@link StreamInput#readOptionalWriteable(Reader)}.
     * <p>
     * As most classes will implement this via a constructor (or a static method in the case of enumerations), it's something that should
     * look like:
     * <pre><code>
     * public MyClass(final StreamInput in) throws IOException {
     *     this.someValue = in.readVInt();
     *     this.someMap = in.readMapOfLists(StreamInput::readString, StreamInput::readString);
     * }
     * </code></pre>
     */
    @FunctionalInterface
    interface Reader<V> {

        /**
         * Read {@code V}-type value from a stream.
         *
         * @param in Input to read the value from
         */
        V read(StreamInput in) throws IOException;
    }

}
