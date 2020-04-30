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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;

/**
 * A holder for {@link Writeable}s that delays reading the underlying object
 * on the receiving end. To be used for objects whose deserialized
 * representation is inefficient to keep in memory compared to their
 * corresponding serialized representation.
 * The node that produces the {@link Writeable} calls {@link #referencing(Writeable)}
 * to create a {@link DelayableWriteable} that serializes the inner object
 * first to a buffer and writes the content of the buffer to the {@link StreamOutput}.
 * The receiver node calls {@link #delayed(Reader, StreamInput)} to create a
 * {@link DelayableWriteable} that reads the buffer from the @link {@link StreamInput}
 * but delays creating the actual object by calling {@link #expand()} when needed.
 * Multiple {@link DelayableWriteable}s coming from different nodes may be buffered
 * on the receiver end, which may hold a mix of {@link DelayableWriteable}s that were
 * produced locally (hence expanded) as well as received form another node (hence subject
 * to delayed expansion). When such objects are buffered for some time it may be desirable
 * to force their buffering in serialized format by calling
 * {@link #asSerialized(Reader, NamedWriteableRegistry)}.
 */
public abstract class DelayableWriteable<T extends Writeable> implements Writeable {
    /**
     * Build a {@linkplain DelayableWriteable} that wraps an existing object
     * but is serialized so that deserializing it can be delayed.
     */
    public static <T extends Writeable> DelayableWriteable<T> referencing(T reference) {
        return new Referencing<>(reference);
    }
    /**
     * Build a {@linkplain DelayableWriteable} that copies a buffer from
     * the provided {@linkplain StreamInput} and deserializes the buffer
     * when {@link #expand()} is called.
     */
    public static <T extends Writeable> DelayableWriteable<T> delayed(Writeable.Reader<T> reader, StreamInput in) throws IOException {
        return new Serialized<>(reader, in.getVersion(), in.namedWriteableRegistry(), in.readBytesReference());
    }

    private DelayableWriteable() {}

    /**
     * Returns a {@linkplain DelayableWriteable} that stores its contents
     * in serialized form.
     */
    public abstract Serialized<T> asSerialized(Writeable.Reader<T> reader, NamedWriteableRegistry registry);

    /**
     * Expands the inner {@link Writeable} to its original representation and returns it
     */
    public abstract T expand();

    /**
     * {@code true} if the {@linkplain Writeable} is being stored in
     * serialized form, {@code false} otherwise.
     */
    abstract boolean isSerialized();

    private static class Referencing<T extends Writeable> extends DelayableWriteable<T> {
        private final T reference;

        private Referencing(T reference) {
            this.reference = reference;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(writeToBuffer(out.getVersion()).bytes());
        }

        @Override
        public T expand() {
            return reference;
        }

        @Override
        public Serialized<T> asSerialized(Reader<T> reader, NamedWriteableRegistry registry) {
            BytesStreamOutput buffer;
            try {
                buffer = writeToBuffer(Version.CURRENT);
            } catch (IOException e) {
                throw new RuntimeException("unexpected error writing writeable to buffer", e);
            }
            return new Serialized<>(reader, Version.CURRENT, registry, buffer.bytes());
        }

        @Override
        boolean isSerialized() {
            return false;
        }

        private BytesStreamOutput writeToBuffer(Version version) throws IOException {
            try (BytesStreamOutput buffer = new BytesStreamOutput()) {
                buffer.setVersion(version);
                reference.writeTo(buffer);
                return buffer;
            }
        }
    }

    /**
     * A {@link Writeable} stored in serialized form.
     */
    public static class Serialized<T extends Writeable> extends DelayableWriteable<T> implements Accountable {
        private final Writeable.Reader<T> reader;
        private final Version serializedAtVersion;
        private final NamedWriteableRegistry registry;
        private final BytesReference serialized;

        private Serialized(Writeable.Reader<T> reader, Version serializedAtVersion,
                NamedWriteableRegistry registry, BytesReference serialized) {
            this.reader = reader;
            this.serializedAtVersion = serializedAtVersion;
            this.registry = registry;
            this.serialized = serialized;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion() == serializedAtVersion) {
                /*
                 * If the version *does* line up we can just copy the bytes
                 * which is good because this is how shard request caching
                 * works.
                 */
                out.writeBytesReference(serialized);
            } else {
                /*
                 * If the version doesn't line up then we have to deserialize
                 * into the Writeable and re-serialize it against the new
                 * output stream so it can apply any backwards compatibility
                 * differences in the wire protocol. This ain't efficient but
                 * it should be quite rare.
                 */
                referencing(expand()).writeTo(out);
            }
        }

        @Override
        public T expand() {
            try {
                try (StreamInput in = registry == null ?
                        serialized.streamInput() : new NamedWriteableAwareStreamInput(serialized.streamInput(), registry)) {
                    in.setVersion(serializedAtVersion);
                    return reader.read(in);
                }
            } catch (IOException e) {
                throw new RuntimeException("unexpected error expanding serialized delayed writeable", e);
            }
        }

        @Override
        public Serialized<T> asSerialized(Reader<T> reader, NamedWriteableRegistry registry) {
            return this; // We're already serialized
        }

        @Override
        boolean isSerialized() {
            return true;
        }

        @Override
        public long ramBytesUsed() {
            return serialized.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF * 3 + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
        }
    }
}
