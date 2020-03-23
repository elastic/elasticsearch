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

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * A holder for {@link Writeable}s that can delays reading the underlying
 * {@linkplain Writeable} when it is read from a remote node.
 */
public abstract class DelayableWriteable<T extends Writeable> implements Supplier<T>, Writeable {
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
     * when {@link Supplier#get()} is called.
     */
    public static <T extends Writeable> DelayableWriteable<T> delayed(Writeable.Reader<T> reader, StreamInput in) throws IOException {
        return new Delayed<>(reader, in);
    }

    private DelayableWriteable() {}

    public abstract boolean isDelayed();

    private static class Referencing<T extends Writeable> extends DelayableWriteable<T> {
        private T reference;

        Referencing(T reference) {
            this.reference = reference;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            try (BytesStreamOutput buffer = new BytesStreamOutput()) {
                reference.writeTo(buffer);
                out.writeBytesReference(buffer.bytes());
            }
        }

        @Override
        public T get() {
            return reference;
        }

        @Override
        public boolean isDelayed() {
            return false;
        }
    }

    private static class Delayed<T extends Writeable> extends DelayableWriteable<T> {
        private final Writeable.Reader<T> reader;
        private final Version remoteVersion;
        private final BytesReference serialized;
        private final NamedWriteableRegistry registry;

        Delayed(Writeable.Reader<T> reader, StreamInput in) throws IOException {
            this.reader = reader;
            remoteVersion = in.getVersion();
            serialized = in.readBytesReference();
            registry = in.namedWriteableRegistry();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion() == remoteVersion) {
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
                referencing(get()).writeTo(out);
            }
        }

        @Override
        public T get() {
            try {
                try (StreamInput in = registry == null ?
                        serialized.streamInput() : new NamedWriteableAwareStreamInput(serialized.streamInput(), registry)) {
                    in.setVersion(remoteVersion);
                    return reader.read(in);
                }
            } catch (IOException e) {
                throw new RuntimeException("unexpected error expanding aggregations", e);
            }
        }

        @Override
        public boolean isDelayed() {
            return true;
        }
    }
}
