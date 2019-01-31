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

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Abstract diffable object with simple diffs implementation that sends the entire object if object has changed or
 * nothing is object remained the same. Comparing to AbstractDiffable, this class also works with NamedWriteables
 */
public abstract class AbstractNamedDiffable<T extends NamedDiffable<T>> implements Diffable<T>, NamedWriteable {

    @Override
    public Diff<T> diff(T previousState) {
        if (this.get().equals(previousState)) {
            return new CompleteNamedDiff<>(previousState.getWriteableName(), previousState.getMinimalSupportedVersion());
        } else {
            return new CompleteNamedDiff<>(get());
        }
    }

    public static <T extends NamedDiffable<T>> NamedDiff<T> readDiffFrom(Class<? extends T> tClass, String name, StreamInput in)
        throws IOException {
        return new CompleteNamedDiff<>(tClass, name, in);
    }

    private static class CompleteNamedDiff<T extends NamedDiffable<T>> implements NamedDiff<T> {

        @Nullable
        private final T part;

        private final String name;

        /**
         * A non-null value is only required for write operation, if the diff was just read from the stream the version
         * is unnecessary.
         */
        @Nullable
        private final Version minimalSupportedVersion;

        /**
         * Creates simple diff with changes
         */
        CompleteNamedDiff(T part) {
            this.part = part;
            this.name = part.getWriteableName();
            this.minimalSupportedVersion = part.getMinimalSupportedVersion();
        }

        /**
         * Creates simple diff without changes
         */
        CompleteNamedDiff(String name, Version minimalSupportedVersion) {
            this.part = null;
            this.name = name;
            this.minimalSupportedVersion = minimalSupportedVersion;
        }

        /**
         * Read simple diff from the stream
         */
        CompleteNamedDiff(Class<? extends T> tClass, String name, StreamInput in) throws IOException {
            if (in.readBoolean()) {
                this.part = in.readNamedWriteable(tClass, name);
                this.minimalSupportedVersion = part.getMinimalSupportedVersion();
            } else {
                this.part = null;
                this.minimalSupportedVersion = null; // We just read this diff, so it's not going to be written
            }
            this.name = name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert minimalSupportedVersion != null : "shouldn't be called on diff that was de-serialized from the stream";
            if (part != null) {
                out.writeBoolean(true);
                part.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public T apply(T part) {
            if (this.part != null) {
                return this.part;
            } else {
                return part;
            }
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            assert minimalSupportedVersion != null : "shouldn't be called on the diff that was de-serialized from the stream";
            return minimalSupportedVersion;
        }
    }

    @SuppressWarnings("unchecked")
    public T get() {
        return (T) this;
    }

}
