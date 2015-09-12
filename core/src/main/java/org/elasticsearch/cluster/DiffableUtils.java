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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class DiffableUtils {
    private DiffableUtils() {
    }

    /**
     * Calculates diff between two ImmutableOpenMaps of Diffable objects
     */
    public static <T extends Diffable<T>> Diff<ImmutableOpenMap<String, T>> diff(ImmutableOpenMap<String, T> before, ImmutableOpenMap<String, T> after) {
        assert after != null && before != null;
        return new ImmutableOpenMapDiff<>(before, after);
    }

    /**
     * Calculates diff between two ImmutableMaps of Diffable objects
     */
    public static <T extends Diffable<T>> Diff<ImmutableMap<String, T>> diff(ImmutableMap<String, T> before, ImmutableMap<String, T> after) {
        assert after != null && before != null;
        return new ImmutableMapDiff<>(before, after);
    }

    /**
     * Loads an object that represents difference between two ImmutableOpenMaps
     */
    public static <T extends Diffable<T>> Diff<ImmutableOpenMap<String, T>> readImmutableOpenMapDiff(StreamInput in, KeyedReader<T> keyedReader) throws IOException {
        return new ImmutableOpenMapDiff<>(in, keyedReader);
    }

    /**
     * Loads an object that represents difference between two ImmutableMaps
     */
    public static <T extends Diffable<T>> Diff<ImmutableMap<String, T>> readImmutableMapDiff(StreamInput in, KeyedReader<T> keyedReader) throws IOException {
        return new ImmutableMapDiff<>(in, keyedReader);
    }

    /**
     * Loads an object that represents difference between two ImmutableOpenMaps
     */
    public static <T extends Diffable<T>> Diff<ImmutableOpenMap<String, T>> readImmutableOpenMapDiff(StreamInput in, T proto) throws IOException {
        return new ImmutableOpenMapDiff<>(in, new PrototypeReader<>(proto));
    }

    /**
     * Loads an object that represents difference between two ImmutableMaps
     */
    public static <T extends Diffable<T>> Diff<ImmutableMap<String, T>> readImmutableMapDiff(StreamInput in, T proto) throws IOException {
        return new ImmutableMapDiff<>(in, new PrototypeReader<>(proto));
    }

    /**
     * A reader that can deserialize an object. The reader can select the deserialization type based on the key. It's
     * used in custom metadata deserialization.
     */
    public interface KeyedReader<T> {

        /**
         * reads an object of the type T from the stream input
         */
        T readFrom(StreamInput in, String key) throws IOException;

        /**
         * reads an object that respresents differences between two objects with the type T from the stream input
         */
        Diff<T> readDiffFrom(StreamInput in, String key) throws IOException;
    }

    /**
     * Implementation of the KeyedReader that is using a prototype object for reading operations
     *
     * Note: this implementation is ignoring the key.
     */
    public static class PrototypeReader<T extends Diffable<T>> implements KeyedReader<T> {
        private T proto;

        public PrototypeReader(T proto) {
            this.proto = proto;
        }

        @Override
        public T readFrom(StreamInput in, String key) throws IOException {
            return proto.readFrom(in);
        }

        @Override
        public Diff<T> readDiffFrom(StreamInput in, String key) throws IOException {
            return proto.readDiffFrom(in);
        }
    }

    /**
     * Represents differences between two ImmutableMaps of diffable objects
     *
     * @param <T> the diffable object
     */
    private static class ImmutableMapDiff<T extends Diffable<T>> extends MapDiff<T, ImmutableMap<String, T>> {

        protected ImmutableMapDiff(StreamInput in, KeyedReader<T> reader) throws IOException {
            super(in, reader);
        }

        public ImmutableMapDiff(ImmutableMap<String, T> before, ImmutableMap<String, T> after) {
            assert after != null && before != null;
            for (String key : before.keySet()) {
                if (!after.containsKey(key)) {
                    deletes.add(key);
                }
            }
            for (ImmutableMap.Entry<String, T> partIter : after.entrySet()) {
                T beforePart = before.get(partIter.getKey());
                if (beforePart == null) {
                    adds.put(partIter.getKey(), partIter.getValue());
                } else if (partIter.getValue().equals(beforePart) == false) {
                    diffs.put(partIter.getKey(), partIter.getValue().diff(beforePart));
                }
            }
        }

        @Override
        public ImmutableMap<String, T> apply(ImmutableMap<String, T> map) {
            HashMap<String, T> builder = new HashMap<>();
            builder.putAll(map);

            for (String part : deletes) {
                builder.remove(part);
            }

            for (Map.Entry<String, Diff<T>> diff : diffs.entrySet()) {
                builder.put(diff.getKey(), diff.getValue().apply(builder.get(diff.getKey())));
            }

            for (Map.Entry<String, T> additon : adds.entrySet()) {
                builder.put(additon.getKey(), additon.getValue());
            }
            return ImmutableMap.copyOf(builder);
        }
    }

    /**
     * Represents differences between two ImmutableOpenMap of diffable objects
     *
     * @param <T> the diffable object
     */
    private static class ImmutableOpenMapDiff<T extends Diffable<T>> extends MapDiff<T, ImmutableOpenMap<String, T>> {

        protected ImmutableOpenMapDiff(StreamInput in, KeyedReader<T> reader) throws IOException {
            super(in, reader);
        }

        public ImmutableOpenMapDiff(ImmutableOpenMap<String, T> before, ImmutableOpenMap<String, T> after) {
            assert after != null && before != null;
            for (ObjectCursor<String> key : before.keys()) {
                if (!after.containsKey(key.value)) {
                    deletes.add(key.value);
                }
            }
            for (ObjectObjectCursor<String, T> partIter : after) {
                T beforePart = before.get(partIter.key);
                if (beforePart == null) {
                    adds.put(partIter.key, partIter.value);
                } else if (partIter.value.equals(beforePart) == false) {
                    diffs.put(partIter.key, partIter.value.diff(beforePart));
                }
            }
        }

        @Override
        public ImmutableOpenMap<String, T> apply(ImmutableOpenMap<String, T> map) {
            ImmutableOpenMap.Builder<String, T> builder = ImmutableOpenMap.builder();
            builder.putAll(map);

            for (String part : deletes) {
                builder.remove(part);
            }

            for (Map.Entry<String, Diff<T>> diff : diffs.entrySet()) {
                builder.put(diff.getKey(), diff.getValue().apply(builder.get(diff.getKey())));
            }

            for (Map.Entry<String, T> additon : adds.entrySet()) {
                builder.put(additon.getKey(), additon.getValue());
            }
            return builder.build();
        }
    }

    /**
     * Represents differences between two maps of diffable objects
     *
     * This class is used as base class for different map implementations
     *
     * @param <T> the diffable object
     */
    private static abstract class MapDiff<T extends Diffable<T>, M> implements Diff<M> {

        protected final List<String> deletes;
        protected final Map<String, Diff<T>> diffs;
        protected final Map<String, T> adds;

        protected MapDiff() {
            deletes = new ArrayList<>();
            diffs = new HashMap<>();
            adds = new HashMap<>();
        }

        protected MapDiff(StreamInput in, KeyedReader<T> reader) throws IOException {
            deletes = new ArrayList<>();
            diffs = new HashMap<>();
            adds = new HashMap<>();
            int deletesCount = in.readVInt();
            for (int i = 0; i < deletesCount; i++) {
                deletes.add(in.readString());
            }

            int diffsCount = in.readVInt();
            for (int i = 0; i < diffsCount; i++) {
                String key = in.readString();
                Diff<T> diff = reader.readDiffFrom(in, key);
                diffs.put(key, diff);
            }

            int addsCount = in.readVInt();
            for (int i = 0; i < addsCount; i++) {
                String key = in.readString();
                T part = reader.readFrom(in, key);
                adds.put(key, part);
            }
        }
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(deletes.size());
            for (String delete : deletes) {
                out.writeString(delete);
            }

            out.writeVInt(diffs.size());
            for (Map.Entry<String, Diff<T>> entry : diffs.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }

            out.writeVInt(adds.size());
            for (Map.Entry<String, T> entry : adds.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }
}
