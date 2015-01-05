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
import org.elasticsearch.cluster.metadata.IndexClusterStatePart;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 * Represents a map of cluster state parts with the same type.
 */
public class MapClusterStatePart<T extends NamedClusterStatePart> extends AbstractClusterStatePart implements IndexClusterStatePart< MapClusterStatePart<T>> {

    private final ImmutableOpenMap<String, T> parts;
    private final EnumSet<XContentContext> xContentContext;
    private final String type;

    public MapClusterStatePart(String type, ImmutableOpenMap<String, T> parts, EnumSet<XContentContext> xContentContext) {
        this.parts = parts;
        this.type = type;
        this.xContentContext = xContentContext;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(parts.size());
        for (ObjectObjectCursor<String, T> part : parts) {
            part.value.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (ObjectCursor<T> part : parts.values()) {
            part.value.toXContent(builder, params);
        }
        return builder;
    }

    public ImmutableOpenMap<String, T> parts() {
        return parts;
    }

    public T get(String type) {
        return parts.get(type);
    }

    @Override
    public EnumSet<XContentContext> context() {
        return xContentContext;
    }

    @Override
    public MapClusterStatePart<T> mergeWith(MapClusterStatePart<T> second) {
        //TODO: Implement
        return null;
    }

    @Override
    public String partType() {
        return type;
    }

    public static class Factory<T extends NamedClusterStatePart> extends AbstractFactory<MapClusterStatePart<T>> {

        private final EnumSet<XContentContext> xContentContext;

        private final ClusterStatePart.Factory<T> factory;

        private final String type;

        public Factory(String type, ClusterStatePart.Factory<T> factory) {
            this(type, factory, API);
        }

        public Factory(String type, ClusterStatePart.Factory<T> factory, EnumSet<XContentContext> xContentContext) {
            this.type = type;
            this.factory = factory;
            this.xContentContext = xContentContext;
        }

        @Override
        public MapClusterStatePart<T> readFrom(StreamInput in, LocalContext context) throws IOException {
            ImmutableOpenMap.Builder<String, T> builder = ImmutableOpenMap.builder();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                T part = factory.readFrom(in, context);
                builder.put(part.key(), part);
            }
            return new MapClusterStatePart<>(type, builder.build(), xContentContext);
        }

        @Override
        public MapClusterStatePart<T> fromXContent(XContentParser parser, LocalContext context) throws IOException {
            ImmutableOpenMap.Builder<String, T> builder = ImmutableOpenMap.builder();
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                T part = factory.fromXContent(parser, context);
                builder.put(part.key(), part);
            }
            return new MapClusterStatePart<>(type, builder.build(), xContentContext);
        }

        @Override
        public Diff<MapClusterStatePart<T>> diff(@Nullable MapClusterStatePart<T> before, MapClusterStatePart<T> after) {
            assert after != null;
            Map<String, Diff<T>> diffs = newHashMap();
            List<String> deletes = newArrayList();
            if (before != null) {
                ImmutableOpenMap<String, T> beforeParts = before.parts();
                ImmutableOpenMap<String, T> afterParts = after.parts();
                if (before.equals(after)) {
                    return new NoDiff<>();
                } else {
                    for (ObjectObjectCursor<String, T> partIter : beforeParts) {
                        if (!afterParts.containsKey(partIter.key)) {
                            deletes.add(partIter.key);
                        }
                    }
                    for (ObjectObjectCursor<String, T> partIter : afterParts) {
                        T beforePart = beforeParts.get(partIter.key);
                        if (!partIter.value.equals(beforePart)) {
                            diffs.put(partIter.key, factory.diff(beforePart, partIter.value));
                        }
                    }
                }
            } else {
                ImmutableOpenMap<String, T> afterParts = after.parts();
                for (ObjectObjectCursor<String, T> partIter : afterParts) {
                    diffs.put(partIter.key, factory.diff(null, partIter.value));
                }
            }
            return new MapDiff<>(type, deletes, diffs, xContentContext);
        }

        @Override
        public Diff<MapClusterStatePart<T>> readDiffFrom(StreamInput in, LocalContext context) throws IOException {
            if (in.readBoolean()) {
                int deletesSize = in.readVInt();
                List<String> deletes = new ArrayList<>();
                for (int i = 0; i < deletesSize; i++) {
                    deletes.add(in.readString());
                }

                int diffsSize = in.readVInt();
                Map<String, Diff<T>> diffs = newHashMap();
                for (int i = 0; i < diffsSize; i++) {
                    String key = in.readString();
                    diffs.put(key, factory.readDiffFrom(in, context));
                }
                return new MapDiff<>(type, deletes, diffs, xContentContext);

            } else {
                return new NoDiff<>();
            }
        }

        public MapClusterStatePart<T> fromOpenMap(ImmutableOpenMap<String, T> map) {
            return new MapClusterStatePart<>(type, map, xContentContext);
        }


        @Override
        public String partType() {
            return type;
        }

    }

    private static class MapDiff<T extends NamedClusterStatePart> implements Diff<MapClusterStatePart<T>> {

        private final String type;
        private final EnumSet<XContentContext> xContentContext;
        private final Map<String, Diff<T>> diffs;
        private final List<String> deletes;

        private MapDiff(String type, List<String> deletes, Map<String, Diff<T>> diffs, EnumSet<XContentContext> xContentContext) {
            this.type = type;
            this.diffs = diffs;
            this.deletes = deletes;
            this.xContentContext = xContentContext;
        }

        @Override
        public MapClusterStatePart<T> apply(MapClusterStatePart<T> part) {
            ImmutableOpenMap.Builder<String, T> parts = ImmutableOpenMap.builder();
            if (part != null) {
                parts.putAll(part.parts);
                for (String delete : deletes) {
                    parts.remove(delete);
                }

                for (Map.Entry<String, Diff<T>> entry : diffs.entrySet()) {
                    parts.put(entry.getKey(), entry.getValue().apply(part.get(entry.getKey())));
                }
            } else {
                for (Map.Entry<String, Diff<T>> entry : diffs.entrySet()) {
                    parts.put(entry.getKey(), entry.getValue().apply(null));
                }
            }
            return new MapClusterStatePart(type, parts.build(), xContentContext);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(true); // We have diffs
            out.writeVInt(deletes.size());
            for (String delete : deletes) {
                out.writeString(delete);
            }

            out.writeVInt(diffs.size());
            for (Map.Entry<String, Diff<T>> entry : diffs.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

}
