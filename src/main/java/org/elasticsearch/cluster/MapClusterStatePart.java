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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 * Represents a map of cluster state parts with the same type.
 */
public class MapClusterStatePart<T extends MapItemClusterStatePart> extends AbstractClusterStatePart {

    private final ImmutableOpenMap<String, T> parts;

    public MapClusterStatePart(ImmutableOpenMap<String, T> parts) {
        this.parts = parts;
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


    public static class Factory<T extends MapItemClusterStatePart> extends AbstractFactory<MapClusterStatePart<T>> {

        private final ClusterStatePart.Factory<T> factory;

        public Factory(ClusterStatePart.Factory<T> factory) {
            this.factory = factory;
        }

        @Override
        public MapClusterStatePart<T> readFrom(StreamInput in, LocalContext context) throws IOException {
            ImmutableOpenMap.Builder<String, T> builder = ImmutableOpenMap.builder();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                T part = factory.readFrom(in, context);
                builder.put(part.key(), part);
            }
            return new MapClusterStatePart<>(builder.build());
        }

        @Override
        public MapClusterStatePart<T> fromXContent(XContentParser parser, LocalContext context) throws IOException {
            ImmutableOpenMap.Builder<String, T> builder = ImmutableOpenMap.builder();
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                T part = factory.fromXContent(parser, context);
                builder.put(part.key(), part);
            }
            return new MapClusterStatePart<>(builder.build());
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
                        diffs.put(partIter.key, factory.diff(beforePart, partIter.value));
                    }
                }
            } else {
                ImmutableOpenMap<String, T> afterParts = after.parts();
                for (ObjectObjectCursor<String, T> partIter : afterParts) {
                    diffs.put(partIter.key, factory.diff(null, partIter.value));
                }
            }
            return new MapDiff<>(deletes, diffs);
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
                return new MapDiff<>(deletes, diffs);

            } else {
                return new NoDiff<>();
            }
        }

    }

    private static class MapDiff<T extends MapItemClusterStatePart> implements Diff<MapClusterStatePart<T>> {

        private final Map<String, Diff<T>> diffs;
        private final List<String> deletes;

        private MapDiff(List<String> deletes, Map<String, Diff<T>> diffs) {
            this.diffs = diffs;
            this.deletes = deletes;
        }

        @Override
        public MapClusterStatePart<T> apply(MapClusterStatePart<T> part) {
            ImmutableOpenMap.Builder<String, T> parts = ImmutableOpenMap.builder();
            parts.putAll(part.parts);
            for (String delete : deletes) {
                parts.remove(delete);
            }

            for (Map.Entry<String, Diff<T>> entry : diffs.entrySet()) {
                parts.put(entry.getKey(), entry.getValue().apply(part.get(entry.getKey())));
            }
            return new MapClusterStatePart(parts.build());
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
