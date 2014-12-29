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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 * Represents a map of cluster state parts with different types.
 * <p/>
 * Only one instance of each type can be present in the composite part. The key of the map is the part's type.
 */
public abstract class CompositeClusterStatePart<T extends CompositeClusterStatePart> extends AbstractClusterStatePart {

    private final static Map<String, Factory> partFactories = new HashMap<>();
    protected final long version;
    protected final String uuid;
    protected final ImmutableOpenMap<String, ClusterStatePart> parts;

    protected CompositeClusterStatePart(long version, String uuid, ImmutableOpenMap<String, ClusterStatePart> parts) {
        this.version = version;
        this.uuid = uuid;
        this.parts = parts;
    }

    /**
     * Register a custom index meta data factory. Make sure to call it from a static block.
     */
    public static void registerFactory(String type, ClusterStatePart.Factory factory) {
        partFactories.put(type, factory);
    }

    @Nullable
    public static <T extends ClusterStatePart> ClusterStatePart.Factory<T> lookupFactory(String type) {
        return partFactories.get(type);
    }

    public static <T extends ClusterStatePart> ClusterStatePart.Factory<T> lookupFactorySafe(String type) throws ElasticsearchIllegalArgumentException {
        ClusterStatePart.Factory<T> factory = lookupFactory(type);
        if (factory == null) {
            throw new ElasticsearchIllegalArgumentException("No cluster state part factory registered for type [" + type + "]");
        }
        return factory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompositeClusterStatePart that = (CompositeClusterStatePart) o;

        if (parts != null ? !parts.equals(that.parts) : that.parts != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return parts != null ? parts.hashCode() : 0;
    }

    public ImmutableOpenMap<String, ClusterStatePart> parts() {
        return parts;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(version);
        out.writeString(uuid);
        out.writeVInt(parts().size());
        for (ObjectObjectCursor<String, ClusterStatePart> cursor : parts()) {
            out.writeString(cursor.key);
            cursor.value.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("version", version);
        builder.field("uuid", uuid);
        for (ObjectObjectCursor<String, ClusterStatePart> partIter : parts) {
            builder.startObject(partIter.key);
            partIter.value.toXContent(builder, params);
        }
        return builder;
    }

    public <T extends ClusterStatePart> T get(String type) {
        return (T) parts.get(type);
    }

    public long getVersion() {
        return version;
    }

    public String getUuid() {
        return uuid;
    }

    public long version() {
        return version;
    }

    public String uuid() {
        return uuid;
    }

    public static abstract class AbstractCompositeClusterStatePartFactory<T extends CompositeClusterStatePart> extends AbstractClusterStatePart.AbstractFactory<T> {

        @Override
        public T readFrom(StreamInput in, LocalContext context) throws IOException {
            long version = in.readVLong();
            String uuid = in.readString();
            ImmutableOpenMap.Builder<String, ClusterStatePart> builder = ImmutableOpenMap.builder();
            int partsSize = in.readVInt();
            for (int i = 0; i < partsSize; i++) {
                String type = in.readString();
                ClusterStatePart part = lookupFactorySafe(type).readFrom(in, context);
                builder.put(type, part);
            }
            return fromParts(version, uuid, builder);
        }

        public abstract T fromParts(long version, String uuid, ImmutableOpenMap.Builder<String, ClusterStatePart> parts);

        @Override
        public Diff<T> diff(T before, T after) {
            ImmutableOpenMap<String, ClusterStatePart> beforeParts = before.parts();
            ImmutableOpenMap<String, ClusterStatePart> afterParts = after.parts();
            if (before.equals(after)) {
                return new NoDiff<T>();
            } else {
                Map<String, Diff<ClusterStatePart>> diffs = newHashMap();
                List<String> deletes = newArrayList();
                for (ObjectObjectCursor<String, ClusterStatePart> partIter : beforeParts) {
                    if (!afterParts.containsKey(partIter.key)) {
                        deletes.add(partIter.key);
                    }
                }
                for (ObjectObjectCursor<String, ClusterStatePart> partIter : afterParts) {
                    ClusterStatePart.Factory<ClusterStatePart> factory = lookupFactorySafe(partIter.key);
                    diffs.put(partIter.key, factory.diff(beforeParts.get(partIter.key), partIter.value));
                }
                return new CompositeDiff<>(this, after.version(), before.uuid(), after.uuid(), deletes, diffs);
            }
        }

        @Override
        public Diff<T> readDiffFrom(StreamInput in, LocalContext context) throws IOException {
            if (in.readBoolean()) {
                long version = in.readVLong();
                String previousUuid = in.readString();
                String uuid = in.readString();
                int deletesSize = in.readVInt();
                List<String> deletes = new ArrayList<>();
                for (int i = 0; i < deletesSize; i++) {
                    deletes.add(in.readString());
                }

                int diffsSize = in.readVInt();
                Map<String, Diff<ClusterStatePart>> diffs = newHashMap();
                for (int i = 0; i < diffsSize; i++) {
                    String key = in.readString();
                    diffs.put(key, lookupFactorySafe(key).readDiffFrom(in, context));
                }
                return new CompositeDiff<>(this, version, previousUuid, uuid, deletes, diffs);

            } else {
                return new NoDiff<T>();
            }
        }
    }

    private static class CompositeDiff<T extends CompositeClusterStatePart> implements Diff<T> {

        private long version;
        private String previousUuid;
        private String uuid;
        private Map<String, Diff<ClusterStatePart>> diffs;
        private List<String> deletes;
        private AbstractCompositeClusterStatePartFactory<T> factory;

        private CompositeDiff(AbstractCompositeClusterStatePartFactory<T> factory, long version, String previousUuid, String uuid, List<String> deletes, Map<String, Diff<ClusterStatePart>> diffs) {
            this.version = version;
            this.previousUuid = previousUuid;
            this.uuid = uuid;
            this.diffs = diffs;
            this.deletes = deletes;
            this.factory = factory;
        }

        @Override
        public T apply(T part) {
            if(!previousUuid.equals(part.getUuid())) {
                throw new IncompatibleClusterStateVersionException("Expected diffs for version " + part.version + " with uuid " + part.uuid + " got uuid " + previousUuid);
            }
            ImmutableOpenMap.Builder<String, ClusterStatePart> parts = ImmutableOpenMap.builder();
            parts.putAll(part.parts);
            for (String delete : deletes) {
                parts.remove(delete);
            }

            for (Map.Entry<String, Diff<ClusterStatePart>> entry : diffs.entrySet()) {
                parts.put(entry.getKey(), entry.getValue().apply(part.get(entry.getKey())));
            }
            return factory.fromParts(version, uuid, parts);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(true); // We have diffs
            out.writeVLong(version);
            out.writeString(previousUuid);
            out.writeString(uuid);
            out.writeVInt(deletes.size());
            for (String delete : deletes) {
                out.writeString(delete);
            }

            out.writeVInt(diffs.size());
            for (Map.Entry<String, Diff<ClusterStatePart>> entry : diffs.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }


}
