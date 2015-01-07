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
import org.apache.commons.lang3.builder.Diff;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
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

    protected final long version;
    protected final String uuid;
    protected final ImmutableOpenMap<String, ClusterStatePart> parts;

    protected CompositeClusterStatePart(long version, String uuid, ImmutableOpenMap<String, ClusterStatePart> parts) {
        this.version = version;
        this.uuid = uuid;
        this.parts = parts;
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

    protected static abstract class AbstractCompositeFactory<T extends CompositeClusterStatePart> extends AbstractClusterStatePart.AbstractFactory<T> {

        private final Map<String, Factory> partFactories = new HashMap<>();
        /**
         * Register a custom index meta data factory. Make sure to call it from a static block.
         */
        public void registerFactory(String type, ClusterStatePart.Factory factory) {
            partFactories.put(type, factory);
        }

        @Nullable
        public <T extends ClusterStatePart> ClusterStatePart.Factory<T> lookupFactory(String type) {
            return partFactories.get(type);
        }

        public <T extends ClusterStatePart> ClusterStatePart.Factory<T> lookupFactorySafe(String type) throws ElasticsearchIllegalArgumentException {
            ClusterStatePart.Factory<T> factory = lookupFactory(type);
            if (factory == null) {
                throw new ElasticsearchIllegalArgumentException("No cluster state part factory registered for type [" + type + "]");
            }
            return factory;
        }

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
        public void writeTo(T part, StreamOutput out) throws IOException {
            out.writeVLong(part.version);
            out.writeString(part.uuid);
            out.writeVInt(part.parts().size());
            ImmutableOpenMap<String, ClusterStatePart> parts = part.parts;
            for (ObjectObjectCursor<String, ClusterStatePart> cursor : parts) {
                out.writeString(cursor.key);
                lookupFactorySafe(cursor.key).writeTo(cursor.value, out);
            }
        }

        @Override
        public void toXContent(T part, XContentBuilder builder, Params params) throws IOException {
            XContentContext context = XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, XContentContext.API.toString()));
            builder.field("version", part.version);
            builder.field("uuid", part.uuid);
            ImmutableOpenMap<String, ClusterStatePart> parts = part.parts;
            for (ObjectObjectCursor<String, ClusterStatePart> partIter : parts) {
                Factory<ClusterStatePart> factory = lookupFactorySafe(partIter.key);
                if (factory.context().contains(context)) {
                    builder.field(partIter.key);
                    factory.toXContent(partIter.value, builder, params);
                }
            }
        }

        @Override
        public Diff<T> diff(@Nullable T before, T after) {
            assert after != null;
            Map<String, Diff<ClusterStatePart>> diffs = newHashMap();
            List<String> deletes = newArrayList();
            if (before != null) {
                ImmutableOpenMap<String, ClusterStatePart> beforeParts = before.parts();
                ImmutableOpenMap<String, ClusterStatePart> afterParts = after.parts();
                for (ObjectObjectCursor<String, ClusterStatePart> partIter : beforeParts) {
                    if (!afterParts.containsKey(partIter.key)) {
                        deletes.add(partIter.key);
                    }
                }
                for (ObjectObjectCursor<String, ClusterStatePart> partIter : afterParts) {
                    ClusterStatePart.Factory<ClusterStatePart> factory = lookupFactorySafe(partIter.key);
                    ClusterStatePart beforePart = beforeParts.get(partIter.key);
                    if (!partIter.value.equals(beforePart)) {
                        diffs.put(partIter.key, factory.diff(beforePart, partIter.value));
                    }
                }
            } else {
                ImmutableOpenMap<String, ClusterStatePart> afterParts = after.parts();
                for (ObjectObjectCursor<String, ClusterStatePart> partIter : afterParts) {
                    ClusterStatePart.Factory<ClusterStatePart> factory = lookupFactorySafe(partIter.key);
                    diffs.put(partIter.key, factory.diff(null, partIter.value));
                }
            }
            return new CompositeDiff<>(this, after.version(), before.uuid(), after.uuid(), deletes, diffs);
        }

        @Override
        public Diff<T> readDiffFrom(StreamInput in, LocalContext context) throws IOException {
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
        }

        @Override
        public void writeDiffsTo(Diff<T> diff, StreamOutput out) throws IOException {
            CompositeDiff<T> compositeDiff = (CompositeDiff<T>) diff;
            out.writeVLong(compositeDiff.version);
            out.writeString(compositeDiff.previousUuid);
            out.writeString(compositeDiff.uuid);
            out.writeVInt(compositeDiff.deletes.size());
            for (String delete : compositeDiff.deletes) {
                out.writeString(delete);
            }

            out.writeVInt(compositeDiff.diffs.size());
            for (Map.Entry<String, Diff<ClusterStatePart>> entry : compositeDiff.diffs.entrySet()) {
                Factory<ClusterStatePart> factory = lookupFactory(entry.getKey());
                if (factory.addedIn().onOrAfter(out.getVersion())) {
                    out.writeString(entry.getKey());
                    factory.writeDiffsTo(entry.getValue(), out);
                }
            }
        }

        @Override
        public T fromXContent(XContentParser parser, LocalContext context) throws IOException {
            XContentParser.Token token;
            long version = -1;
            String uuid = "_na_";
            ImmutableOpenMap.Builder<String, ClusterStatePart> parts = ImmutableOpenMap.builder();
            String currentFieldName = parser.currentName();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    // check if its a custom index metadata
                    ClusterStatePart.Factory<ClusterStatePart> factory = lookupFactory(currentFieldName);
                    if (factory == null) {
                        //TODO warn?
                        parser.skipChildren();
                    } else {
                        parts.put(currentFieldName, factory.fromXContent(parser, context));
                    }
                } else if (token.isValue()) {
                    if ("version".equals(currentFieldName)) {
                        version = parser.longValue();
                    } else if ("uuid".equals(currentFieldName)) {
                        uuid = parser.text();
                    }
                }
            }
            return fromParts(version, uuid, parts);
        }
    }

    private static class CompositeDiff<T extends CompositeClusterStatePart> implements Diff<T> {

        private final long version;
        private final String previousUuid;
        private final String uuid;
        private final Map<String, Diff<ClusterStatePart>> diffs;
        private final List<String> deletes;
        private final AbstractCompositeFactory<T> factory;

        private CompositeDiff(AbstractCompositeFactory<T> factory, long version, String previousUuid, String uuid, List<String> deletes, Map<String, Diff<ClusterStatePart>> diffs) {
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
    }


}
