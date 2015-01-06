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
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 * Represents a map of cluster state parts with different types.
 * <p/>
 * Only one instance of each type can be present in the composite part. The key of the map is the part's type.
 */
public abstract class NamedCompositeClusterStatePart<E extends ClusterStatePart> extends AbstractClusterStatePart implements NamedClusterStatePart {

    protected final ImmutableOpenMap<String, E> parts;

    protected NamedCompositeClusterStatePart(ImmutableOpenMap<String, E> parts) {
        this.parts = parts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NamedCompositeClusterStatePart that = (NamedCompositeClusterStatePart) o;

        if (parts != null ? !parts.equals(that.parts) : that.parts != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return parts != null ? parts.hashCode() : 0;
    }

    public ImmutableOpenMap<String, E> parts() {
        return parts;
    }

    public <T extends E> T get(String type) {
        return (T) parts.get(type);
    }


    public static abstract class Builder<E extends ClusterStatePart, T extends NamedCompositeClusterStatePart<E>> {

        protected ImmutableOpenMap.Builder<String, E> parts = ImmutableOpenMap.builder();

        public void putAll(T part) {
            parts.putAll(part.parts());
        }

        public void put(String type, E part) {
            parts.put(type, part);
        }

        public void remove(String type) {
            parts.remove(type);
        }

        public abstract T build();

        public abstract String getKey();

        public abstract void parseValuePart(XContentParser parser, String currentFieldName, LocalContext context) throws IOException;

        public abstract void readValuePartsFrom(StreamInput in, LocalContext context) throws IOException;

        public abstract void writeValuePartsTo(StreamOutput out) throws IOException;
    }

    public static abstract class AbstractFactory<E extends ClusterStatePart, T extends NamedCompositeClusterStatePart<E>> extends AbstractClusterStatePart.AbstractFactory<T> {

        private final Map<String, Factory<? extends E>> partFactories = new HashMap<>();

        /**
         * Register a custom index meta data factory. Make sure to call it from a static block.
         */
        public void registerFactory(String type, Factory<? extends E> factory) {
            partFactories.put(type, factory);
        }

        public Set<String> availableFactories() {
            return partFactories.keySet();
        }

        @Nullable
        public <T extends E> Factory<T> lookupFactory(String type) {
            return (Factory<T>) partFactories.get(type);
        }

        public <T extends E> Factory<T> lookupFactorySafe(String type) throws ElasticsearchIllegalArgumentException {
            Factory<T> factory = lookupFactory(type);
            if (factory == null) {
                throw new ElasticsearchIllegalArgumentException("No cluster state part factory registered for type [" + type + "]");
            }
            return factory;
        }

        public abstract Builder<E, T> builder(String key);

        public abstract Builder<E, T> builder(T part);

        @Override
        public T readFrom(StreamInput in, LocalContext context) throws IOException {
            String key = in.readString();
            Builder<E, T> builder = builder(key);
            builder.readValuePartsFrom(in, context);
            int partsSize = in.readVInt();
            for (int i = 0; i < partsSize; i++) {
                String type = in.readString();
                E part = lookupFactorySafe(type).readFrom(in, context);
                builder.put(type, part);
            }
            return builder.build();
        }


        @Override
        public void writeTo(T part, StreamOutput out) throws IOException {
            out.writeString(part.key());
            valuesPartWriteTo(part, out);
            out.writeVInt(part.parts().size());
            for (ObjectObjectCursor<String, E> cursor : part.parts()) {
                Factory<E> factory = lookupFactorySafe(cursor.key);
                if (factory.addedIn().onOrAfter(out.getVersion())) {
                    out.writeString(cursor.key);
                    factory.writeTo(cursor.value, out);
                }
            }
        }

        @Override
        public T fromXContent(XContentParser parser, LocalContext context) throws IOException {
            XContentParser.Token token;
            if (parser.currentToken() == null) { // fresh parser? move to the first token
                parser.nextToken();
            }
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
                parser.nextToken();
            }
            Builder<E, T> builder = builder(parser.currentName());
            String currentFieldName = parser.currentName();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    // check if its a custom index metadata
                    Factory<E> factory = lookupFactory(currentFieldName);
                    if (factory == null) {
                        //TODO warn?
                        parser.skipChildren();
                    } else {
                        builder.put(currentFieldName, factory.fromXContent(parser, context));
                    }
                } else if (token.isValue()) {
                    builder.parseValuePart(parser, currentFieldName, context);
                }
            }
            return builder.build();
        }

        protected abstract void valuesPartWriteTo(T part, StreamOutput out) throws IOException;

        protected abstract void valuesPartToXContent(T part, XContentBuilder builder, Params params) throws IOException;

        @Override
        public void toXContent(T part, XContentBuilder builder, Params params) throws IOException {
            XContentContext context = XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, XContentContext.API.toString()));
            valuesPartToXContent(part, builder, params);
            for (ObjectObjectCursor<String, E> cursor : part.parts) {
                Factory<E> factory = lookupFactorySafe(cursor.key);
                if (factory.context().contains(context)) {
                    builder.startObject(cursor.key);
                    factory.toXContent(cursor.value, builder, params);
                    builder.endObject();
                }
            }
        }

        @Override
        public Diff<T> diff(@Nullable T before, T after) {
            assert after != null;
            Map<String, Diff<E>> diffs = newHashMap();
            List<String> deletes = newArrayList();
            if (before != null) {
                ImmutableOpenMap<String, E> beforeParts = before.parts();
                ImmutableOpenMap<String, E> afterParts = after.parts();
                for (ObjectObjectCursor<String, E> partIter : beforeParts) {
                    if (!afterParts.containsKey(partIter.key)) {
                        deletes.add(partIter.key);
                    }
                }
                for (ObjectObjectCursor<String, E> partIter : afterParts) {
                    Factory<E> factory = lookupFactorySafe(partIter.key);
                    E beforePart = beforeParts.get(partIter.key);
                    if (!partIter.value.equals(beforePart)) {
                        diffs.put(partIter.key, factory.diff(beforePart, partIter.value));
                    }
                }
            } else {
                ImmutableOpenMap<String, E> afterParts = after.parts();
                for (ObjectObjectCursor<String, E> partIter : afterParts) {
                    Factory<E> factory = lookupFactorySafe(partIter.key);
                    diffs.put(partIter.key, factory.diff(null, partIter.value));
                }
            }
            return new CompositeDiff<>(builder(after), deletes, diffs);
        }

        @Override
        public Diff<T> readDiffFrom(StreamInput in, LocalContext context) throws IOException {
            String key = in.readString();
            Builder<E, T> builder = builder(key);
            builder.readValuePartsFrom(in, context);
            int deletesSize = in.readVInt();
            List<String> deletes = new ArrayList<>();
            for (int i = 0; i < deletesSize; i++) {
                deletes.add(in.readString());
            }

            int diffsSize = in.readVInt();
            Map<String, Diff<E>> diffs = newHashMap();
            for (int i = 0; i < diffsSize; i++) {
                String partKey = in.readString();
                diffs.put(partKey, lookupFactorySafe(partKey).readDiffFrom(in, context));
            }
            return new CompositeDiff<>(builder, deletes, diffs);
        }

        @Override
        public void writeDiffsTo(Diff<T> diff, StreamOutput out) throws IOException {
            CompositeDiff<E, T> compositeDiff = (CompositeDiff<E, T>) diff;
            out.writeString(compositeDiff.builder.getKey());
            compositeDiff.builder.writeValuePartsTo(out);
            out.writeVInt(compositeDiff.deletes.size());
            for (String delete : compositeDiff.deletes) {
                out.writeString(delete);
            }

            out.writeVInt(compositeDiff.diffs.size());
            for (Map.Entry<String, Diff<E>> entry : compositeDiff.diffs.entrySet()) {
                Factory<E> factory = lookupFactorySafe(entry.getKey());
                if (factory.addedIn().onOrAfter(out.getVersion())) {
                    out.writeString(entry.getKey());
                    factory.writeDiffsTo(entry.getValue(), out);
                }
            }
        }
    }

    private static class CompositeDiff<E extends ClusterStatePart, T extends NamedCompositeClusterStatePart<E>> implements Diff<T> {

        private final Builder<E, T> builder;
        private final Map<String, Diff<E>> diffs;
        private final List<String> deletes;

        private CompositeDiff(Builder<E, T> builder, List<String> deletes, Map<String, Diff<E>> diffs) {
            this.diffs = diffs;
            this.deletes = deletes;
            this.builder = builder;
        }

        @Override
        public T apply(T part) {
            if (part != null) {
                builder.putAll(part);
                for (String delete : deletes) {
                    builder.remove(delete);
                }

                for (Map.Entry<String, Diff<E>> entry : diffs.entrySet()) {
                    builder.put(entry.getKey(), entry.getValue().apply(part.get(entry.getKey())));
                }
            } else {
                for (Map.Entry<String, Diff<E>> entry : diffs.entrySet()) {
                    builder.put(entry.getKey(), entry.getValue().apply(null));
                }
            }
            return builder.build();
        }

    }


}
