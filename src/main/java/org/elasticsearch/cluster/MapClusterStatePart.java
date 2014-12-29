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
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

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
            out.writeString(part.key);
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
    }

}
