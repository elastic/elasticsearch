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

package org.elasticsearch.cluster.factory;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;

public abstract class MultiClusterStatePartFactory<T, P> extends ClusterStatePartFactory<T> {

    protected MultiClusterStatePartFactory(String type, EnumSet<XContentContext> context) {
        super(type, context);
    }

    protected MultiClusterStatePartFactory(String type) {
        super(type);
    }

    public MultiClusterStatePartFactory(String type, EnumSet<XContentContext> context, Version since) {
        super(type, context, since);
    }

    @Nullable
    public abstract ClusterStatePartFactory<P> lookupFactory(String type);

    public ClusterStatePartFactory<P> lookupFactorySafe(String type) throws ElasticsearchIllegalArgumentException {
        ClusterStatePartFactory factory = lookupFactory(type);
        if (factory == null) {
            throw new ElasticsearchIllegalArgumentException("No cluster state part factory registered for type [" + type + "]");
        }
        return factory;
    }

    public abstract T fromParts(ImmutableOpenMap<String, P> parts);

    public abstract ImmutableOpenMap<String, P> toParts(T parts);

    @Override
    public T readFrom(StreamInput in, String partName, DiscoveryNode localNode) throws IOException {
        ImmutableOpenMap.Builder<String, P> builder = ImmutableOpenMap.builder();
        int partsSize = in.readVInt();
        for (int i = 0; i < partsSize; i++) {
            String type = in.readString();
            P part = lookupFactorySafe(type).readFrom(in, type, localNode);
            builder.put(type, part);
        }
        return fromParts(builder.build());
    }

    @Override
    public void writeTo(T part, StreamOutput out) throws IOException {
        ImmutableOpenMap<String, P> parts = toParts(part);
        int partsCount = 0;
        for (ObjectObjectCursor<String, P> cursor : parts) {
            ClusterStatePartFactory<P> factory = lookupFactorySafe(cursor.key);
            if (factory.since().onOrAfter(out.getVersion())) {
                partsCount++;
            }
        }
        out.writeVInt(partsCount);
        for (ObjectObjectCursor<String, P> cursor : parts) {
            ClusterStatePartFactory<P> factory = lookupFactorySafe(cursor.key);
            if (factory.since().onOrAfter(out.getVersion())) {
                out.writeString(cursor.key);
                factory.writeTo(cursor.value, out);
            }
        }
    }

    @Override
    public T fromXContent(XContentParser parser, String partName, DiscoveryNode localNode) throws IOException {
        ImmutableOpenMap.Builder<String, P> builder = ImmutableOpenMap.builder();
        String currentFieldName = partName;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                // check if its a custom index metadata
                ClusterStatePartFactory<P> factory = lookupFactory(currentFieldName);
                if (factory == null) {
                    //TODO warn?
                    parser.skipChildren();
                } else {
                    builder.put(currentFieldName, factory.fromXContent(parser, currentFieldName, localNode));
                }
            } else if (token.isValue()) {
                ClusterStatePartFactory<P> factory = lookupFactory(currentFieldName);
                if (factory != null) {
                    builder.put(currentFieldName, factory.fromXContent(parser, currentFieldName, localNode));
                }
            }
        }
        return fromParts(builder.build());
    }

    @Override
    public void toXContent(T part, XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        XContentContext context = XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, XContentContext.API.toString()));
        ImmutableOpenMap<String, P> parts = toParts(part);
        for (ObjectObjectCursor<String, P> partIter : parts) {
            ClusterStatePartFactory<P> factory = lookupFactorySafe(partIter.key);
            if (factory.context().contains(context)) {
                builder.field(partIter.key);
                factory.toXContent(partIter.value, builder, params);
            }
        }
        builder.endObject();
    }
}