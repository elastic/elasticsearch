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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

public abstract class ClusterStatePartFactory<T> {

    public static final String CONTEXT_MODE_PARAM = "context_mode";

    /**
     * Represents the context in which this part should be serialized as JSON
     */
    public enum XContentContext {
        /* A part should be returned as part of API call */
        API,

        /* A part should be stored as a persistent cluster state */
        PERSISTENCE,

        /* A part should be stored as part of a snapshot */
        SNAPSHOT;
    }

    public static EnumSet<XContentContext> NONE = EnumSet.noneOf(XContentContext.class);
    public static EnumSet<XContentContext> API = EnumSet.of(XContentContext.API);
    public static EnumSet<XContentContext> API_PERSISTENCE = EnumSet.of(XContentContext.API, XContentContext.PERSISTENCE);
    public static EnumSet<XContentContext> API_SNAPSHOT = EnumSet.of(XContentContext.API, XContentContext.SNAPSHOT);
    public static EnumSet<XContentContext> API_PERSISTENCE_SNAPSHOT = EnumSet.of(XContentContext.API, XContentContext.PERSISTENCE, XContentContext.SNAPSHOT);
    public static EnumSet<XContentContext> PERSISTENCE = EnumSet.of(XContentContext.PERSISTENCE);
    public static EnumSet<XContentContext> PERSISTENCE_SNAPSHOT = EnumSet.of(XContentContext.PERSISTENCE, XContentContext.SNAPSHOT);

    private final String type;

    private final EnumSet<XContentContext> context;

    private final Version since;

    protected ClusterStatePartFactory(String type) {
        this(type, API);
    }

    protected ClusterStatePartFactory(String type, EnumSet<XContentContext> context) {
        this(type, context, Version.V_2_0_0);
    }

    protected ClusterStatePartFactory(String type, EnumSet<XContentContext> context, Version since) {
        this.type = type;
        this.context = context;
        this.since = since;
    }

    /**
     * Reads a part from input stream
     */
    public abstract T readFrom(StreamInput in, String partName, @Nullable DiscoveryNode localNode) throws IOException;

    /**
     * Writes the  part to output stream
     */
    public abstract void writeTo(T part, StreamOutput out) throws IOException;

    /**
     * Reads a part from XContent stream
     */
    public T fromXContent(XContentParser parser, String partName, @Nullable DiscoveryNode localNode) throws IOException {
        throw new UnsupportedOperationException("The cluster state part with type [" + type + "] doesn't support fromXContent operation");
    }

    /**
     * Writes a part from XContent stream
     */
    public abstract void toXContent(T part, XContentBuilder builder, ToXContent.Params params) throws IOException;

    public T merge(T ... parts) {
        throw new UnsupportedOperationException("This cluster state part doesn't support merge operation");
    }

    /**
     * Reads a part from map
     */
    public T fromMap(Map<String, Object> map, String partName, @Nullable DiscoveryNode localNode) throws IOException {
        // if it starts with the type, remove it
        if (map.size() == 1 && map.containsKey(partType())) {
            map = (Map<String, Object>) map.values().iterator().next();
        }
        XContentBuilder builder = XContentFactory.smileBuilder().map(map);
        try (XContentParser parser = XContentFactory.xContent(XContentType.SMILE).createParser(builder.bytes())) {
            // move to START_OBJECT
            parser.nextToken();
            return fromXContent(parser, partName, localNode);
        }

    }

    /**
     * Returns part type
     */
    public String partType() {
        return type;
    }

    /**
     * Returns the version of Elasticsearch where this part was first added
     */
    public Version since() {
        return since;
    }

    /**
     * Returns a set of contexts in which this part should be serialized as JSON
     */
    public EnumSet<XContentContext> context() {
        return context;
    }

}
