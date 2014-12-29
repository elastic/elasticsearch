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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;

/**
 */
public interface ClusterStatePart extends ToXContent {

    public static final String ALL = "_all";

    public enum XContentContext {
        /* Custom metadata should be returns as part of API call */
        API,

        /* Custom metadata should be stored as part of the persistent cluster state */
        GATEWAY,

        /* Custom metadata should be stored as part of a snapshot */
        SNAPSHOT;
    }

    public static EnumSet<XContentContext> API_ONLY = EnumSet.of(XContentContext.API);
    public static EnumSet<XContentContext> GATEWAY_ONLY = EnumSet.of(XContentContext.GATEWAY);
    public static EnumSet<XContentContext> API_GATEWAY = EnumSet.of(XContentContext.API, XContentContext.GATEWAY);
    public static EnumSet<XContentContext> API_SNAPSHOT = EnumSet.of(XContentContext.API, XContentContext.SNAPSHOT);
    public static EnumSet<XContentContext> API_GATEWAY_SNAPSHOT = EnumSet.of(XContentContext.API, XContentContext.GATEWAY, XContentContext.SNAPSHOT);

    String type();

    void writeTo(StreamOutput out) throws IOException;

    EnumSet<XContentContext> context();

    interface Factory<T extends ClusterStatePart> {

        String type();

        Diff<T> diff(T before, T after);

        Diff<T> readDiffFrom(StreamInput in, LocalContext context) throws IOException;

        T readFrom(StreamInput in, LocalContext context) throws IOException;

        T fromXContent(XContentParser parser, LocalContext context) throws IOException;

        Version addedIn();

    }

    interface Diff<T extends ClusterStatePart> {

        T apply(T part);

        void writeTo(StreamOutput out) throws IOException;
    }
}
