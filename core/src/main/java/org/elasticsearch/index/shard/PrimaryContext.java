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

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Represents the primary context.
 */
public class PrimaryContext implements Writeable {

    private final long clusterStateVersion;

    /**
     * The cluster state version on the primary.
     *
     * @return the cluster state version
     */
    public long clusterStateVersion() {
        return clusterStateVersion;
    }

    /**
     * Construct a primary context with the specified cluster state version as the cluster state version for the context.
     *
     * @param clusterStateVersion the cluster state version
     */
    public PrimaryContext(final long clusterStateVersion) {
        this.clusterStateVersion = clusterStateVersion;
    }

    /**
     * Construct a primary context from a stream.
     *
     * @param in the stream
     * @throws IOException if an I/O exception occurs reading from the stream
     */
    public PrimaryContext(final StreamInput in) throws IOException {
        clusterStateVersion = in.readVLong();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVLong(clusterStateVersion);
    }

}
