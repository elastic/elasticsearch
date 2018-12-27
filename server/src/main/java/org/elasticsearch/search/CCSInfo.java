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

package org.elasticsearch.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Encloses information about a cross-cluster search request: the cluster alias that will be used to prefix the index name returned
 * with the {@link SearchHit}, and whether the sub-request is part of a locally reduced cross-cluster search request.
 * In case a cross-cluster search request fans out to all the (local and remote) shards in one go, the cluster alias has to be used
 * as both the index prefix and the identifier for {@link org.elasticsearch.transport.Transport.Connection} lookup when sending each
 * shard level request. On the other hand, when reducing results locally in each remote cluster, the cluster alias has to be used only
 * as the index prefix, all the targeted shards in each sub-search request are local.
 */
public final class CCSInfo implements Writeable {
    private final String clusterAlias;
    private final boolean localReduction;

    public CCSInfo(String clusterAlias, boolean localReduction) {
        this.clusterAlias = Objects.requireNonNull(clusterAlias);
        this.localReduction = localReduction;
    }

    private CCSInfo(StreamInput in) throws IOException {
        this.clusterAlias = in.readString();
        this.localReduction = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(clusterAlias);
        out.writeBoolean(localReduction);
    }

    /**
     * Returns the prefix to be used for the index names of each search result, to indicate which cluster returned it.
     */
    public String getHitIndexPrefix() {
        return clusterAlias;
    }

    /**
     * Returns the cluster alias needed to lookup the connection when sending shard level requests. <code>null</code> indicates that the
     * shards are local, hence we are executing a cross-cluster search request as part of which each cluster performs its own reduction.
     */
    @Nullable
    public String getConnectionAlias() {
        return localReduction ? null : clusterAlias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CCSInfo ccsInfo = (CCSInfo) o;
        return localReduction == ccsInfo.localReduction &&
            Objects.equals(clusterAlias, ccsInfo.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterAlias, localReduction);
    }

    /**
     * Reads an optional {@link CCSInfo} instance from the provided {@link StreamInput}.
     * Handles backwards compatibility with previous versions that supported only the cluster alias as an optional string. In such
     * cases the localReduction flag will be set to false as CCS local reductions were not supported anyways.
     */
    @Nullable
    public static CCSInfo read(StreamInput in) throws IOException {
        //TODO update version once backported
        if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
            return in.readOptionalWriteable(CCSInfo::new);
        }
        if (in.readBoolean()) {
            return new CCSInfo(in.readString(), false);
        }
        return null;
    }

    /**
     * Writes an optional {@link CCSInfo} instance to the provided {@link StreamOutput}.
     * Handles backwards compatibility with previous versions that supported only the cluster alias as an optional string. In such
     * cases the localReduction flag will be set to false as CCS local reductions were not supported anyways.
     */
    public static void write(@Nullable CCSInfo ccsInfo, StreamOutput out) throws IOException {
        //TODO update version once backported
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeOptionalWriteable(ccsInfo);
        } else {
            if (ccsInfo == null) {
                out.writeBoolean(false);
            } else {
                out.writeOptionalString(ccsInfo.clusterAlias);
            }
        }
    }

    /**
     * Returns a new {@link CCSInfo} instance or <code>null</code> depending on the provided cluster alias.
     * This method is used as a central place to build a {@link CCSInfo} when the cluster alias may be null, hence <code>null</code>
     * should be returned, and when the localReduction flag is not available (as it's not serialized at REST) hence false will be assumed.
     */
    @Nullable
    public static CCSInfo fromClusterAlias(@Nullable String clusterAlias) {
        return clusterAlias == null ? null : new CCSInfo(clusterAlias, false);
    }
}
