/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public final class RepositoryStatsSnapshot implements Writeable, ToXContentObject {
    public static final long UNKNOWN_CLUSTER_VERSION = -1;
    private final RepositoryInfo repositoryInfo;
    private final RepositoryStats repositoryStats;
    private final long clusterVersion;
    private final boolean archived;

    public RepositoryStatsSnapshot(RepositoryInfo repositoryInfo, RepositoryStats repositoryStats, long clusterVersion, boolean archived) {
        assert archived != (clusterVersion == UNKNOWN_CLUSTER_VERSION);
        this.repositoryInfo = repositoryInfo;
        this.repositoryStats = repositoryStats;
        this.clusterVersion = clusterVersion;
        this.archived = archived;
    }

    public RepositoryStatsSnapshot(StreamInput in) throws IOException {
        this.repositoryInfo = new RepositoryInfo(in);
        this.repositoryStats = new RepositoryStats(in);
        this.clusterVersion = in.readLong();
        this.archived = in.readBoolean();
    }

    public RepositoryInfo getRepositoryInfo() {
        return repositoryInfo;
    }

    public RepositoryStats getRepositoryStats() {
        return repositoryStats;
    }

    public boolean isArchived() {
        return archived;
    }

    public long getClusterVersion() {
        return clusterVersion;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        repositoryInfo.writeTo(out);
        repositoryStats.writeTo(out);
        out.writeLong(clusterVersion);
        out.writeBoolean(archived);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        repositoryInfo.toXContent(builder, params);
        builder.field("request_counts", repositoryStats.requestCounts);
        builder.field("archived", archived);
        if (archived) {
            builder.field("cluster_version", clusterVersion);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepositoryStatsSnapshot that = (RepositoryStatsSnapshot) o;
        return repositoryInfo.equals(that.repositoryInfo)
            && repositoryStats.equals(that.repositoryStats)
            && clusterVersion == that.clusterVersion
            && archived == that.archived;
    }

    @Override
    public int hashCode() {
        return Objects.hash(repositoryInfo, repositoryStats, clusterVersion, archived);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
