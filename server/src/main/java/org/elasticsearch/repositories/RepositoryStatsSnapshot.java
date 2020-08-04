/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;

public final class RepositoryStatsSnapshot implements Writeable, ToXContent {
    private final RepositoryInfo repositoryInfo;
    private final RepositoryStats repositoryStats;
    @Nullable
    private final Long createdAtMillis;

    public RepositoryStatsSnapshot(RepositoryInfo repositoryInfo,
                                   RepositoryStats repositoryStats,
                                   @Nullable Long createdAtMillis) {
        this.repositoryInfo = repositoryInfo;
        this.repositoryStats = repositoryStats;
        this.createdAtMillis = createdAtMillis;
    }

    public RepositoryStatsSnapshot(StreamInput in) throws IOException {
        this.repositoryInfo = new RepositoryInfo(in);
        this.repositoryStats = new RepositoryStats(in);
        this.createdAtMillis = null;
    }

    public RepositoryInfo getRepositoryInfo() {
        return repositoryInfo;
    }

    public RepositoryStats getRepositoryStats() {
        return repositoryStats;
    }

    public long ageInMillis(LongSupplier relativeTimeInMillis) {
        return createdAtMillis == null ? 0 : relativeTimeInMillis.getAsLong() - createdAtMillis;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        repositoryInfo.writeTo(out);
        repositoryStats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        repositoryInfo.toXContent(builder, params);
        builder.field("request_counts", repositoryStats.requestCounts);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepositoryStatsSnapshot that = (RepositoryStatsSnapshot) o;
        return Objects.equals(repositoryInfo, that.repositoryInfo) &&
            Objects.equals(repositoryStats, that.repositoryStats) &&
            Objects.equals(createdAtMillis, that.createdAtMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(repositoryInfo, repositoryStats, createdAtMillis);
    }

    @Override
    public String toString() {
        return "RepositoryStatsSnapshot{" +
            "repositoryInfo=" + repositoryInfo +
            ", repositoryStats=" + repositoryStats +
            ", createdAtMillis=" + createdAtMillis +
            '}';
    }
}
