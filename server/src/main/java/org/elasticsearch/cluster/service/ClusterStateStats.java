/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.common.annotation.PublicApi;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cluster state related stats.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.12.0")
public abstract class ClusterStateStats implements Writeable, ToXContentObject {

    private AtomicLong updateSuccess = new AtomicLong(0);
    private AtomicLong updateTotalTimeInMillis = new AtomicLong(0);
    private AtomicLong updateFailed = new AtomicLong(0);
    private List<PersistedStateStats> persistenceStats = new ArrayList<>();

    public ClusterStateStats() {}

    public long getUpdateSuccess() {
        return updateSuccess.get();
    }

    public long getUpdateTotalTimeInMillis() {
        return updateTotalTimeInMillis.get();
    }

    public long getUpdateFailed() {
        return updateFailed.get();
    }

    public List<PersistedStateStats> getPersistenceStats() {
        return persistenceStats;
    }

    public void stateUpdated() {
        updateSuccess.incrementAndGet();
    }

    public void stateUpdateFailed() {
        updateFailed.incrementAndGet();
    }

    public void stateUpdateTook(long stateUpdateTime) {
        updateTotalTimeInMillis.addAndGet(stateUpdateTime);
    }

    public ClusterStateStats setPersistenceStats(List<PersistedStateStats> persistenceStats) {
        this.persistenceStats = persistenceStats;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(updateSuccess.get());
        out.writeVLong(updateTotalTimeInMillis.get());
        out.writeVLong(updateFailed.get());
        out.writeVInt(persistenceStats.size());
        for (PersistedStateStats stats : persistenceStats) {
            stats.writeTo(out);
        }
    }

    public ClusterStateStats(StreamInput in) throws IOException {
        this.updateSuccess = new AtomicLong(in.readVLong());
        this.updateTotalTimeInMillis = new AtomicLong(in.readVLong());
        this.updateFailed = new AtomicLong(in.readVLong());
        int persistedStatsSize = in.readVInt();
        this.persistenceStats = new ArrayList<>();
        for (int statsNumber = 0; statsNumber < persistedStatsSize; statsNumber++) {
            PersistedStateStats stats = new PersistedStateStats(in);
            this.persistenceStats.add(stats);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.CLUSTER_STATE_STATS);
        builder.startObject(Fields.OVERALL);
        builder.field(Fields.UPDATE_COUNT, getUpdateSuccess());
        builder.field(Fields.TOTAL_TIME_IN_MILLIS, getUpdateTotalTimeInMillis());
        builder.field(Fields.FAILED_COUNT, getUpdateFailed());
        builder.endObject();
        for (PersistedStateStats stats : persistenceStats) {
            stats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String CLUSTER_STATE_STATS = "cluster_state_stats";
        static final String OVERALL = "overall";
        static final String UPDATE_COUNT = "update_count";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String FAILED_COUNT = "failed_count";
    }
}
