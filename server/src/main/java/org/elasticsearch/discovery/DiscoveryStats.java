/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.coordination.PendingClusterStateStats;
import org.elasticsearch.cluster.coordination.PublishClusterStateStats;
import org.elasticsearch.cluster.service.ClusterApplierRecordingService;
import org.elasticsearch.cluster.service.ClusterStateUpdateStats;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class DiscoveryStats implements Writeable, ToXContentFragment {

    private final PendingClusterStateStats queueStats;
    private final PublishClusterStateStats publishStats;
    private final ClusterStateUpdateStats clusterStateUpdateStats;
    private final ClusterApplierRecordingService.Stats applierRecordingStats;

    public DiscoveryStats(
        PendingClusterStateStats queueStats,
        PublishClusterStateStats publishStats,
        ClusterStateUpdateStats clusterStateUpdateStats,
        ClusterApplierRecordingService.Stats applierRecordingStats
    ) {
        this.queueStats = queueStats;
        this.publishStats = publishStats;
        this.clusterStateUpdateStats = clusterStateUpdateStats;
        this.applierRecordingStats = applierRecordingStats;
    }

    public DiscoveryStats(StreamInput in) throws IOException {
        queueStats = in.readOptionalWriteable(PendingClusterStateStats::new);
        publishStats = in.readOptionalWriteable(PublishClusterStateStats::new);
        if (in.getVersion().onOrAfter(Version.V_7_16_0)) {
            clusterStateUpdateStats = in.readOptionalWriteable(ClusterStateUpdateStats::new);
        } else {
            clusterStateUpdateStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_16_0)) {
            applierRecordingStats = in.readOptionalWriteable(ClusterApplierRecordingService.Stats::new);
        } else {
            applierRecordingStats = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(queueStats);
        out.writeOptionalWriteable(publishStats);
        if (out.getVersion().onOrAfter(Version.V_7_16_0)) {
            out.writeOptionalWriteable(clusterStateUpdateStats);
        }
        if (out.getVersion().onOrAfter(Version.V_7_16_0)) {
            out.writeOptionalWriteable(applierRecordingStats);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DISCOVERY);
        if (queueStats != null) {
            queueStats.toXContent(builder, params);
        }
        if (publishStats != null) {
            publishStats.toXContent(builder, params);
        }
        if (clusterStateUpdateStats != null) {
            clusterStateUpdateStats.toXContent(builder, params);
        }
        if (applierRecordingStats != null) {
            applierRecordingStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public ClusterStateUpdateStats getClusterStateUpdateStats() {
        return clusterStateUpdateStats;
    }

    static final class Fields {
        static final String DISCOVERY = "discovery";
    }

    public PendingClusterStateStats getQueueStats() {
        return queueStats;
    }

    public PublishClusterStateStats getPublishStats() {
        return publishStats;
    }

    public ClusterApplierRecordingService.Stats getApplierRecordingStats() {
        return applierRecordingStats;
    }
}
