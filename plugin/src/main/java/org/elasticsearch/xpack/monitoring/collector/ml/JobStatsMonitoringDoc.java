/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.util.Objects;

/**
 * Monitoring document collected by {@link JobStatsCollector}.
 * <p>
 * The collected details provide stats for an individual Machine Learning Job rather than the entire payload.
 */
public class JobStatsMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "job_stats";

    private final JobStats jobStats;

    public JobStatsMonitoringDoc(final String clusterUuid, final long timestamp, final DiscoveryNode node,
                                 final JobStats jobStats) {
        super(MonitoredSystem.ES.getSystem(), Version.CURRENT.toString(), TYPE, null, clusterUuid, timestamp, node);

        this.jobStats = Objects.requireNonNull(jobStats);
    }

    public JobStats getJobStats() {
        return jobStats;
    }

}
