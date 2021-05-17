/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.ml;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.Objects;

/**
 * Monitoring document collected by {@link JobStatsCollector}.
 * <p>
 * The collected details provide stats for an individual Machine Learning Job rather than the entire payload.
 */
public class JobStatsMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "job_stats";

    private final JobStats jobStats;

    public JobStatsMonitoringDoc(final String cluster,
                                 final long timestamp,
                                 final long intervalMillis,
                                 final MonitoringDoc.Node node,
                                 final JobStats jobStats) {
        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null);
        this.jobStats = Objects.requireNonNull(jobStats);
    }

    JobStats getJobStats() {
        return jobStats;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        {
            jobStats.toUnwrappedXContent(builder);
        }
        builder.endObject();
    }
}
