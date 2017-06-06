/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.ml;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.collector.ml.JobStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;

import java.io.IOException;

public class JobStatsResolver extends MonitoringIndexNameResolver.Timestamped<JobStatsMonitoringDoc> {

    public JobStatsResolver(MonitoredSystem system, Settings settings) {
        super(system, settings);
    }

    @Override
    protected void buildXContent(JobStatsMonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
        final JobStats jobStats = document.getJobStats();

        builder.startObject(document.getType());
        {
            jobStats.toUnwrappedXContent(builder);
        }
        builder.endObject();
    }

}
