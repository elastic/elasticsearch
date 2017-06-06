/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.monitoring.collector.ml.JobStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolverTestCase;

import static org.hamcrest.Matchers.startsWith;

public class JobStatsResolverTests extends MonitoringIndexNameResolverTestCase<JobStatsMonitoringDoc, JobStatsResolver> {

    @Override
    protected JobStatsMonitoringDoc newMonitoringDoc() {
        final JobStats jobStats =
                new JobStats("fake-job1", new DataCounts("fake-job1"),
                             null, JobState.OPENED, null, null, null);

        return new JobStatsMonitoringDoc(randomAlphaOfLength(5),
                                         Math.abs(randomLong()),
                                         new DiscoveryNode("id", buildNewFakeTransportAddress(), Version.CURRENT),
                                         jobStats);
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    public void testClusterInfoResolver() throws Exception {
        JobStatsMonitoringDoc doc = newMonitoringDoc();
        JobStatsResolver resolver = newResolver();

        assertThat(resolver.index(doc), startsWith(".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION));

        assertSource(resolver.source(doc, XContentType.JSON),
                     Sets.newHashSet(
                         "cluster_uuid",
                         "timestamp",
                         "type",
                         "source_node",
                         "job_stats"),
                     XContentType.JSON);
    }

}
