/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link JobStatsMonitoringDocTests}.
 */
public class JobStatsMonitoringDocTests extends ESTestCase {

    private final String clusterUuid = randomAlphaOfLength(5);
    private final long timestamp = System.currentTimeMillis();
    private final String nodeUuid = randomAlphaOfLength(5);
    private final DiscoveryNode node =
            new DiscoveryNode(nodeUuid, new TransportAddress(TransportAddress.META_ADDRESS, 9300), Version.CURRENT);
    private final JobStats jobStats = mock(JobStats.class);

    private final JobStatsMonitoringDoc doc = new JobStatsMonitoringDoc(clusterUuid, timestamp, node, jobStats);

    public void testConstructorJobStatsMustNotBeNull() {
        expectThrows(NullPointerException.class,
                     () -> new JobStatsMonitoringDoc(clusterUuid, timestamp, node, null));
    }

    public void testConstructor() {
        assertThat(doc.getMonitoringId(), is(MonitoredSystem.ES.getSystem()));
        assertThat(doc.getMonitoringVersion(), is(Version.CURRENT.toString()));
        assertThat(doc.getType(), is(JobStatsMonitoringDoc.TYPE));
        assertThat(doc.getId(), nullValue());
        assertThat(doc.getClusterUUID(), is(clusterUuid));
        assertThat(doc.getTimestamp(), is(timestamp));
        assertThat(doc.getSourceNode(), notNullValue());
        assertThat(doc.getSourceNode().getUUID(), is(nodeUuid));
        assertThat(doc.getJobStats(), is(jobStats));
    }

}
