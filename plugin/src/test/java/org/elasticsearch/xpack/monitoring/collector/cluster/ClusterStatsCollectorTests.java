/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import java.util.Collection;

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.collector.AbstractCollectorTestCase;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

//test is just too slow, please fix it to not be sleep-based
@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
public class ClusterStatsCollectorTests extends AbstractCollectorTestCase {

    public void testClusterStatsCollector() throws Exception {
        Collection<MonitoringDoc> results = newClusterStatsCollector().doCollect();
        assertThat(results, hasSize(2));

        // Check cluster info document
        MonitoringDoc monitoringDoc = results.stream().filter(o -> o instanceof ClusterInfoMonitoringDoc).findFirst().get();
        assertNotNull(monitoringDoc);
        assertThat(monitoringDoc, instanceOf(ClusterInfoMonitoringDoc.class));

        ClusterInfoMonitoringDoc clusterInfoMonitoringDoc = (ClusterInfoMonitoringDoc) monitoringDoc;
        assertThat(clusterInfoMonitoringDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
        assertThat(clusterInfoMonitoringDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(clusterInfoMonitoringDoc.getClusterUUID(),
                equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(clusterInfoMonitoringDoc.getTimestamp(), greaterThan(0L));
        assertThat(clusterInfoMonitoringDoc.getSourceNode(), notNullValue());

        assertThat(clusterInfoMonitoringDoc.getClusterName(),
                equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getClusterName().value()));
        assertThat(clusterInfoMonitoringDoc.getVersion(),
                equalTo(client().admin().cluster().prepareNodesInfo().get().getNodes().get(0).getVersion().toString()));

        assertThat(clusterInfoMonitoringDoc.getLicense(), notNullValue());

        assertNotNull(clusterInfoMonitoringDoc.getClusterStats());
        assertThat(clusterInfoMonitoringDoc.getClusterStats().getNodesStats().getCounts().getTotal(),
                equalTo(internalCluster().getNodeNames().length));

        // Check cluster stats document
        monitoringDoc = results.stream().filter(o -> o instanceof ClusterStatsMonitoringDoc).findFirst().get();
        assertNotNull(monitoringDoc);
        assertThat(monitoringDoc, instanceOf(ClusterStatsMonitoringDoc.class));

        ClusterStatsMonitoringDoc clusterStatsMonitoringDoc = (ClusterStatsMonitoringDoc) monitoringDoc;
        assertThat(clusterStatsMonitoringDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
        assertThat(clusterStatsMonitoringDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(clusterStatsMonitoringDoc.getClusterUUID(),
                equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(clusterStatsMonitoringDoc.getTimestamp(), greaterThan(0L));
        assertThat(clusterStatsMonitoringDoc.getSourceNode(), notNullValue());

        assertNotNull(clusterStatsMonitoringDoc.getClusterStats());
        assertThat(clusterStatsMonitoringDoc.getClusterStats().getNodesStats().getCounts().getTotal(),
                equalTo(internalCluster().getNodeNames().length));
    }

    private ClusterStatsCollector newClusterStatsCollector() {
        // This collector runs on master node only
        return newClusterStatsCollector(internalCluster().getMasterName());
    }

    private ClusterStatsCollector newClusterStatsCollector(String nodeId) {
        assertNotNull(nodeId);
        return new ClusterStatsCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MonitoringSettings.class, nodeId),
                internalCluster().getInstance(XPackLicenseState.class, nodeId),
                securedClient(nodeId),
                internalCluster().getInstance(LicenseService.class, nodeId));
    }
}
