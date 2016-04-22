/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.collector.AbstractCollectorTestCase;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.MonitoringLicensee;

import java.util.Collection;

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

        ClusterInfoMonitoringDoc clusterInfoMarvelDoc = (ClusterInfoMonitoringDoc) monitoringDoc;
        assertThat(clusterInfoMarvelDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
        assertThat(clusterInfoMarvelDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(clusterInfoMarvelDoc.getClusterUUID(),
                equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(clusterInfoMarvelDoc.getTimestamp(), greaterThan(0L));
        assertThat(clusterInfoMarvelDoc.getSourceNode(), notNullValue());

        assertThat(clusterInfoMarvelDoc.getClusterName(),
                equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getClusterName().value()));
        assertThat(clusterInfoMarvelDoc.getVersion(),
                equalTo(client().admin().cluster().prepareNodesInfo().get().getNodes()[0].getVersion().toString()));

        assertThat(clusterInfoMarvelDoc.getLicense(), notNullValue());

        assertNotNull(clusterInfoMarvelDoc.getClusterStats());
        assertThat(clusterInfoMarvelDoc.getClusterStats().getNodesStats().getCounts().getTotal(),
                equalTo(internalCluster().getNodeNames().length));

        // Check cluster stats document
        monitoringDoc = results.stream().filter(o -> o instanceof ClusterStatsMonitoringDoc).findFirst().get();
        assertNotNull(monitoringDoc);
        assertThat(monitoringDoc, instanceOf(ClusterStatsMonitoringDoc.class));

        ClusterStatsMonitoringDoc clusterStatsMarvelDoc = (ClusterStatsMonitoringDoc) monitoringDoc;
        assertThat(clusterStatsMarvelDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
        assertThat(clusterStatsMarvelDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(clusterStatsMarvelDoc.getClusterUUID(),
                equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(clusterStatsMarvelDoc.getTimestamp(), greaterThan(0L));
        assertThat(clusterStatsMarvelDoc.getSourceNode(), notNullValue());

        assertNotNull(clusterStatsMarvelDoc.getClusterStats());
        assertThat(clusterStatsMarvelDoc.getClusterStats().getNodesStats().getCounts().getTotal(),
                equalTo(internalCluster().getNodeNames().length));
    }

    public void testClusterStatsCollectorWithLicensing() {
        try {
            String[] nodes = internalCluster().getNodeNames();
            for (String node : nodes) {
                logger.debug("--> creating a new instance of the collector");
                ClusterStatsCollector collector = newClusterStatsCollector(node);
                assertNotNull(collector);

                logger.debug("--> enabling license and checks that the collector can collect data if node is master");
                enableLicense();
                if (node.equals(internalCluster().getMasterName())) {
                    assertCanCollect(collector, ClusterInfoMonitoringDoc.class, ClusterStatsMonitoringDoc.class);
                } else {
                    assertCannotCollect(collector);
                }

                logger.debug("--> starting graceful period and checks that the collector can still collect data if node is master");
                beginGracefulPeriod();
                if (node.equals(internalCluster().getMasterName())) {
                    assertCanCollect(collector, ClusterInfoMonitoringDoc.class, ClusterStatsMonitoringDoc.class);
                } else {
                    assertCannotCollect(collector);
                }

                logger.debug("--> ending graceful period and checks that the collector can still collect data (if node is master)");
                endGracefulPeriod();
                if (node.equals(internalCluster().getMasterName())) {
                    assertCanCollect(collector, ClusterInfoMonitoringDoc.class);
                } else {
                    assertCannotCollect(collector);
                }

                logger.debug("--> disabling license and checks that the collector can still collect data (if node is master)");
                disableLicense();
                if (node.equals(internalCluster().getMasterName())) {
                    assertCanCollect(collector, ClusterInfoMonitoringDoc.class);
                } else {
                    assertCannotCollect(collector);
                }
            }
        } finally {
            // Ensure license is enabled before finishing the test
            enableLicense();
        }
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
                internalCluster().getInstance(MonitoringLicensee.class, nodeId),
                securedClient(nodeId),
                internalCluster().getInstance(LicensesManagerService.class, nodeId),
                internalCluster().getInstance(ClusterName.class, nodeId));
    }

    private void assertCanCollect(AbstractCollector collector, Class<?>... classes) {
        super.assertCanCollect(collector);
        Collection results = collector.collect();
        if (classes != null) {
            assertThat(results.size(), equalTo(classes.length));
            for (Class<?> cl : classes) {
                assertThat(results.stream().filter(o -> cl.isInstance(o)).count(), equalTo(1L));
            }
        }
    }
}
