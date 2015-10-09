/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.AbstractCollectorTestCase;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.Matchers.*;

public class ClusterStatsCollectorTests extends AbstractCollectorTestCase {

    @Test
    public void testClusterStatsCollector() throws Exception {
        Collection<MarvelDoc> results = newClusterStatsCollector().doCollect();
        assertThat(results, hasSize(1));

        MarvelDoc marvelDoc = results.iterator().next();
        assertNotNull(marvelDoc);
        assertThat(marvelDoc, instanceOf(ClusterStatsMarvelDoc.class));

        ClusterStatsMarvelDoc clusterStatsMarvelDoc = (ClusterStatsMarvelDoc) marvelDoc;
        assertThat(clusterStatsMarvelDoc.clusterUUID(), equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(clusterStatsMarvelDoc.timestamp(), greaterThan(0L));
        assertThat(clusterStatsMarvelDoc.type(), equalTo(ClusterStatsCollector.TYPE));

        assertNotNull(clusterStatsMarvelDoc.getClusterStats());
        assertThat(clusterStatsMarvelDoc.getClusterStats().getNodesStats().getCounts().getTotal(), equalTo(internalCluster().getNodeNames().length));
    }

    @Test @AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/683")
    public void testClusterStatsCollectorWithLicensing() {
        String[] nodes = internalCluster().getNodeNames();
        for (String node : nodes) {
            logger.debug("--> creating a new instance of the collector");
            ClusterStatsCollector collector = newClusterStatsCollector(node);
            assertNotNull(collector);

            logger.debug("--> enabling license and checks that the collector can collect data if node is master");
            enableLicense();
            if (node.equals(internalCluster().getMasterName())) {
                assertCanCollect(collector);
            } else {
                assertCannotCollect(collector);
            }

            logger.debug("--> starting graceful period and checks that the collector can still collect data if node is master");
            beginGracefulPeriod();
            if (node.equals(internalCluster().getMasterName())) {
                assertCanCollect(collector);
            } else {
                assertCannotCollect(collector);
            }

            logger.debug("--> ending graceful period and checks that the collector cannot collect data");
            endGracefulPeriod();
            assertCannotCollect(collector);

            logger.debug("--> disabling license and checks that the collector cannot collect data");
            disableLicense();
            assertCannotCollect(collector);
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
                internalCluster().getInstance(MarvelSettings.class, nodeId),
                internalCluster().getInstance(MarvelLicensee.class, nodeId),
                securedClient(nodeId));
    }
}
