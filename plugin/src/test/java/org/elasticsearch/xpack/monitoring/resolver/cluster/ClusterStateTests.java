/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.cluster;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStateCollector;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStateMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.elasticsearch.xpack.security.InternalClient;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false)
public class ClusterStateTests extends MonitoringIntegTestCase {

    private int randomInt = randomInt();
    private ThreadPool threadPool = null;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void removeThreadPool() throws InterruptedException {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.exporters.default_local.type", "local")
                .put("node.attr.custom", randomInt)
                .build();
    }

    public void testClusterState() throws Exception {
        final String masterNodeName = internalCluster().getMasterName();
        final MonitoringSettings monitoringSettings = new MonitoringSettings(Settings.EMPTY, clusterService().getClusterSettings());
        final InternalClient client = new InternalClient(Settings.EMPTY, threadPool, internalCluster().client(masterNodeName));
        final ClusterStateCollector collector =
                new ClusterStateCollector(Settings.EMPTY,
                                          internalCluster().clusterService(masterNodeName),
                                          monitoringSettings, new XPackLicenseState(), client);

        final Collection<MonitoringDoc> monitoringDocs = collector.collect();

        // just one cluster state
        assertThat(monitoringDocs, hasSize(1));

        // get the cluster state document that we fetched
        final ClusterStateMonitoringDoc clusterStateDoc = (ClusterStateMonitoringDoc)monitoringDocs.iterator().next();

        assertThat(clusterStateDoc.getClusterState(), notNullValue());
        assertThat(clusterStateDoc.getStatus(), notNullValue());

        // turn the monitoring doc into JSON
        final ClusterStateResolver resolver = new ClusterStateResolver(MonitoredSystem.ES, Settings.EMPTY);
        final BytesReference jsonBytes = resolver.source(clusterStateDoc, XContentType.JSON);

        // parse the JSON to figure out what we just did
        final Map<String, Object> fields = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, jsonBytes).map();

        // ensure we did what we wanted
        for (final String filter : ClusterStateResolver.FILTERS) {
            assertContains(filter, fields);
        }
    }

}
