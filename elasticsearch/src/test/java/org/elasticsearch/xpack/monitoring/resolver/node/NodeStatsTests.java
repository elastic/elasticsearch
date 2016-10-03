/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.node;

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.node.NodeStatsCollector;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.junit.After;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.greaterThan;

// numClientNodes is set to 0 because Client nodes don't have Filesystem stats
@ClusterScope(scope = Scope.TEST, numClientNodes = 0, transportClientRatio = 0.0)
public class NodeStatsTests extends MonitoringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put(MonitoringSettings.COLLECTORS.getKey(), NodeStatsCollector.NAME)
                .put("xpack.monitoring.exporters.default_local.type", LocalExporter.TYPE)
                .build();
    }

    @After
    public void cleanup() throws Exception {
        updateMonitoringInterval(-1, TimeUnit.SECONDS);
        wipeMonitoringIndices();
    }

    public void testNodeStats() throws Exception {
        logger.debug("--> creating some indices for future node stats");
        final int numDocs = between(50, 150);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "foo").setSource("value", randomInt()).get();
        }

        securedFlush();
        securedRefresh();

        updateMonitoringInterval(3L, TimeUnit.SECONDS);
        waitForMonitoringIndices();

        awaitMonitoringDocsCount(greaterThan(0L), NodeStatsResolver.TYPE);

        logger.debug("--> searching for monitoring documents of type [{}]", NodeStatsResolver.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(NodeStatsResolver.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");

        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : nodeStatsFilters(watcherEnabled)) {
                if (Constants.WINDOWS) {
                    // load average is unavailable on Windows
                    if ("node_stats.os.cpu.load_average.1m".equals(filter)) {
                        continue;
                    }
                }
                if (filter.startsWith("node_stats.fs")) {
                    // NORELEASE what is going on here?
                    continue;
                }

                assertContains(filter, fields);
            }
        }

        logger.debug("--> node stats successfully collected");
    }

    /**
     * Optionally exclude {@link NodeStatsResolver#FILTERS} that require Watcher to be enabled.
     *
     * @param includeWatcher {@code true} to keep watcher filters.
     * @return Never {@code null} or empty.
     * @see #watcherEnabled
     */
    private static Set<String> nodeStatsFilters(boolean includeWatcher) {
        if (includeWatcher) {
            return NodeStatsResolver.FILTERS;
        }

        return NodeStatsResolver.FILTERS.stream().filter(s -> s.contains("watcher") == false).collect(Collectors.toSet());
    }

    @Override
    protected boolean enableWatcher() {
        // currently this is the only Monitoring test that expects Watcher to be enabled.
        // Once this becomes the default, then this should be removed.
        return randomBoolean();
    }
}
