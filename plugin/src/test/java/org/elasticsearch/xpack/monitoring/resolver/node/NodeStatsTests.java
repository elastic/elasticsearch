/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.node;

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
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

    private static Boolean WATCHER_ENABLED = null;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.exporters.default_local.type", LocalExporter.TYPE)
                .build();
    }

    @After
    public void cleanup() throws Exception {
        disableMonitoringInterval();
        wipeMonitoringIndices();
    }

    public void testNodeStats() throws Exception {
        final int numDocs = between(50, 150);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "foo").setSource("value", randomInt()).get();
        }

        flush();
        refresh();

        updateMonitoringInterval(3L, TimeUnit.SECONDS);
        waitForMonitoringIndices();

        awaitMonitoringDocsCountOnPrimary(greaterThan(0L), NodeStatsResolver.TYPE);

        SearchResponse response =
                client().prepareSearch()
                        .setQuery(QueryBuilders.termQuery("type", NodeStatsResolver.TYPE))
                        .setPreference("_primary")
                        .get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.getSourceAsMap();

            for (String filter : nodeStatsFilters()) {
                if (Constants.WINDOWS) {
                    // load average is unavailable on Windows
                    if (filter.startsWith("node_stats.os.cpu.load_average")) {
                        continue;
                    }
                }

                // fs and cgroup stats are only reported on Linux, but it's acceptable for _node/stats to report them as null if the OS is
                //  misconfigured or not reporting them for some reason (e.g., older kernel)
                if (filter.startsWith("node_stats.fs") || filter.startsWith("node_stats.os.cgroup")) {
                    continue;
                }

                // load average is unavailable on macOS for 5m and 15m (but we get 1m), but it's also possible on Linux too
                if ("node_stats.os.cpu.load_average.5m".equals(filter) || "node_stats.os.cpu.load_average.15m".equals(filter)) {
                    continue;
                }

                assertContains(filter, fields);
            }
        }
    }

    /**
     * Optionally exclude {@link NodeStatsResolver#FILTERS} that require Watcher to be enabled.
     *
     * @return Never {@code null} or empty.
     */
    private Set<String> nodeStatsFilters() {
        if (enableWatcher()) {
            return NodeStatsResolver.FILTERS;
        }

        return NodeStatsResolver.FILTERS.stream().filter(s -> s.contains("watcher") == false).collect(Collectors.toSet());
    }

    @Override
    protected boolean enableWatcher() {
        if (WATCHER_ENABLED == null) {
            WATCHER_ENABLED = randomBoolean();
        }
        // currently this is the only Monitoring test that expects Watcher to be enabled.
        // Once this becomes the default, then this should be removed.
        return WATCHER_ENABLED;
    }
}
