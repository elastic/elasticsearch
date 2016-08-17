/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.watcher.support.xcontent.ObjectPath;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.watch.WatchStore;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0)
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class WatcherBackwardsCompatibilityTests extends AbstractWatcherIntegrationTestCase {

    private static final String INDEX_NAME = WatchStore.INDEX;
    private static final String TYPE_NAME = WatchStore.DOC_TYPE;

    @Override
    protected boolean enableSecurity() {
        return false;
    }

    @Override
    protected boolean timeWarped() {
        return false;
    }

    public void testWatchLoadedSuccessfullyAfterUpgrade() throws Exception {
        // setup node
        Path dataDir = createTempDir();
        Path clusterDir = Files.createDirectory(dataDir.resolve(cluster().getClusterName()));
        try (InputStream stream = WatcherBackwardsCompatibilityTests.class.
                getResourceAsStream("/bwc_indices/bwc_index_2_3_5.zip")) {
            TestUtil.unzip(stream, clusterDir);
        }

        Settings.Builder nodeSettings = Settings.builder()
                .put(super.nodeSettings(0))
                .put(Environment.PATH_DATA_SETTING.getKey(), dataDir);
        internalCluster().startNode(nodeSettings.build());
        ensureYellow();

        // verify cluster state:
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertThat(state.metaData().indices().size(), equalTo(1)); // only the .watches index
            // (the watch has a very high interval (99 weeks))
            assertThat(state.metaData().indices().get(INDEX_NAME), notNullValue());
            assertThat(state.metaData().indices().get(INDEX_NAME).getCreationVersion(), equalTo(Version.V_2_3_5));
            assertThat(state.metaData().indices().get(INDEX_NAME).getUpgradedVersion(), equalTo(Version.CURRENT));
            assertThat(state.metaData().indices().get(INDEX_NAME).getMappings().size(), equalTo(1));
            assertThat(state.metaData().indices().get(INDEX_NAME).getMappings().get(TYPE_NAME), notNullValue());
        });

        // verify existing watcher documents:
        SearchResponse searchResponse = client().prepareSearch(INDEX_NAME)
            .setTypes(TYPE_NAME)
            .get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("log_error_watch"));

        // Verify that we can get the watch, which means the watch stored in ES 2.3.5 cluster has been successfully
        // loaded with the current version of ES:
        ensureWatcherStarted();
        assertThat(watcherClient().prepareWatcherStats().get().getWatchesCount(), equalTo(1L));
        GetWatchResponse getWatchResponse = watcherClient().prepareGetWatch("log_error_watch").get();
        assertThat(getWatchResponse.isFound(), is(true));
        Map<String, Object> watchSourceAsMap = getWatchResponse.getSource().getAsMap();
        assertThat(ObjectPath.eval("trigger.schedule.interval", watchSourceAsMap), equalTo("99w"));
        assertThat(ObjectPath.eval("input.search.request.body.query.bool.filter.1.range.date.lte", watchSourceAsMap),
                equalTo("{{ctx.trigger.scheduled_time}}"));
        assertThat(ObjectPath.eval("actions.log_error.logging.text", watchSourceAsMap),
                equalTo("Found {{ctx.payload.hits.total}} errors in the logs"));
    }

}
