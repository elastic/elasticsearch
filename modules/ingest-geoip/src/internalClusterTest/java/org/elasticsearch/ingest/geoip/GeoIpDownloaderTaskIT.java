/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class GeoIpDownloaderTaskIT extends AbstractGeoIpIT {

    protected static final String ENDPOINT = System.getProperty("geoip_endpoint");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, IngestGeoIpPlugin.class, IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), "http://invalid-endpoint")
            .build();
    }

    @After
    public void cleanUp() throws Exception {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .putNull(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey())
                        .putNull(GeoIpDownloader.POLL_INTERVAL_SETTING.getKey())
                        .putNull("ingest.geoip.database_validity")
                )
                .get()
        );
    }

    public void testTaskRemovedAfterCancellation() throws Exception {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true))
                .get()
        );
        assertBusy(() -> {
            PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> task = getTask();
            assertNotNull(task);
            assertTrue(task.isAssigned());
        });
        assertBusy(() -> {
            ListTasksResponse tasks = client().admin()
                .cluster()
                .listTasks(new ListTasksRequest().setActions("geoip-downloader[c]"))
                .actionGet();
            assertEquals(1, tasks.getTasks().size());
        });
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), false))
                .get()
        );
        assertBusy(() -> {
            ListTasksResponse tasks2 = client().admin()
                .cluster()
                .listTasks(new ListTasksRequest().setActions("geoip-downloader[c]"))
                .actionGet();
            assertEquals(0, tasks2.getTasks().size());
        });
    }

    private PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> getTask() {
        return PersistentTasksCustomMetadata.getTaskWithId(clusterService().state(), GeoIpDownloader.GEOIP_DOWNLOADER);
    }
}
