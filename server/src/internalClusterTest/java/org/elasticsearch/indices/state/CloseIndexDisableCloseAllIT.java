/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.state;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class CloseIndexDisableCloseAllIT extends ESIntegTestCase {

    @After
    public void afterTest() {
        Settings settings = Settings.builder()
            .put(TransportCloseIndexAction.CLUSTER_INDICES_CLOSE_ENABLE_SETTING.getKey(), (String) null)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
    }

    public void testCloseAllRequiresName() {
        createIndex("test1", "test2", "test3");

        assertAcked(client().admin().indices().prepareClose("test3", "test2"));
        assertIndexIsClosed("test2", "test3");

        // disable closing
        createIndex("test_no_close");
        Settings settings = Settings.builder().put(TransportCloseIndexAction.CLUSTER_INDICES_CLOSE_ENABLE_SETTING.getKey(), false).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));

        IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> client().admin().indices().prepareClose("test_no_close").get()
        );
        assertEquals(
            illegalStateException.getMessage(),
            "closing indices is disabled - set [cluster.indices.close.enable: true] to enable it. NOTE: closed indices still "
                + "consume a significant amount of diskspace"
        );
    }

    private void assertIndexIsClosed(String... indices) {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
        for (String index : indices) {
            IndexMetadata indexMetadata = clusterStateResponse.getState().metadata().indices().get(index);
            assertNotNull(indexMetadata);
            assertEquals(IndexMetadata.State.CLOSE, indexMetadata.getState());
        }
    }
}
