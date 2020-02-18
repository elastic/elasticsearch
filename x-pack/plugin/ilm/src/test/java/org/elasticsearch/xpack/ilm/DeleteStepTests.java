/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.AsyncActionStep;
import org.elasticsearch.xpack.core.ilm.DeleteStep;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

public class DeleteStepTests extends ESSingleNodeTestCase {

    public void testDeleteStepRetriesOnError() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put(SETTING_READ_ONLY_ALLOW_DELETE, false).put(SETTING_READ_ONLY, true))
            .get());

        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        ClusterState state = clusterService.state();
        IndexMetaData indexMetaData = state.metaData().index("test");
        ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());

        DeleteStep step = new DeleteStep(new StepKey("delete", "action", "delete"), null, client());
        AtomicBoolean done = new AtomicBoolean();

        step.performAction(indexMetaData, state, observer, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                done.set(true);
                fail("expected the test to fail when updating the setting to an invalid value");
            }

            @Override
            public void onFailure(Exception e) {
                assertAcked(client().admin().indices().prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put(SETTING_READ_ONLY, false))
                    .get());
                step.performAction(indexMetaData, state, observer, new AsyncActionStep.Listener() {
                    @Override
                    public void onResponse(boolean complete) {
                        done.set(true);
                        assertThat(complete, is(true));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        done.set(true);
                        fail("unexpected failure when trying to update setting to a valid value");
                    }
                });
            }
        });

        assertBusy(() -> assertTrue(done.get()));
    }
}
