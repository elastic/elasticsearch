/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.ml.LocalStateMachineLearning;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

public class AnnotationIndexIT extends MlSingleNodeTestCase {

    @Override
    protected Settings nodeSettings()  {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());
        newSettings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        return newSettings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(LocalStateMachineLearning.class);
    }

    // TODO remove this when the jindex feature branches are merged, as this is in the base class then
    @Before
    public void waitForMlTemplates() throws Exception {
        // Block until the templates are installed
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertTrue("Timed out waiting for the ML templates to be installed",
                MachineLearning.allTemplatesInstalled(state));
        });
    }

    public void testNotCreatedWhenNoOtherMlIndices() {

        // Ask a few times to increase the chance of failure if the .ml-annotations index is created when no other ML index exists
        for (int i = 0; i < 10; ++i) {
            assertFalse(annotationsIndexExists());
            assertEquals(0, numberOfAnnotationsAliases());
        }
    }

    public void testCreatedWhenAfterOtherMlIndex() throws Exception {

        Auditor auditor = new Auditor(client(), "node_1");
        auditor.info("whatever", "blah");

        // Creating a document in the .ml-notifications index should cause .ml-annotations
        // to be created, as it should get created as soon as any other ML index exists

        assertBusy(() -> {
            assertTrue(annotationsIndexExists());
            assertEquals(2, numberOfAnnotationsAliases());
        });
    }

    private boolean annotationsIndexExists() {
        return client().admin().indices().prepareExists(AnnotationIndex.INDEX_NAME).get().isExists();
    }

    private int numberOfAnnotationsAliases() {
        int count = 0;
        ImmutableOpenMap<String, List<AliasMetaData>> aliases = client().admin().indices()
            .prepareGetAliases(AnnotationIndex.READ_ALIAS_NAME, AnnotationIndex.WRITE_ALIAS_NAME).get().getAliases();
        if (aliases != null) {
            for (ObjectObjectCursor<String, List<AliasMetaData>> entry : aliases) {
                count += entry.value.size();
            }
        }
        return count;
    }
}
