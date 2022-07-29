/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction.Request;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;

public class SnapshotLifecycleInitialisationTests extends ESSingleNodeTestCase {

    private static Path repositoryLocation;

    @BeforeClass
    public static void setupRepositoryPath() {
        repositoryLocation = createTempDir();
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings());
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(Environment.PATH_REPO_SETTING.getKey(), repositoryLocation);
        return settings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
    }

    public void testSLMIsInRunningModeWhenILMIsDisabled() throws Exception {
        client().execute(
            PutRepositoryAction.INSTANCE,
            new PutRepositoryRequest().name("repo")
                .type("fs")
                .settings(Settings.builder().put("repositories.fs.location", repositoryLocation).build())
        ).get(10, TimeUnit.SECONDS);

        client().execute(
            PutSnapshotLifecycleAction.INSTANCE,
            new Request(
                "snapshot-policy",
                new SnapshotLifecyclePolicy(
                    "test-policy",
                    "snap",
                    "0 0/15 * * * ?",
                    "repo",
                    Collections.emptyMap(),
                    SnapshotRetentionConfiguration.EMPTY
                )
            )
        ).get(10, TimeUnit.SECONDS);

        ClusterState state = getInstanceFromNode(ClusterService.class).state();
        SnapshotLifecycleMetadata snapMeta = state.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        assertThat(snapMeta.getOperationMode(), is(OperationMode.RUNNING));
    }
}
