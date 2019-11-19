/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction.Request;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class SnapshotLifecycleInitialisationTests extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put(XPackSettings.INDEX_LIFECYCLE_ENABLED.getKey(), false);
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(XPackSettings.LOGSTASH_ENABLED.getKey(), false);

        settings.put(XPackSettings.SNAPSHOT_LIFECYCLE_ENABLED.getKey(), true);
        return settings.build();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
    }

    public void testSLMIsInRunningModeWhenILMIsDisabled() throws Exception {
        internalCluster().startNode();

        client().execute(PutRepositoryAction.INSTANCE,
            new PutRepositoryRequest().name("repo").type("fs").settings(Settings.builder().put("repositories.fs.location",
                randomRepoPath()).build())
        ).get(10, TimeUnit.SECONDS);

        client().execute(PutSnapshotLifecycleAction.INSTANCE,
            new Request("snapshot-policy", new SnapshotLifecyclePolicy("test-policy", "snap",
                "*/1 * * * * ?", "repo", Collections.emptyMap(), SnapshotRetentionConfiguration.EMPTY))
        ).get(10, TimeUnit.SECONDS);

        ClusterState state = clusterService().state();
        SnapshotLifecycleMetadata snapMeta = state.metaData().custom(SnapshotLifecycleMetadata.TYPE);
        assertThat(snapMeta.getOperationMode(), is(OperationMode.RUNNING));
    }
}
