/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle;

import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.snapshotlifecycle.action.DeleteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.snapshotlifecycle.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycle;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;

public class SnapshotLifecycleIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(XPackClientPlugin.class, IndexLifecycle.class);
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
            .put("client.transport.sniff", false)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, SecurityField.NAME4)
            .put(NetworkModule.HTTP_TYPE_KEY, SecurityField.NAME4)
            .build();
    }

    public void testFullPolicySnapshot() throws Exception {
        final String IDX = "test";
        createIndex(IDX);
        int docCount = randomIntBetween(10, 50);
        List<IndexRequestBuilder> indexReqs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            indexReqs.add(client().prepareIndex(IDX, "_doc", "" + i).setSource("foo", "bar"));
        }
        indexRandom(true, indexReqs);

        Path repo = randomRepoPath();

        // Create a snapshot repo
        assertAcked(client().admin().cluster().preparePutRepository("my-repo").setType("fs").setSettings(Settings.builder()
            .put("location", repo.toAbsolutePath().toString())));

        Map<String, Object> snapConfig = new HashMap<>();
        snapConfig.put("indices", Collections.singletonList(IDX));
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy("test-policy", "snap", "*/1 * * * * ?", "my-repo", snapConfig);

        client().execute(PutSnapshotLifecycleAction.INSTANCE, new PutSnapshotLifecycleAction.Request("test-policy", policy)).get();

        // Check that the snapshot was actually taken
        assertBusy(() -> {
            GetSnapshotsResponse snapResponse = client().admin().cluster().getSnapshots(new GetSnapshotsRequest("my-repo")).get();
            assertThat(snapResponse.getSnapshots().size(), greaterThan(0));
            SnapshotInfo info = snapResponse.getSnapshots().get(0);
            assertThat(info.snapshotId().getName(), startsWith("snap-"));
            assertThat(info.indices(), equalTo(Collections.singletonList(IDX)));
        });

        client().execute(DeleteSnapshotLifecycleAction.INSTANCE, new DeleteSnapshotLifecycleAction.Request("test-policy")).get();
    }
}
