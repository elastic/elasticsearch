/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.StopILMRequest;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.GetSLMStatusAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.StopSLMAction;
import org.elasticsearch.xpack.slm.SnapshotLifecycle;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadataTests.randomRetention;
import static org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadataTests.randomSchedule;
import static org.hamcrest.Matchers.equalTo;

public class LifecycleOperationSnapshotTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class, SnapshotLifecycle.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("slm.history_index_enabled", false).build();
    }

    public void testModeSnapshotRestore() throws Exception {
        clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "repo")
            .setType("fs")
            .setSettings(Settings.builder().put("location", "repo").build())
            .get();

        client().execute(
            PutSnapshotLifecycleAction.INSTANCE,
            new PutSnapshotLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "slm-policy",
                new SnapshotLifecyclePolicy(
                    "slm-policy",
                    randomAlphaOfLength(4).toLowerCase(Locale.ROOT),
                    randomSchedule(),
                    "repo",
                    null,
                    randomRetention()
                )
            )
        ).get();

        client().execute(
            ILMActions.PUT,
            new PutLifecycleRequest(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new LifecyclePolicy(
                    "ilm-policy",
                    Map.of("warm", new Phase("warm", TimeValue.timeValueHours(1), Map.of("readonly", new ReadOnlyAction())))
                )
            )
        );

        assertThat(ilmMode(), equalTo(OperationMode.RUNNING));
        assertThat(slmMode(), equalTo(OperationMode.RUNNING));

        // Take snapshot
        ExecuteSnapshotLifecycleAction.Response resp = client().execute(
            ExecuteSnapshotLifecycleAction.INSTANCE,
            new ExecuteSnapshotLifecycleAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "slm-policy")
        ).get();
        final String snapshotName = resp.getSnapshotName();
        // Wait for the snapshot to be successful
        assertBusy(() -> {
            logger.info("--> checking for snapshot success");
            try {
                GetSnapshotsResponse getResp = client().execute(
                    TransportGetSnapshotsAction.TYPE,
                    new GetSnapshotsRequest(TEST_REQUEST_TIMEOUT, new String[] { "repo" }, new String[] { snapshotName })
                ).get();
                assertThat(getResp.getSnapshots().size(), equalTo(1));
                assertThat(getResp.getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
            } catch (Exception e) {
                fail("snapshot does not yet exist");
            }
        });

        assertAcked(client().execute(ILMActions.STOP, new StopILMRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)).get());
        assertAcked(client().execute(StopSLMAction.INSTANCE, new StopSLMAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)).get());
        assertBusy(() -> assertThat(ilmMode(), equalTo(OperationMode.STOPPED)));
        assertBusy(() -> assertThat(slmMode(), equalTo(OperationMode.STOPPED)));

        // Restore snapshot
        client().execute(
            TransportRestoreSnapshotAction.TYPE,
            new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", snapshotName).includeGlobalState(true)
                .indices(Strings.EMPTY_ARRAY)
                .waitForCompletion(true)
        ).get();

        assertBusy(() -> assertThat(ilmMode(), equalTo(OperationMode.STOPPED)));
        assertBusy(() -> assertThat(slmMode(), equalTo(OperationMode.STOPPED)));
    }

    private OperationMode ilmMode() throws Exception {
        return client().execute(GetStatusAction.INSTANCE, new GetStatusAction.Request(TEST_REQUEST_TIMEOUT)).get().getMode();
    }

    private OperationMode slmMode() throws Exception {
        return client().execute(GetSLMStatusAction.INSTANCE, new AcknowledgedRequest.Plain(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
            .get()
            .getOperationMode();
    }
}
