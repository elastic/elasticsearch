/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.StopILMRequest;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.StopILMAction;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.action.GetSLMStatusAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.StopSLMAction;

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
        return List.of(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
    }

    public void testModeSnapshotRestore() throws Exception {
        client().admin()
            .cluster()
            .preparePutRepository("repo")
            .setType("fs")
            .setSettings(Settings.builder().put("location", "repo").build())
            .get();

        client().execute(
            PutSnapshotLifecycleAction.INSTANCE,
            new PutSnapshotLifecycleAction.Request(
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
            PutLifecycleAction.INSTANCE,
            new PutLifecycleAction.Request(
                new LifecyclePolicy(
                    "ilm-policy",
                    Map.of("warm", new Phase("warm", TimeValue.timeValueHours(1), Map.of("readonly", new ReadOnlyAction())))
                )
            )
        );

        assertThat(ilmMode(), equalTo(OperationMode.RUNNING));
        assertThat(slmMode(), equalTo(OperationMode.RUNNING));

        // Take snapshot

        assertAcked(client().execute(StopILMAction.INSTANCE, new StopILMRequest()).get());
        assertAcked(client().execute(StopSLMAction.INSTANCE, new StopSLMAction.Request()).get());
        assertBusy(() -> assertThat(ilmMode(), equalTo(OperationMode.STOPPED)));
        assertBusy(() -> assertThat(slmMode(), equalTo(OperationMode.STOPPED)));

        // Restore snapshot

        assertBusy(() -> assertThat(ilmMode(), equalTo(OperationMode.STOPPED)));
        assertBusy(() -> assertThat(slmMode(), equalTo(OperationMode.STOPPED)));
    }

    private OperationMode ilmMode() throws Exception {
        return client().execute(GetStatusAction.INSTANCE, new GetStatusAction.Request()).get().getMode();
    }

    private OperationMode slmMode() throws Exception {
        return client().execute(GetSLMStatusAction.INSTANCE, new GetSLMStatusAction.Request()).get().getOperationMode();
    }
}
