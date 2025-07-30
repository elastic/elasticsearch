/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.SetUpgradeModeActionRequest;
import org.elasticsearch.xpack.core.action.SetUpgradeModeActionResponse;
import org.elasticsearch.xpack.core.transform.action.SetTransformUpgradeModeAction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransformTests extends ESTestCase {
    public void testSetTransformUpgradeMode() {
        testUpgradeMode(true);
    }

    public void testIgnoreSetTransformUpgradeMode() {
        testUpgradeMode(false);
    }

    private void testUpgradeMode(boolean alreadyInUpgradeMode) {
        ThreadPool threadPool = new ThreadPool(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), TransformTests.class.getSimpleName()).put(Settings.EMPTY).build(),
            MeterRegistry.NOOP,
            (settings1, allocatedProcessors) -> Map.of()
        ) {
            @Override
            public ExecutorService executor(String name) {
                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
            }

            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor name) {
                command.run();
                return null;
            }
        };
        ClusterService clusterService = mock();
        Client client = mock();
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            ActionListener<SetUpgradeModeActionResponse> listener = invocationOnMock.getArgument(2);
            listener.onResponse(new SetUpgradeModeActionResponse(true, alreadyInUpgradeMode));
            return Void.TYPE;
        }).when(client).execute(same(SetTransformUpgradeModeAction.INSTANCE), any(), any());

        try (var transformPlugin = new Transform(Settings.EMPTY)) {
            SetOnce<Map<String, Object>> response = new SetOnce<>();
            transformPlugin.prepareForIndicesMigration(clusterService, client, ActionTestUtils.assertNoFailureListener(response::set));

            assertThat(response.get(), equalTo(Collections.singletonMap("already_in_upgrade_mode", alreadyInUpgradeMode)));
            verify(client).execute(same(SetTransformUpgradeModeAction.INSTANCE), eq(new SetUpgradeModeActionRequest(true)), any());

            transformPlugin.indicesMigrationComplete(
                response.get(),
                clusterService,
                client,
                ActionTestUtils.assertNoFailureListener(ESTestCase::assertTrue)
            );

            var timesCalled = alreadyInUpgradeMode ? never() : times(1);
            verify(client, timesCalled).execute(
                same(SetTransformUpgradeModeAction.INSTANCE),
                eq(new SetUpgradeModeActionRequest(false)),
                any()
            );
        } finally {
            terminate(threadPool);
        }
    }
}
