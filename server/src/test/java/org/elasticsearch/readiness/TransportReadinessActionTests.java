/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.readiness;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportReadinessActionTests extends ESTestCase {

    ThreadPool threadPool;
    ClusterService clusterService;
    Environment env;
    ReadinessService readinessService;
    TransportReadinessAction action;

    @Before
    public void setupAction() {
        threadPool = new TestThreadPool("readiness_service_tests");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            null
        );
        env = newEnvironment(Settings.builder().put(ReadinessService.PORT.getKey(), 0).build());
        readinessService = new ReadinessService(clusterService, env);
        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getTaskManager()).thenReturn(mock(TaskManager.class));
        action = new TransportReadinessAction(new ActionFilters(Set.of()), transportService, readinessService);
    }

    @After
    public void cleanupResources() {
        readinessService.stop();
        readinessService.close();
        threadPool.shutdownNow();
    }

    private void setReady() {
        readinessService.start();
        readinessService.startListener();
    }

    private AtomicBoolean runAction() {
        AtomicBoolean responseReceived = new AtomicBoolean();
        action.execute(null, new ReadinessRequest(), new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty response) {
                responseReceived.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        return responseReceived;
    }

    public void testReadyImmediately() {
        setReady();
        AtomicBoolean responseReceived = runAction();
        assertThat(responseReceived.get(), is(true));
    }

    public void testReadyEventually() {
        AtomicBoolean responseReceived = runAction();
        assertThat(responseReceived.get(), is(false));
        setReady();
        assertThat(responseReceived.get(), is(true));
    }
}
