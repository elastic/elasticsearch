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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
        action = new TransportReadinessAction(
            new ActionFilters(Set.of()),
            null,
            readinessService
        );
    }

    @After
    public void cleanupResources() {
        readinessService.stop();
        readinessService.close();
        threadPool.shutdownNow();
    }

    public void setReady() {
        readinessService.start();
        readinessService.startListener();
    }

    private AtomicReference<ReadinessResponse> runAction() {
        AtomicReference<ReadinessResponse> responseRef = new AtomicReference<>();
        action.execute(null, new ReadinessRequest(), new ActionListener<>() {
            @Override
            public void onResponse(ReadinessResponse response) {
                assertThat(response.isReady(), is(true));
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        return responseRef;
    }

    public void testReadyImmediately() {
        setReady();
        AtomicReference<ReadinessResponse> responseRef = runAction();
        assertThat(responseRef.get(), notNullValue());
    }

    public void testReadyEventually() {
        AtomicReference<ReadinessResponse> responseRef = runAction();
        assertThat(responseRef.get(), nullValue());
        setReady();
        assertThat(responseRef.get(), notNullValue());
    }
}
