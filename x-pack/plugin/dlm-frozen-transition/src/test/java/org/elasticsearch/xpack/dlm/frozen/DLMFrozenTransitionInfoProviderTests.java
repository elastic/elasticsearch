/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.datastreams.lifecycle.ExplainIndexFrozenTransition;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DLMFrozenTransitionInfoProviderTests extends DLMFrozenTransitionExecutorTestCase {

    @Before
    public void setUpExecutor() throws Exception {
        setupExecutorTestCase();
    }

    @After
    public void tearDownExecutor() throws Exception {
        tearDownExecutorTestCase();
    }

    public void testInfoAvailableTrue() {
        DLMFrozenTransitionPlugin plugin = new DLMFrozenTransitionPlugin();
        DLMFrozenTransitionInfoProvider provider = new DLMFrozenTransitionInfoProvider(plugin);
        assertThat(provider.infoAvailable(), is(true));
    }

    public void testReturnsNotStartedWhenExecutorNotYetCreated() {
        DLMFrozenTransitionPlugin plugin = new DLMFrozenTransitionPlugin();
        DLMFrozenTransitionInfoProvider provider = new DLMFrozenTransitionInfoProvider(plugin);

        assertThat(provider.getTransitionStatus(ProjectId.DEFAULT, "some-index"), equalTo(ExplainIndexFrozenTransition.Status.NOT_STARTED));
    }

    public void testDelegatesToExecutorOnceCreated() throws Exception {
        DLMFrozenTransitionPlugin plugin = new DLMFrozenTransitionPlugin();
        try (var handle = newExecutor(1, 10)) {
            var executor = handle.executor();
            plugin.setTransitionExecutorForTesting(executor);
            DLMFrozenTransitionInfoProvider provider = new DLMFrozenTransitionInfoProvider(plugin);

            assertThat(
                provider.getTransitionStatus(ProjectId.DEFAULT, "never-submitted"),
                equalTo(ExplainIndexFrozenTransition.Status.NOT_STARTED)
            );

            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch block = new CountDownLatch(1);
            var task = new TestDLMFrozenTransitionRunnable("running-index");
            task.started = started;
            task.blockUntil = block;
            executor.submit(task);
            safeAwait(started);

            assertThat(
                provider.getTransitionStatus(ProjectId.DEFAULT, "running-index"),
                equalTo(ExplainIndexFrozenTransition.Status.RUNNING)
            );

            block.countDown();
        }
    }
}
