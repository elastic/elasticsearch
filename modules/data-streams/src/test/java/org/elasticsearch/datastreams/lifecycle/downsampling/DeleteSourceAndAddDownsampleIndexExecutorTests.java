/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.downsampling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener.rerouteCompletionIsNotRequired;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DeleteSourceAndAddDownsampleIndexExecutorTests extends ESTestCase {

    public void testExecutorNotifiesListenerAndReroutesAllocationService() {
        final var projectId = randomUniqueProjectId();
        String dataStreamName = randomAlphaOfLengthBetween(10, 100);
        String sourceIndex = randomAlphaOfLengthBetween(10, 100);
        String downsampleIndex = randomAlphaOfLengthBetween(10, 100);

        AllocationService allocationService = mock(AllocationService.class);
        DeleteSourceAndAddDownsampleIndexExecutor executor = new DeleteSourceAndAddDownsampleIndexExecutor(allocationService);

        AtomicBoolean taskListenerCalled = new AtomicBoolean(false);
        executor.taskSucceeded(
            new DeleteSourceAndAddDownsampleToDS(
                Settings.EMPTY,
                projectId,
                dataStreamName,
                sourceIndex,
                downsampleIndex,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        taskListenerCalled.set(true);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(e.getMessage(), e);
                        fail("unexpected exception: " + e.getMessage());
                    }
                }
            ),
            null
        );
        assertThat(taskListenerCalled.get(), is(true));

        ClusterState state = ClusterState.EMPTY_STATE;
        executor.afterBatchExecution(state, true);
        verify(allocationService).reroute(state, "deleted indices", rerouteCompletionIsNotRequired());
    }
}
