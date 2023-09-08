/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;

public class TransportDeleteLifecycleActionTests extends ESTestCase {
    public void testReservedStateHandler() {
        TransportDeleteLifecycleAction putAction = new TransportDeleteLifecycleAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class)
        );
        assertEquals(ReservedLifecycleAction.NAME, putAction.reservedStateHandlerName().get());

        DeleteLifecycleAction.Request request = new DeleteLifecycleAction.Request("my_timeseries_lifecycle2");
        assertThat(putAction.modifiedKeys(request), containsInAnyOrder("my_timeseries_lifecycle2"));
    }
}
