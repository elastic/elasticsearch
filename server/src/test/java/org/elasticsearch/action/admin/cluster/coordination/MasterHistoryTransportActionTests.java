/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.coordination.MasterHistory;
import org.elasticsearch.cluster.coordination.MasterHistoryService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MasterHistoryTransportActionTests extends ESTestCase {
    public void testDoExecute() {
        TransportService transportService = mock(TransportService.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        MasterHistoryService masterHistoryService = mock(MasterHistoryService.class);
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.relativeTimeInMillis()).thenReturn(System.currentTimeMillis());
        MasterHistory masterHistory = new MasterHistory(threadPool, clusterService);
        when(masterHistoryService.getLocalMasterHistory()).thenReturn(masterHistory);
        MasterHistoryTransportAction action = new MasterHistoryTransportAction(transportService, actionFilters, masterHistoryService);
        final List<List<DiscoveryNode>> result = new ArrayList<>();
        ActionListener<MasterHistoryAction.Response> listener = new ActionListener<>() {
            @Override
            public void onResponse(MasterHistoryAction.Response response) {
                result.add(response.getMasterHistory());
            }

            @Override
            public void onFailure(Exception e) {
                fail("Not expecting failure");
            }
        };
        action.doExecute(null, new MasterHistoryAction.Request(), listener);
        assertEquals(masterHistory.getImmutableView(), result.get(0));
    }
}
