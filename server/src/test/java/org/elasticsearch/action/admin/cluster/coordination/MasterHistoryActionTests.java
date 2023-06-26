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
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MasterHistoryActionTests extends ESTestCase {
    public void testSerialization() {
        List<DiscoveryNode> masterHistory = List.of(DiscoveryNodeUtils.create("_id1"), DiscoveryNodeUtils.create("_id2"));
        MasterHistoryAction.Response response = new MasterHistoryAction.Response(masterHistory);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            response,
            history -> copyWriteable(history, writableRegistry(), MasterHistoryAction.Response::new),
            this::mutateMasterHistoryResponse
        );

        MasterHistoryAction.Request request = new MasterHistoryAction.Request();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            request,
            history -> copyWriteable(history, writableRegistry(), MasterHistoryAction.Request::new)
        );
    }

    private MasterHistoryAction.Response mutateMasterHistoryResponse(MasterHistoryAction.Response originalResponse) {
        List<DiscoveryNode> nodes = originalResponse.getMasterHistory();
        switch (randomIntBetween(1, 4)) {
            case 1 -> {
                List<DiscoveryNode> newNodes = new ArrayList<>(nodes);
                newNodes.add(DiscoveryNodeUtils.create("_id3"));
                return new MasterHistoryAction.Response(newNodes);
            }
            case 2 -> {
                List<DiscoveryNode> newNodes = new ArrayList<>(nodes);
                newNodes.remove(0);
                return new MasterHistoryAction.Response(newNodes);
            }
            case 3 -> {
                List<DiscoveryNode> newNodes = new ArrayList<>(nodes);
                newNodes.remove(0);
                newNodes.add(0, DiscoveryNodeUtils.create("_id1"));
                return new MasterHistoryAction.Response(newNodes);
            }
            case 4 -> {
                List<DiscoveryNode> newNodes = new ArrayList<>(nodes);
                newNodes.remove(0);
                newNodes.add(0, null);
                return new MasterHistoryAction.Response(newNodes);
            }
            default -> throw new IllegalStateException();
        }
    }

    public void testTransportDoExecute() {
        TransportService transportService = mock(TransportService.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        MasterHistoryService masterHistoryService = mock(MasterHistoryService.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.relativeTimeInMillis()).thenReturn(System.currentTimeMillis());
        MasterHistory masterHistory = new MasterHistory(threadPool, clusterService);
        when(masterHistoryService.getLocalMasterHistory()).thenReturn(masterHistory);
        MasterHistoryAction.TransportAction action = new MasterHistoryAction.TransportAction(
            transportService,
            actionFilters,
            masterHistoryService
        );
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
        assertEquals(masterHistory.getNodes(), result.get(0));
    }
}
