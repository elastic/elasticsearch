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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MasterHistoryTransportActionTests extends ESTestCase {
    public void testDoExecute() {
        TransportService transportService = mock(TransportService.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        MasterHistoryService masterHistoryService = mock(MasterHistoryService.class);
        ClusterService clusterService = mock(ClusterService.class);
        MasterHistory masterHistory = new MasterHistory(clusterService);
        when(masterHistoryService.getLocalMasterHistory()).thenReturn(masterHistory);
        MasterHistoryTransportAction action = new MasterHistoryTransportAction(transportService, actionFilters, masterHistoryService);
        final MasterHistory[] result = new MasterHistory[1];
        ActionListener<MasterHistoryAction.Response> listener = new ActionListener<>() {
            @Override
            public void onResponse(MasterHistoryAction.Response response) {
                result[0] = response.getMasterHistory();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Not expecting failure");
            }
        };
        action.doExecute(null, new MasterHistoryAction.Request(), listener);
        assertEquals(masterHistory, result[0]);
    }
}
