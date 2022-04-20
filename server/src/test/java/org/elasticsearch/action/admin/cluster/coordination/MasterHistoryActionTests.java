/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.cluster.coordination.ImmutableMasterHistory;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Collections;

import static org.mockito.Mockito.mock;

public class MasterHistoryActionTests extends ESTestCase {
    public void testSerialization() {
        var clusterService = mock(ClusterService.class);
        ImmutableMasterHistory masterHistory = new ImmutableMasterHistory(Collections.emptyList());
        MasterHistoryAction.Response response = new MasterHistoryAction.Response(masterHistory);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            response,
            history -> copyWriteable(history, writableRegistry(), MasterHistoryAction.Response::new)
        );

        MasterHistoryAction.Request request = new MasterHistoryAction.Request();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            request,
            history -> copyWriteable(history, writableRegistry(), MasterHistoryAction.Request::new)
        );
    }
}
