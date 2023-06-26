/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.util.Collections;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;

public class TransportDeleteRoleMappingActionTests extends ESTestCase {
    public void testReservedStateHandler() {
        var store = mock(NativeRoleMappingStore.class);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        var action = new TransportDeleteRoleMappingAction(mock(ActionFilters.class), transportService, mock(ClusterService.class), store);

        assertEquals(ReservedRoleMappingAction.NAME, action.reservedStateHandlerName().get());

        var deleteRequest = new DeleteRoleMappingRequest();
        deleteRequest.setName("kibana_all");
        assertThat(action.modifiedKeys(deleteRequest), containsInAnyOrder("kibana_all"));
    }
}
