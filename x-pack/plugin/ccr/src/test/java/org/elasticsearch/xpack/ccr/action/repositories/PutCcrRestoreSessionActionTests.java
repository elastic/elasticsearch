/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class PutCcrRestoreSessionActionTests extends ESTestCase {
    public void testPrivilegeForActions() {
        // Indices action is granted by the all privilege
        assertThat(IndexPrivilege.MANAGE.predicate().test(PutCcrRestoreSessionAction.NAME), is(false));
        assertThat(IndexPrivilege.READ.predicate().test(PutCcrRestoreSessionAction.NAME), is(false));
        assertThat(IndexPrivilege.ALL.predicate().test(PutCcrRestoreSessionAction.NAME), is(true));

        // Internal action is granted by neither regular indices nor cluster privileges
        assertThat(IndexPrivilege.ALL.predicate().test(PutCcrRestoreSessionAction.INTERNAL_NAME), is(false));
        assertThat(
            ClusterPrivilegeResolver.ALL.permission()
                .check(PutCcrRestoreSessionAction.INTERNAL_NAME, mock(TransportRequest.class), AuthenticationTestHelper.builder().build()),
            is(false)
        );
    }

    public void testTransportActionNames() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final ClusterService clusterService = mock(ClusterService.class);
        final ActionFilters actionFilters = mock(ActionFilters.class);
        final IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        final TransportService transportService = mock(TransportService.class);
        final IndicesService indicesService = mock(IndicesService.class);
        final CcrRestoreSourceService ccrRestoreSourceService = mock(CcrRestoreSourceService.class);

        final PutCcrRestoreSessionAction.TransportPutCcrRestoreSessionAction action1 = new PutCcrRestoreSessionAction.TransportAction(
            threadPool,
            clusterService,
            actionFilters,
            indexNameExpressionResolver,
            transportService,
            indicesService,
            ccrRestoreSourceService
        );
        assertThat(action1.actionName, equalTo(PutCcrRestoreSessionAction.NAME));

        final PutCcrRestoreSessionAction.TransportPutCcrRestoreSessionAction action2 =
            new PutCcrRestoreSessionAction.InternalTransportAction(
                threadPool,
                clusterService,
                actionFilters,
                indexNameExpressionResolver,
                transportService,
                indicesService,
                ccrRestoreSourceService
            );
        assertThat(action2.actionName, equalTo(PutCcrRestoreSessionAction.INTERNAL_NAME));
    }
}
