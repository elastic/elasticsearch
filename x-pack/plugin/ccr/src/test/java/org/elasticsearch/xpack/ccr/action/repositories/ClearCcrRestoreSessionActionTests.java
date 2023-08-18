/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClearCcrRestoreSessionActionTests extends ESTestCase {

    public void testPrivilegeForActions() {
        // Indices action is granted by the all privilege
        assertThat(IndexPrivilege.MANAGE.predicate().test(ClearCcrRestoreSessionAction.NAME), is(false));
        assertThat(IndexPrivilege.READ.predicate().test(ClearCcrRestoreSessionAction.NAME), is(false));
        assertThat(IndexPrivilege.ALL.predicate().test(ClearCcrRestoreSessionAction.NAME), is(true));

        // Internal action is granted by neither regular indices nor cluster privileges
        assertThat(IndexPrivilege.ALL.predicate().test(ClearCcrRestoreSessionAction.INTERNAL_NAME), is(false));
        assertThat(
            ClusterPrivilegeResolver.ALL.permission()
                .check(
                    ClearCcrRestoreSessionAction.INTERNAL_NAME,
                    mock(TransportRequest.class),
                    AuthenticationTestHelper.builder().build()
                ),
            is(false)
        );
    }

    public void testActionNames() {
        final ActionFilters actionFilters = mock(ActionFilters.class);
        final TransportService transportService = mock(TransportService.class);
        final CcrRestoreSourceService ccrRestoreSourceService = mock(CcrRestoreSourceService.class);

        final var action = new ClearCcrRestoreSessionAction.TransportAction(actionFilters, transportService, ccrRestoreSourceService);
        assertThat(action.actionName, equalTo(ClearCcrRestoreSessionAction.NAME));

        final var internalAction = new ClearCcrRestoreSessionAction.InternalTransportAction(
            actionFilters,
            transportService,
            ccrRestoreSourceService
        );
        assertThat(internalAction.actionName, equalTo(ClearCcrRestoreSessionAction.INTERNAL_NAME));
    }

    public void testRequestedShardIdMustBeConsistentWithSessionShardId() {
        final ActionFilters actionFilters = mock(ActionFilters.class);
        final TransportService transportService = mock(TransportService.class);
        final CcrRestoreSourceService ccrRestoreSourceService = mock(CcrRestoreSourceService.class);

        final ShardId expectedShardId = mock(ShardId.class);
        final String indexName = randomAlphaOfLengthBetween(3, 8);
        when(expectedShardId.getIndexName()).thenReturn(indexName);

        final IllegalArgumentException e = new IllegalArgumentException("inconsistent shard");
        doAnswer(invocation -> {
            final ShardId requestedShardId = invocation.getArgument(1);
            if (expectedShardId != requestedShardId) {
                throw e;
            } else {
                return null;
            }
        }).when(ccrRestoreSourceService).ensureSessionShardIdConsistency(anyString(), any());

        final var action = new ClearCcrRestoreSessionAction.TransportAction(actionFilters, transportService, ccrRestoreSourceService);

        final String sessionUUID = UUIDs.randomBase64UUID();

        // 1. Requested ShardId is consistent
        final var request1 = new ClearCcrRestoreSessionRequest(sessionUUID, mock(DiscoveryNode.class), expectedShardId);
        assertThat(request1.indices(), arrayContaining(indexName));
        final PlainActionFuture<ActionResponse.Empty> future1 = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request1, future1);
        assertThat(future1.actionGet(), is(ActionResponse.Empty.INSTANCE));

        // 2. Inconsistent requested ShardId
        final var request3 = new ClearCcrRestoreSessionRequest(sessionUUID, mock(DiscoveryNode.class), mock(ShardId.class));
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> action.doExecute(mock(Task.class), request3, new PlainActionFuture<>())),
            is(e)
        );
    }
}
