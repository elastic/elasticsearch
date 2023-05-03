/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
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

public class GetCcrRestoreFileChunkActionTests extends ESTestCase {

    public void testPrivilegeForActions() {
        // Indices action is granted by the all privilege
        assertThat(IndexPrivilege.MANAGE.predicate().test(GetCcrRestoreFileChunkAction.NAME), is(false));
        assertThat(IndexPrivilege.READ.predicate().test(GetCcrRestoreFileChunkAction.NAME), is(false));
        assertThat(IndexPrivilege.ALL.predicate().test(GetCcrRestoreFileChunkAction.NAME), is(true));

        // Internal action is granted by neither regular indices nor cluster privileges
        assertThat(IndexPrivilege.ALL.predicate().test(GetCcrRestoreFileChunkAction.INTERNAL_NAME), is(false));
        assertThat(
            ClusterPrivilegeResolver.ALL.permission()
                .check(
                    GetCcrRestoreFileChunkAction.INTERNAL_NAME,
                    mock(TransportRequest.class),
                    AuthenticationTestHelper.builder().build()
                ),
            is(false)
        );
    }

    public void testActionNames() {
        final ActionFilters actionFilters = mock(ActionFilters.class);
        final BigArrays bigArrays = mock(BigArrays.class);
        final TransportService transportService = mock(TransportService.class);
        final CcrRestoreSourceService ccrRestoreSourceService = mock(CcrRestoreSourceService.class);

        final var action = new GetCcrRestoreFileChunkAction.TransportAction(
            bigArrays,
            transportService,
            actionFilters,
            ccrRestoreSourceService
        );
        assertThat(action.actionName, equalTo(GetCcrRestoreFileChunkAction.NAME));

        final var internalAction = new GetCcrRestoreFileChunkAction.InternalTransportAction(
            bigArrays,
            transportService,
            actionFilters,
            ccrRestoreSourceService
        );
        assertThat(internalAction.actionName, equalTo(GetCcrRestoreFileChunkAction.INTERNAL_NAME));
    }

    public void testRequestedShardIdMustBeConsistentWithSessionShardId() {
        final ActionFilters actionFilters = mock(ActionFilters.class);
        final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), ByteSizeValue.ofBytes(1024));
        final TransportService transportService = mock(TransportService.class);
        final CcrRestoreSourceService ccrRestoreSourceService = mock(CcrRestoreSourceService.class);

        final String sessionUUID = UUIDs.randomBase64UUID();
        final CcrRestoreSourceService.SessionReader sessionReader = mock(CcrRestoreSourceService.SessionReader.class);
        when(ccrRestoreSourceService.getSessionReader(sessionUUID)).thenReturn(sessionReader);

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

        final var action = new GetCcrRestoreFileChunkAction.TransportAction(
            bigArrays,
            transportService,
            actionFilters,
            ccrRestoreSourceService
        );

        final String expectedFileName = randomAlphaOfLengthBetween(3, 12);
        final int size = randomIntBetween(1, 42);

        // 1. Requested ShardId is consistent
        final var request1 = new GetCcrRestoreFileChunkRequest(
            mock(DiscoveryNode.class),
            sessionUUID,
            expectedFileName,
            size,
            expectedShardId
        );
        assertThat(request1.indices(), arrayContaining(indexName));
        final PlainActionFuture<GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse> future1 = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request1, future1);
        // The actual response content does not matter as long as the future executes without any error
        future1.actionGet().decRef();

        // 2. Inconsistent requested ShardId
        final var request2 = new GetCcrRestoreFileChunkRequest(
            mock(DiscoveryNode.class),
            sessionUUID,
            expectedFileName,
            size,
            mock(ShardId.class)
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> action.doExecute(mock(Task.class), request2, new PlainActionFuture<>())),
            is(e)
        );

        // 3. Unknown file name
        final IllegalArgumentException e3 = new IllegalArgumentException("invalid fileName");
        doAnswer(invocation -> {
            final String requestedFileName = invocation.getArgument(1);
            if (false == expectedFileName.equals(requestedFileName)) {
                throw e3;
            } else {
                return null;
            }
        }).when(ccrRestoreSourceService).ensureFileNameIsKnownToSession(anyString(), anyString());
        final var request3 = new GetCcrRestoreFileChunkRequest(
            mock(DiscoveryNode.class),
            sessionUUID,
            randomValueOtherThan(expectedFileName, () -> randomAlphaOfLengthBetween(3, 12)),
            size,
            expectedShardId
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> action.doExecute(mock(Task.class), request3, new PlainActionFuture<>())),
            is(e3)
        );
    }
}
