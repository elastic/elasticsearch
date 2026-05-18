/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.PeerRecoverySourceService.Actions.START_RECOVERY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PeerRecoverySourceServiceTests extends IndexShardTestCase {

    public void testQueuedWhenAtConcurrencyLimit() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService();
        service.start();
        final var task = recoveryTask();

        // Fill both slots
        final var handler1 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary1),
            task,
            primary1,
            ActionListener.noop()
        );
        final var handler2 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary2),
            task,
            primary2,
            ActionListener.noop()
        );
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertNotNull(handler1);
        assertNotNull(handler2);
        assertEquals(0, service.ongoingRecoveries.pendingRecoveriesCount());

        final var queued = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary3),
            task,
            primary3,
            ActionListener.noop()
        );
        assertNull(queued);
        assertEquals(1, service.ongoingRecoveries.pendingRecoveriesCount());
        assertEquals(1, primary3.recoveryStats().currentQueuedAsSource());

        closeShards(primary1, primary2, primary3);
    }

    public void testQueueDrainsWhenSlotFreed() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService();
        service.start();
        final var task = recoveryTask();

        final var handler1 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary1),
            task,
            primary1,
            ActionListener.noop()
        );
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
        assertNotNull(handler1);

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.pendingRecoveriesCount());

        // primary1's slot is freed
        // TODO: intercept recoverToTarget
        service.ongoingRecoveries.onRecoveryComplete(primary1, handler1);
        assertEquals(0, service.ongoingRecoveries.pendingRecoveriesCount());
        assertEquals(0, primary3.recoveryStats().currentQueuedAsSource());

        closeShards(primary1, primary2, primary3);
    }

    public void testQueuedRecoveryCancelledOnShardClose() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService();
        service.start();
        final var task = recoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        // Queue a recovery for primary3
        final AtomicReference<Exception> capturedFailure = new AtomicReference<>();
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary3),
            task,
            primary3,
            ActionListener.wrap(r -> fail("expected failure, not success"), capturedFailure::set)
        );
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.pendingRecoveriesCount());

        // Simulate shard close, pending entry fail
        service.ongoingRecoveries.cancel(primary3);
        assertEquals(0, service.ongoingRecoveries.pendingRecoveriesCount());
        assertEquals(0, primary3.recoveryStats().currentQueuedAsSource());
        assertNotNull(capturedFailure.get());
        assertThat(capturedFailure.get(), instanceOf(DelayRecoveryException.class));
        assertThat(capturedFailure.get().getMessage(), containsString("index shard closed"));

        closeShards(primary1, primary2, primary3);
    }

    public void testQueuedRecoveryCancelledOnNodeLeft() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService();
        service.start();
        final var task = recoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        // Queue a recovery targeting a specific node
        final DiscoveryNode departedNode = getFakeDiscoNode("departing");
        final var requestToDepartingNode = new StartRecoveryRequest(
            primary3.shardId(),
            randomAlphaOfLength(10),
            getFakeDiscoNode("source"),
            departedNode,
            0L,
            Store.MetadataSnapshot.EMPTY,
            randomBoolean(),
            randomLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            true
        );
        final AtomicReference<Exception> capturedFailure = new AtomicReference<>();
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            requestToDepartingNode,
            task,
            primary3,
            ActionListener.wrap(r -> fail("expected failure, not success"), capturedFailure::set)
        );
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.pendingRecoveriesCount());

        // Simulate node departure, pending entry should fail
        service.ongoingRecoveries.cancelOnNodeLeft(departedNode);
        assertEquals(0, service.ongoingRecoveries.pendingRecoveriesCount());
        assertEquals(0, primary3.recoveryStats().currentQueuedAsSource());
        assertNotNull(capturedFailure.get());
        assertThat(capturedFailure.get(), instanceOf(DelayRecoveryException.class));
        assertThat(capturedFailure.get().getMessage(), containsString("target node left"));

        closeShards(primary1, primary2, primary3);
    }

    public void testPendingQueueDrainedOnStop() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService();
        service.start();
        final var task = recoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        // Queue one recovery
        final AtomicReference<Exception> capturedFailure = new AtomicReference<>();
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary3),
            task,
            primary3,
            ActionListener.wrap(r -> fail("expected failure, not success"), capturedFailure::set)
        );
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.pendingRecoveriesCount());

        // Drain the queue as node shutdown would
        service.ongoingRecoveries.cancelAllPendingRecoveries();
        assertEquals(0, service.ongoingRecoveries.pendingRecoveriesCount());
        assertEquals(0, primary3.recoveryStats().currentQueuedAsSource());
        assertNotNull(capturedFailure.get());
        assertThat(capturedFailure.get().getMessage(), containsString("node is closing"));

        closeShards(primary1, primary2, primary3);
    }

    public void testDuplicateRejected() throws IOException {
        final IndexShard primary = newStartedShard(true);
        final var service = newPeerRecoverySourceService();
        service.start();
        final var task = recoveryTask();

        final var request = newStartRecoveryRequest(primary, randomAlphaOfLength(10));
        final var handler = service.ongoingRecoveries.addOrEnqueueNewRecovery(request, task, primary, ActionListener.noop());
        assertNotNull(handler);

        // Same target allocation ID should be rejected
        final var duplicate = new StartRecoveryRequest(
            primary.shardId(),
            request.targetAllocationId(),
            getFakeDiscoNode("source"),
            getFakeDiscoNode("target-dup"),
            0L,
            Store.MetadataSnapshot.EMPTY,
            randomBoolean(),
            randomLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            true
        );
        final var exception = expectThrows(
            DelayRecoveryException.class,
            () -> service.ongoingRecoveries.addOrEnqueueNewRecovery(duplicate, task, primary, ActionListener.noop())
        );
        assertThat(exception.getMessage(), containsString("recovery with same target already registered"));

        service.ongoingRecoveries.remove(primary, handler);

        // Re-adding after removing previous attempt works
        final var handler2 = service.ongoingRecoveries.addOrEnqueueNewRecovery(request, task, primary, ActionListener.noop());
        assertNotNull(handler2);
        service.ongoingRecoveries.remove(primary, handler2);

        closeShards(primary);
    }

    public void testQueuedDuplicateRejected() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService();
        service.start();
        final var task = recoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        // Queue a recovery for primary3 with a known target allocation ID
        final var queuedRequest = newStartRecoveryRequest(primary3);
        service.ongoingRecoveries.addOrEnqueueNewRecovery(queuedRequest, task, primary3, ActionListener.noop());
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.pendingRecoveriesCount());

        // Second request for the same target allocation ID
        final var duplicateRequest = new StartRecoveryRequest(
            primary3.shardId(),
            queuedRequest.targetAllocationId(),
            getFakeDiscoNode("source"),
            getFakeDiscoNode("target-dup"),
            0L,
            Store.MetadataSnapshot.EMPTY,
            randomBoolean(),
            randomLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            true
        );
        final var exception = expectThrows(
            DelayRecoveryException.class,
            () -> service.ongoingRecoveries.addOrEnqueueNewRecovery(duplicateRequest, task, primary3, ActionListener.noop())
        );
        assertThat(exception.getMessage(), containsString("recovery with same target already registered"));

        closeShards(primary1, primary2, primary3);
    }

    public void testActiveHandlerDuplicateRejected() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService();
        service.start();
        final var task = recoveryTask();

        final var activeRequest = newStartRecoveryRequest(primary1);
        service.ongoingRecoveries.addOrEnqueueNewRecovery(activeRequest, task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        final var duplicateOfActive = new StartRecoveryRequest(
            primary3.shardId(),
            activeRequest.targetAllocationId(),
            getFakeDiscoNode("source"),
            getFakeDiscoNode("target-dup"),
            0L,
            Store.MetadataSnapshot.EMPTY,
            randomBoolean(),
            randomLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            true
        );
        final var exception = expectThrows(
            DelayRecoveryException.class,
            () -> service.ongoingRecoveries.addOrEnqueueNewRecovery(duplicateOfActive, task, primary3, ActionListener.noop())
        );
        assertThat(exception.getMessage(), containsString("recovery with same target already registered"));
        assertEquals(0, service.ongoingRecoveries.pendingRecoveriesCount());

        closeShards(primary1, primary2, primary3);
    }

    public void testSameShardFillsMultipleSlots() throws IOException {
        final IndexShard primary = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var service = newPeerRecoverySourceService();
        service.start();
        final var task = recoveryTask();

        // Two handlers for the same shard each consume one slot
        final var handler1 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary),
            task,
            primary,
            ActionListener.noop()
        );
        final var handler2 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary),
            task,
            primary,
            ActionListener.noop()
        );
        assertNotNull(handler1);
        assertNotNull(handler2);
        assertEquals(0, service.ongoingRecoveries.pendingRecoveriesCount());

        // Third request queues
        final var queued = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary2),
            task,
            primary2,
            ActionListener.noop()
        );
        assertNull(queued);
        assertEquals(1, service.ongoingRecoveries.pendingRecoveriesCount());

        closeShards(primary, primary2);
    }

    private PeerRecoverySourceService newPeerRecoverySourceService() {
        final var indicesService = mock(IndicesService.class);
        final var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(NodeRoles.dataNode());
        when(indicesService.clusterService()).thenReturn(clusterService);
        final TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        return new PeerRecoverySourceService(
            transportService,
            indicesService,
            clusterService,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(RecoveryPlannerService.class)
        );
    }

    private StartRecoveryRequest newStartRecoveryRequest(IndexShard shard) {
        return newStartRecoveryRequest(shard, randomAlphaOfLength(10));
    }

    private StartRecoveryRequest newStartRecoveryRequest(IndexShard shard, String allocationId) {
        return new StartRecoveryRequest(
            shard.shardId(),
            allocationId,
            getFakeDiscoNode("source"),
            getFakeDiscoNode("target-" + randomAlphaOfLength(5)),
            0L,
            Store.MetadataSnapshot.EMPTY,
            randomBoolean(),
            randomLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            true
        );
    }

    private Task recoveryTask() {
        return new Task(randomNonNegativeLong(), "test", START_RECOVERY, "", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }
}
