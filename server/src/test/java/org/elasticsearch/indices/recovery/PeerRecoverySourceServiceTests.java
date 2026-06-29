/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.PeerRecoverySourceService.Actions.START_RECOVERY;
import static org.elasticsearch.indices.recovery.PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PeerRecoverySourceServiceTests extends IndexShardTestCase {

    public void testQueuedWhenAtConcurrencyLimit() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();

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
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

        final var queued = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary3),
            task,
            primary3,
            ActionListener.noop()
        );
        assertNull(queued);
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());
        assertEquals(1, primary3.recoveryStats().currentAsSourceQueued());

        closeShards(primary1, primary2, primary3);
    }

    public void testQueueProcessesNextQueuedWhenSlotFreed() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();

        final var handler1 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary1),
            task,
            primary1,
            ActionListener.noop()
        );
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
        assertNotNull(handler1);

        final var completedListener = new CountDownLatch(1);
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary3),
            task,
            primary3,
            // The recovery will fail immediately because the fake target allocation ID is not in the shard's routing table
            ActionListener.wrap(ignored -> fail("unexpected success"), exception -> {
                assertThat(exception, instanceOf(DelayRecoveryException.class));
                completedListener.countDown();
            })
        );
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        service.ongoingRecoveries.onRecoveryComplete(primary1, handler1);
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
        assertEquals(0, primary3.recoveryStats().currentAsSourceQueued());
        safeAwait(completedListener);

        closeShards(primary1, primary2, primary3);
    }

    public void testQueueFifoOrdering() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final IndexShard primary5 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();
        final var handler1 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary1),
            task,
            primary1,
            ActionListener.noop()
        );
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        final List<Integer> callOrder = new ArrayList<>();
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary3),
            task,
            primary3,
            ActionListener.wrap(ignored -> fail("unexpected success"), ignored -> callOrder.add(1))
        );
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary4),
            task,
            primary4,
            ActionListener.wrap(ignored -> fail("unexpected success"), ignored -> callOrder.add(2))
        );
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary5),
            task,
            primary5,
            ActionListener.wrap(ignored -> fail("unexpected success"), ignored -> callOrder.add(3))
        );
        assertEquals(3, service.ongoingRecoveries.queuedRecoveryCount());
        // Newly processed recoveries will fail immediately: their target allocation ID is not in the shard's routing table
        service.ongoingRecoveries.onRecoveryComplete(primary1, handler1);
        assertEquals(List.of(1, 2, 3), callOrder);
        closeShards(primary1, primary2, primary3, primary4, primary5);
    }

    /// When a recovery slot becomes available, a new request must queue behind any already-pending
    /// items rather than starting immediately, even though active < limit at that moment.
    ///
    /// In production this is a transient concurrent state: `onRecoveryComplete` releases the
    /// lock after `remove()` but before calling `startRecoveriesUpToLimit()`, creating a
    /// window where active < limit while the queue is non-empty. The test reproduces it
    /// deterministically by calling `remove()` directly (freeing a slot without draining the
    /// queue) and then calling `addOrEnqueueNewRecovery()` in the same thread.
    public void testNewRequestEnqueuesIfPendingItemsExistWhenSlotBecomesAvailable() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();

        // Fill both active slots
        final var handler1 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary1),
            task,
            primary1,
            ActionListener.noop()
        );
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        // primary3 arrives while all slots are occupied, so it goes to the queue.
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        // Reproduce the concurrent window in onRecoveryComplete: call remove() directly to free
        // a slot without triggering startRecoveriesUpToLimit(), leaving active (1) < limit (2)
        // while the queue is still non-empty.
        service.ongoingRecoveries.remove(primary1, handler1);
        assertEquals(1, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        // primary4 must be queued behind primary3 and not start directly, even though a slot is
        // now free. Without pendingRecoveries.isEmpty() in addOrEnqueueNewRecovery it would bypass
        // primary3 and start immediately.
        final var handler4 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary4),
            task,
            primary4,
            ActionListener.noop()
        );
        assertNull("primary4 must queue behind primary3, not bypass it", handler4);
        // Both primary3 (added first) and primary4 are in the FIFO queue, with primary3 at the head.
        assertEquals(2, service.ongoingRecoveries.queuedRecoveryCount());

        closeShards(primary1, primary2, primary3, primary4);
    }

    public void testSameShardFillsMultipleSlots() throws IOException {
        final IndexShard primary = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();

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
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

        final var queued = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary2),
            task,
            primary2,
            ActionListener.noop()
        );
        assertNull(queued);
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        closeShards(primary, primary2);
    }

    public void testQueuedRecoveryCancelledOnShardClose() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        // Queue two recoveries for different shards.
        final AtomicReference<Exception> primary3Response = new AtomicReference<>();
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary3),
            task,
            primary3,
            ActionListener.wrap(ignored -> fail("unexpected success"), primary3Response::set)
        );
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(2, service.ongoingRecoveries.queuedRecoveryCount());
        assertEquals(1, primary4.recoveryStats().currentAsSourceQueued());

        service.beforeIndexShardClosed(primary3.shardId(), primary3, Settings.EMPTY);
        assertEquals(0, primary3.recoveryStats().currentAsSourceQueued());
        assertEquals(0, primary3.recoveryStats().currentAsSource());
        assertNotNull(primary3Response.get());
        assertThat(primary3Response.get(), instanceOf(DelayRecoveryException.class));
        assertThat(primary3Response.get().getMessage(), containsString("index shard closed"));

        assertEquals(1, primary4.recoveryStats().currentAsSourceQueued());
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        closeShards(primary1, primary2, primary3, primary4);
    }

    public void testQueuedRecoveryCancelledOnNodeLeft() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();

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
        final AtomicReference<Exception> primary3Response = new AtomicReference<>();
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            requestToDepartingNode,
            task,
            primary3,
            ActionListener.wrap(ignored -> fail("unexpected success"), primary3Response::set)
        );
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        // Simulate node departure, pending entry should fail
        service.ongoingRecoveries.cancelOnNodeLeft(departedNode);
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
        assertEquals(0, primary3.recoveryStats().currentAsSourceQueued());
        assertNotNull(primary3Response.get());
        assertThat(primary3Response.get(), instanceOf(DelayRecoveryException.class));
        assertThat(primary3Response.get().getMessage(), containsString("target node left"));

        closeShards(primary1, primary2, primary3);
    }

    public void testCancelAllPendingRecoveries() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        final AtomicReference<Exception> primary3Response = new AtomicReference<>();
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary3),
            task,
            primary3,
            ActionListener.wrap(ignored -> fail("unexpected success"), primary3Response::set)
        );
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        service.ongoingRecoveries.cancelAllPendingRecoveries();
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
        assertEquals(0, primary3.recoveryStats().currentAsSourceQueued());
        assertNotNull(primary3Response.get());
        assertThat(primary3Response.get().getMessage(), containsString("node is closing"));

        closeShards(primary1, primary2, primary3);
    }

    public void testDuplicateRejected() throws IOException {
        final IndexShard primary = newStartedShard(true);
        final var service = newPeerRecoverySourceService();
        service.start();
        final var task = newRecoveryTask();

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
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        final var queuedRequest = newStartRecoveryRequest(primary3);
        service.ongoingRecoveries.addOrEnqueueNewRecovery(queuedRequest, task, primary3, ActionListener.noop());
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        // Request for the same target allocation ID
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
        final var task = newRecoveryTask();

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
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

        closeShards(primary1, primary2, primary3);
    }

    public void testReestablishActiveRecovery() throws IOException {
        final IndexShard primary = newStartedShard(true);
        final var service = newPeerRecoverySourceService();
        service.start();
        final var task = newRecoveryTask();

        final var request = newStartRecoveryRequest(primary);
        final var handler = service.ongoingRecoveries.addOrEnqueueNewRecovery(request, task, primary, ActionListener.noop());
        assertNotNull(handler);

        // Reestablish with the correct recovery ID and allocation ID succeeds.
        final var reestablishRequest = new ReestablishRecoveryRequest(
            request.recoveryId(),
            request.shardId(),
            request.targetAllocationId()
        );
        service.ongoingRecoveries.reestablishRecovery(reestablishRequest, primary, ActionListener.noop());

        // Wrong recovery ID throws ResourceNotFoundException
        final var wrongIdRequest = new ReestablishRecoveryRequest(
            request.recoveryId() + 1,
            request.shardId(),
            request.targetAllocationId()
        );
        expectThrows(
            ResourceNotFoundException.class,
            () -> service.ongoingRecoveries.reestablishRecovery(wrongIdRequest, primary, ActionListener.noop())
        );

        service.ongoingRecoveries.onRecoveryComplete(primary, handler);
        closeShards(primary);
    }

    public void testReestablishPendingRecovery() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        // Capture the original listener to verify it is never called.
        final var request3 = newStartRecoveryRequest(primary3);
        final var oldListenerResponse = new AtomicReference<Exception>();
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            request3,
            task,
            primary3,
            ActionListener.wrap(ignored -> fail("unexpected success"), oldListenerResponse::set)
        );
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());
        assertEquals(1, primary3.recoveryStats().currentAsSourceQueued());

        // Reestablish the pending recovery with a fresh listener.
        final var newListenerResponse = new AtomicReference<Exception>();
        final var reestablishRequest = new ReestablishRecoveryRequest(
            request3.recoveryId(),
            request3.shardId(),
            request3.targetAllocationId()
        );
        service.ongoingRecoveries.reestablishRecovery(
            reestablishRequest,
            primary3,
            ActionListener.wrap(ignored -> fail("unexpected success"), newListenerResponse::set)
        );

        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());
        assertEquals(1, primary3.recoveryStats().currentAsSourceQueued());

        assertNull(oldListenerResponse.get());
        assertNull(newListenerResponse.get());

        // Both the old and new listener are notified on completion (here cancellation).
        service.ongoingRecoveries.cancelAllPendingRecoveries();
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
        assertEquals(0, primary3.recoveryStats().currentAsSourceQueued());
        assertNotNull("old listener must have been called on cancel", oldListenerResponse.get());
        assertThat(oldListenerResponse.get(), instanceOf(DelayRecoveryException.class));
        assertThat(oldListenerResponse.get().getMessage(), containsString("node is closing"));
        assertThat(newListenerResponse.get(), instanceOf(DelayRecoveryException.class));
        assertThat(newListenerResponse.get().getMessage(), containsString("node is closing"));

        // Reestablishing when no matching entry exists throws PeerRecoveryNotFound.
        expectThrows(
            PeerRecoveryNotFound.class,
            () -> service.ongoingRecoveries.reestablishRecovery(reestablishRequest, primary3, ActionListener.noop())
        );

        closeShards(primary1, primary2, primary3);
    }

    /// Tests when a shard has at least one active recovery AND a separate queued recovery (different allocation ID)
    public void testReestablishSameShardActiveAndQueuedRecovery() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();

        // Slot 1: primary1 with allocation ID A (active)
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        // Slot 2: primary2 fills the remaining slot
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

        // Queue a second recovery for primary1 with a different allocation ID
        final var queuedRequest = newStartRecoveryRequest(primary1);
        final var oldListenerResponse = new AtomicReference<Exception>();
        service.ongoingRecoveries.addOrEnqueueNewRecovery(
            queuedRequest,
            task,
            primary1,
            ActionListener.wrap(ignored -> fail("unexpected success"), oldListenerResponse::set)
        );
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        final var reestablishRequest = new ReestablishRecoveryRequest(
            queuedRequest.recoveryId(),
            queuedRequest.shardId(),
            queuedRequest.targetAllocationId()
        );
        final var newListenerResponse = new AtomicReference<Exception>();
        service.ongoingRecoveries.reestablishRecovery(
            reestablishRequest,
            primary1,
            ActionListener.wrap(ignored -> fail("unexpected success"), newListenerResponse::set)
        );
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        // Both the old and new listener are notified on completion (here cancellation).
        service.ongoingRecoveries.cancelAllPendingRecoveries();
        assertNotNull("old listener must be called on cancel", oldListenerResponse.get());
        assertThat(oldListenerResponse.get(), instanceOf(DelayRecoveryException.class));
        assertNotNull("new listener must be called on cancel", newListenerResponse.get());
        assertThat(newListenerResponse.get(), instanceOf(DelayRecoveryException.class));

        closeShards(primary1, primary2);
    }

    /// Regression test for the race where an active recovery completes during service shutdown and would cause
    /// a new pending recovery to be started after the lifecycle already moved to `State.STOPPED`.
    /// The queue is now drained before the lifecycle state changes.
    public void testCompletingRecoveryWhileStopping() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(1);
        service.start();
        final var task = newRecoveryTask();

        // Fill slot
        final var handler1 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary1),
            task,
            primary1,
            ActionListener.noop()
        );
        assertNotNull(handler1);

        // Queue another recovery
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        // Stop the service and complete handler1. Lifecycle assertions in the production code must hold.
        // The pending recovery should never start after the service moved to `State.STOPPED`.
        runInParallel(service::stop, () -> service.ongoingRecoveries.onRecoveryComplete(primary1, handler1));
        closeShards(primary1, primary2);
    }

    public void testDynamicLimitDecreaseQueuesNewRequests() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(3);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        // Decrease the limit before any recoveries start
        clusterSettings.applySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1).build()
        );

        // First request starts immediately under the new lower limit
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        assertEquals(1, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

        // Second request queues because the new limit (1) is already reached
        final var queued = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary2),
            task,
            primary2,
            ActionListener.noop()
        );
        assertNull(queued);
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        closeShards(primary1, primary2);
    }

    public void testDynamicLimitDecreaseDoesNotAffectActiveRecoveries() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(3);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
        assertEquals(3, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

        // Decreasing the limit below the current active count must not cancel or disturb the in-flight recoveries
        clusterSettings.applySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1).build()
        );

        assertEquals(3, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

        // New requests queue because active count (3) exceeds the new limit (1)
        final var handler4 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary4),
            task,
            primary4,
            ActionListener.noop()
        );
        assertNull(handler4);
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        closeShards(primary1, primary2, primary3, primary4);
    }

    public void testDynamicLimitDecreaseDoesNotNotifySchedulingListeners() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(3, schedulingListeners);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        // Fill all 3 active slots, then queue primary4
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
        assertEquals(3, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        final var dequeuedCount = new AtomicInteger();
        schedulingListeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onRecoveryDequeuedAndStarted(RecoverySource.Type type, RecoveryRole role) {
                dequeuedCount.incrementAndGet();
            }
        });

        // Decreasing the limit must not dequeue primary4 even though it is pending
        clusterSettings.applySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1).build()
        );

        assertEquals(0, dequeuedCount.get());
        assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

        closeShards(primary1, primary2, primary3, primary4);
    }

    public void testDynamicLimitIncreaseWithEmptyQueueDoesNotNotifySchedulingListeners() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(2, schedulingListeners);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

        final var dequeuedCount = new AtomicInteger();
        schedulingListeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onRecoveryDequeuedAndStarted(RecoverySource.Type type, RecoveryRole role) {
                dequeuedCount.incrementAndGet();
            }
        });

        // Queue is empty, so raising the limit dequeues nothing and must not fire onRecoveryDequeuedAndStarted
        clusterSettings.applySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4).build()
        );
        assertEquals(0, dequeuedCount.get());

        closeShards(primary1, primary2);
    }

    public void testDynamicLimitIncreaseAllowsDirectStart() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(2);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

        clusterSettings.applySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4).build()
        );

        // Both new requests must start immediately because active count (2) is below the new higher limit (4)
        final var handler3 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary3),
            task,
            primary3,
            ActionListener.noop()
        );
        final var handler4 = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(primary4),
            task,
            primary4,
            ActionListener.noop()
        );
        assertNotNull(handler3);
        assertNotNull(handler4);
        assertEquals(4, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

        closeShards(primary1, primary2, primary3, primary4);
    }

    public void testDynamicLimitIncreaseDrainsFullQueue() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final IndexShard primary5 = newStartedShard(true);
        // onRecoveryCompleted fires after remove() decrements activeRecoveryHandlerCount, so awaiting
        // all 3 guarantees the active count is stable before we assert.
        final var recoveriesCompleted = new CountDownLatch(3);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        schedulingListeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
                recoveriesCompleted.countDown();
            }
        });
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(2, schedulingListeners);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary5), task, primary5, ActionListener.noop());
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(3, service.ongoingRecoveries.queuedRecoveryCount());

        // Raising the limit high enough to fit everything drains the entire queue at once
        clusterSettings.applySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 5).build()
        );

        // Wait for all 3 queued recoveries to complete.
        safeAwait(recoveriesCompleted);
        // primary1 and primary2 handlers were discarded, so recoverToTarget was never explicitly called on them.
        // onRecoveryComplete never fires for those two, so they remain active.
        // For the other 3 enqueued requests, recoverToTarget is invoked as part of startRecoveriesUpToLimit.
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
        assertEquals(0, primary3.recoveryStats().currentAsSourceQueued());
        assertEquals(0, primary4.recoveryStats().currentAsSourceQueued());
        assertEquals(0, primary5.recoveryStats().currentAsSourceQueued());

        closeShards(primary1, primary2, primary3, primary4, primary5);
    }

    /// When the limit increase opens fewer new slots than items in the queue, the initial drain fills
    /// the available slots immediately; remaining items are pulled out via the cascade as recoveries complete.
    public void testDynamicLimitIncreaseDrainsViaInitialSlotsThenCascade() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final IndexShard primary5 = newStartedShard(true);
        // onRecoveryCompleted fires after remove() decrements activeRecoveryHandlerCount, so awaiting
        // all 3 guarantees the active count is stable before we assert.
        final var recoveriesCompleted = new CountDownLatch(3);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        schedulingListeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
                recoveriesCompleted.countDown();
            }
        });
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(2, schedulingListeners);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
        // Queue 3 items. The limit increase opens only 2 initial slots, so the 3rd drains via cascade.
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
        service.ongoingRecoveries.addOrEnqueueNewRecovery(newStartRecoveryRequest(primary5), task, primary5, ActionListener.noop());
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(3, service.ongoingRecoveries.queuedRecoveryCount());

        // Raising to 4 opens 2 initial slots; the remaining queued recovery drains via cascade
        clusterSettings.applySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4).build()
        );

        // Wait for all 3 queued recoveries to complete.
        safeAwait(recoveriesCompleted);
        // primary1 and primary2 handlers were discarded, so recoverToTarget was never called on them.
        // onRecoveryComplete never fires for those two, so they remain active.
        // For the other 3 enqueued requests, recoverToTarget is invoked as part of startRecoveriesUpToLimit.
        assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
        assertEquals(0, primary3.recoveryStats().currentAsSourceQueued());
        assertEquals(0, primary4.recoveryStats().currentAsSourceQueued());
        assertEquals(0, primary5.recoveryStats().currentAsSourceQueued());

        closeShards(primary1, primary2, primary3, primary4, primary5);
    }

    public void testStartRecoveriesUpToLimitHandlesSynchronousFailures() throws Exception {
        final int queuedRecoveryCount = 10000;
        final IndexShard queuedShard = newStartedShard(true);

        final PeerRecoverySourceService service = newPeerRecoverySourceService(1);
        service.start();
        final var task = newRecoveryTask();

        final var runningShard = newStartedShard(true);
        final var handler = service.ongoingRecoveries.addOrEnqueueNewRecovery(
            newStartRecoveryRequest(runningShard),
            task,
            runningShard,
            ActionListener.noop()
        );

        for (int i = 0; i < queuedRecoveryCount; i++) {
            service.ongoingRecoveries.addOrEnqueueNewRecovery(
                newStartRecoveryRequest(queuedShard),
                task,
                queuedShard,
                ActionListener.noop()
            );
        }

        assertEquals(1, service.ongoingRecoveries.activeRecoveryCount());
        assertEquals(queuedRecoveryCount, service.ongoingRecoveries.queuedRecoveryCount());

        // Close queued shard so recoveries fail synchronously in recoverToTarget
        closeShards(queuedShard);

        // Trigger cascading failures
        service.ongoingRecoveries.onRecoveryComplete(runningShard, handler);

        assertBusy(() -> assertEquals(0, service.ongoingRecoveries.activeRecoveryCount()));
        assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
    }

    private PeerRecoverySourceService newPeerRecoverySourceService() {
        return newPeerRecoverySourceService(Integer.MAX_VALUE);
    }

    private PeerRecoverySourceService newPeerRecoverySourceService(int limit) {
        return newPeerRecoverySourceService(limit, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    private Tuple<PeerRecoverySourceService, ClusterSettings> newPeerRecoverySourceServiceWithDynamicLimit(int limit) {
        return newPeerRecoverySourceServiceWithDynamicLimit(limit, new CompositeRecoverySchedulingListener());
    }

    private Tuple<PeerRecoverySourceService, ClusterSettings> newPeerRecoverySourceServiceWithDynamicLimit(
        int limit,
        CompositeRecoverySchedulingListener schedulingListeners
    ) {
        final var registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registeredSettings.add(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING);
        final var settings = Settings.builder()
            .put(NodeRoles.dataNode())
            .put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), limit)
            .build();
        final var clusterSettings = new ClusterSettings(settings, registeredSettings);
        return Tuple.tuple(newPeerRecoverySourceService(settings, clusterSettings, schedulingListeners), clusterSettings);
    }

    private PeerRecoverySourceService newPeerRecoverySourceService(int limit, Set<Setting<?>> registeredSettings) {
        final var settings = Settings.builder()
            .put(NodeRoles.dataNode())
            .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), limit)
            .build();
        return newPeerRecoverySourceService(
            settings,
            new ClusterSettings(settings, registeredSettings),
            new CompositeRecoverySchedulingListener()
        );
    }

    private PeerRecoverySourceService newPeerRecoverySourceService(
        Settings settings,
        ClusterSettings clusterSettings,
        CompositeRecoverySchedulingListener schedulingListeners
    ) {
        final var indicesService = mock(IndicesService.class);
        final var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(indicesService.clusterService()).thenReturn(clusterService);
        final TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        return new PeerRecoverySourceService(
            transportService,
            indicesService,
            clusterService,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(RecoveryPlannerService.class),
            schedulingListeners
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

    private Task newRecoveryTask() {
        return new Task(randomNonNegativeLong(), "test", START_RECOVERY, "", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }
}
