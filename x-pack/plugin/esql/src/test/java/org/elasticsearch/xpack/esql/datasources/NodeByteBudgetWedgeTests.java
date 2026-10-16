/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.NodeByteBudget;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Byte-budget pin, wake-up, and single-reservation protocol for two-phase Parquet.
 */
public class NodeByteBudgetWedgeTests extends ESTestCase {

    /**
     * Owner has retargeted its group reservation down to phase 2 and still holds the slot.
     * The FIFO head waits. On the owner's roll (close, finish, {@code clearOwner}) the head
     * becomes owner without {@code wakeWaiters} or rescue. Ledger ends at 0.
     */
    public void testOwnerPhase2BehindNonOwnerHead() {
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        QueryConcurrencyBudget query = new QueryConcurrencyBudget(4, 60_000, null);
        NodeByteBudget.Hold residual = budget.tryAdmit(60);
        assertNotNull(residual);

        RowGroupIo owner = new RowGroupIo();
        query.bind(owner);
        SubscribableListener<NodeByteBudget.Hold> ownerTicket = budget.admitAsync(60, owner, () -> false, Runnable::run);
        assertTrue(ownerTicket.isDone());
        AtomicReference<NodeByteBudget.Hold> ownerHold = new AtomicReference<>();
        ownerTicket.addListener(ActionListener.wrap(ownerHold::set, e -> fail(e.toString())));
        assertSame(owner, budget.overshootOwner());
        assertTrue(owner.isPinned());

        ownerHold.get().grow(10);
        ownerHold.get().drop(60);
        assertEquals("retarget to phase 2 keeps the reservation", 70, budget.used());
        assertSame(owner, budget.overshootOwner());

        RowGroupIo head = new RowGroupIo();
        query.bind(head);
        SubscribableListener<NodeByteBudget.Hold> headTicket = budget.admitAsync(60, head, () -> false, Runnable::run);
        assertFalse("head waits for the slot the owner still holds", headTicket.isDone());
        assertEquals(1, budget.waiterCount());

        ownerHold.get().close();
        owner.finish();
        budget.clearOwner(owner);
        assertTrue(headTicket.isDone());
        assertSame(head, budget.overshootOwner());
        assertTrue(head.isPinned());
        closeResult(headTicket);
        residual.close();
        budget.clearOwner(head);
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
        assertNull(budget.overshootOwner());
        assertFalse(owner.isPinned());
        assertFalse(head.isPinned());
    }

    /**
     * The overshoot slot is free. The FIFO head is not its query's GET winner (an older lease of
     * the same query is). Pinning no longer requires that winner, so the head takes the slot,
     * is pinned, and becomes favoured.
     */
    public void testFreeSlotButHeadIsNotQueryWinner() {
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        QueryConcurrencyBudget query = new QueryConcurrencyBudget(4, 60_000, null);
        NodeByteBudget.Hold residual = budget.tryAdmit(60);
        assertNotNull(residual);

        RowGroupIo older = new RowGroupIo();
        query.bind(older);

        RowGroupIo head = new RowGroupIo();
        query.bind(head);
        SubscribableListener<NodeByteBudget.Hold> headTicket = budget.admitAsync(60, head, () -> false, Runnable::run);
        assertTrue("head pins even when it is not the query winner", headTicket.isDone());
        assertSame(head, budget.overshootOwner());
        assertTrue(head.isPinned());
        assertSame(head, query.favoured());
        closeResult(headTicket);
        residual.close();
        budget.clearOwner(head);
        assertEquals(0, budget.used());
        assertNull(budget.overshootOwner());
        assertFalse(head.isPinned());
    }

    /**
     * {@code other} holds a live pin, so the head cannot take a free slot. {@code finish} clears
     * the pin without waking the byte budget. {@code clearOwner} of that non-owner must re-run
     * the grant loop.
     */
    public void testClearOwnerOfLivePinRegrants() {
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        QueryConcurrencyBudget query = new QueryConcurrencyBudget(4, 60_000, null);
        NodeByteBudget.Hold residual = budget.tryAdmit(50);
        assertNotNull(residual);

        RowGroupIo other = new RowGroupIo();
        query.bind(other);
        assertTrue(other.tryPinOvershoot());
        assertTrue(other.isPinned());

        RowGroupIo head = new RowGroupIo();
        query.bind(head);
        SubscribableListener<NodeByteBudget.Hold> headTicket = budget.admitAsync(60, head, () -> false, Runnable::run);
        assertFalse("stale pin blocks the head", headTicket.isDone());

        other.finish();
        assertFalse(other.isPinned());
        assertFalse("QCB finish does not wake the byte budget", headTicket.isDone());
        budget.clearOwner(other);
        assertTrue("clearOwner(non-owner) re-runs the grant loop", headTicket.isDone());
        assertSame(head, budget.overshootOwner());
        assertTrue(head.isPinned());
        assertSame(head, query.favoured());
        closeResult(headTicket);
        residual.close();
        budget.clearOwner(head);
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
        assertNull(budget.overshootOwner());
    }

    public void testClearOwnerUnpinsLivePin() {
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        QueryConcurrencyBudget query = new QueryConcurrencyBudget(4, 60_000, null);
        NodeByteBudget.Hold residual = budget.tryAdmit(50);
        assertNotNull(residual);

        RowGroupIo other = new RowGroupIo();
        query.bind(other);
        assertTrue(other.tryPinOvershoot());

        RowGroupIo head = new RowGroupIo();
        query.bind(head);
        SubscribableListener<NodeByteBudget.Hold> headTicket = budget.admitAsync(60, head, () -> false, Runnable::run);
        assertFalse(headTicket.isDone());

        budget.clearOwner(other);
        assertFalse(other.isPinned());
        assertTrue(headTicket.isDone());
        assertSame(head, budget.overshootOwner());
        closeResult(headTicket);
        residual.close();
        budget.clearOwner(head);
        assertEquals(0, budget.used());
        assertNull(budget.overshootOwner());
    }

    public void testCancelOwnerClearsSlot() {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        QueryConcurrencyBudget ownerQuery = new QueryConcurrencyBudget(4, 60_000, null);
        QueryConcurrencyBudget headQuery = new QueryConcurrencyBudget(4, 60_000, null);
        RowGroupIo owner = new RowGroupIo();
        ownerQuery.bind(owner);
        SubscribableListener<NodeByteBudget.Hold> ownerTicket = budget.admitAsync(15, owner, () -> false, Runnable::run);
        assertTrue(ownerTicket.isDone());
        AtomicReference<NodeByteBudget.Hold> ownerHold = new AtomicReference<>();
        ownerTicket.addListener(ActionListener.wrap(ownerHold::set, e -> fail(e.toString())));
        assertTrue(ownerHold.get().isOvershoot());
        assertSame(owner, budget.overshootOwner());
        assertTrue(owner.isPinned());
        assertSame(owner, ownerQuery.favoured());

        RowGroupIo head = new RowGroupIo();
        headQuery.bind(head);
        SubscribableListener<NodeByteBudget.Hold> headTicket = budget.admitAsync(4, head, () -> false, Runnable::run);
        assertFalse(headTicket.isDone());

        ownerQuery.close();
        assertTrue("QCB close must cancel the owner and re-grant the FIFO head", headTicket.isDone());
        assertSame(head, budget.overshootOwner());
        assertTrue(head.isPinned());
        assertFalse(owner.isPinned());
        assertSame(head, headQuery.favoured());
        ownerHold.get().close();
        closeResult(headTicket);
        budget.clearOwner(head);
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
        assertNull(budget.overshootOwner());
        headQuery.close();
    }

    public void testCancelQueuedWaiterFailsWithoutCharging() {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        QueryConcurrencyBudget waiterQuery = new QueryConcurrencyBudget(4, 60_000, null);
        NodeByteBudget.Hold ownerHold = occupyOvershoot(budget, 15);
        long used = budget.used();
        RowGroupIo waiter = new RowGroupIo();
        waiterQuery.bind(waiter);
        SubscribableListener<NodeByteBudget.Hold> ticket = budget.admitAsync(4, waiter, () -> waiter.isCancelled(), Runnable::run);
        assertFalse(ticket.isDone());
        waiterQuery.close();
        assertTrue(ticket.isDone());
        AtomicReference<Exception> error = new AtomicReference<>();
        ticket.addListener(ActionListener.wrap(hold -> fail("cancelled waiter must not grant"), error::set));
        assertThat(error.get(), instanceOf(EsRejectedExecutionException.class));
        assertThat(error.get().getMessage(), containsString("Cancel"));
        assertEquals(used, budget.used());
        assertNull(waiterQuery.favoured());
        ownerHold.close();
        budget.clearOwner(ownerHold.lease());
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
    }

    public void testFinishedHeadFails() {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        NodeByteBudget.Hold blocking = occupyOvershoot(budget, 15);
        RowGroupIo head = new RowGroupIo();
        SubscribableListener<NodeByteBudget.Hold> headTicket = budget.admitAsync(4, head, () -> false, Runnable::run);
        assertFalse(headTicket.isDone());
        head.finish();
        budget.wakeWaiters();
        assertTrue(headTicket.isDone());
        AtomicReference<Exception> error = new AtomicReference<>();
        headTicket.addListener(ActionListener.wrap(hold -> fail("finished head must not grant"), error::set));
        assertThat(error.get(), instanceOf(EsRejectedExecutionException.class));
        assertThat(error.get().getMessage(), containsString("finished"));

        RowGroupIo already = new RowGroupIo();
        already.finish();
        SubscribableListener<NodeByteBudget.Hold> immediate = budget.admitAsync(4, already, () -> false, Runnable::run);
        assertTrue(immediate.isDone());
        AtomicReference<Exception> immediateError = new AtomicReference<>();
        immediate.addListener(ActionListener.wrap(hold -> fail("finished lease must fail at enqueue"), immediateError::set));
        assertThat(immediateError.get().getMessage(), containsString("finished"));

        blocking.close();
        budget.clearOwner(blocking.lease());
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
        assertNull(budget.overshootOwner());
    }

    private static void closeResult(SubscribableListener<NodeByteBudget.Hold> listener) {
        listener.addListener(ActionListener.wrap(NodeByteBudget.Hold::close, e -> fail(e.toString())));
    }

    private static NodeByteBudget.Hold occupyOvershoot(NodeByteBudgetService budget, long bytes) {
        SubscribableListener<NodeByteBudget.Hold> ticket = budget.admitAsync(bytes, new RowGroupIo(), () -> false, Runnable::run);
        assertTrue(ticket.isDone());
        AtomicReference<NodeByteBudget.Hold> hold = new AtomicReference<>();
        ticket.addListener(ActionListener.wrap(hold::set, e -> fail(e.toString())));
        assertTrue(hold.get().isOvershoot());
        return hold.get();
    }
}
