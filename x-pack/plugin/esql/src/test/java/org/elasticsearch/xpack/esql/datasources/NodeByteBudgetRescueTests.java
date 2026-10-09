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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;

/**
 * U2: a wedged FIFO head is granted over the cap as a plain hold, once per call, then the
 * normal grant loop runs. The rescued hold is not the overshoot owner.
 */
public class NodeByteBudgetRescueTests extends ESTestCase {

    public void testRescueGrantsHeadOverCapAsPlainHoldThenOwnerPhase2() {
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        NodeByteBudget.Hold residual = budget.tryAdmit(60);
        assertNotNull(residual);

        RowGroupIo owner = new RowGroupIo();
        SubscribableListener<NodeByteBudget.Hold> ownerPhase1 = budget.admitAsync(60, owner, () -> false, Runnable::run);
        assertTrue(ownerPhase1.isDone());
        assertSame(owner, budget.overshootOwner());
        closeResult(ownerPhase1);
        assertEquals(60, budget.used());

        RowGroupIo head = new RowGroupIo();
        SubscribableListener<NodeByteBudget.Hold> headTicket = budget.admitAsync(60, head, () -> false, Runnable::run);
        assertFalse(headTicket.isDone());

        SubscribableListener<NodeByteBudget.Hold> ownerPhase2 = budget.admitAsync(10, owner, () -> false, Runnable::run);
        assertFalse(ownerPhase2.isDone());
        assertEquals(2, budget.waiterCount());

        assertTrue(budget.rescueHeadOverCap());
        assertTrue("HOL unstuck over the cap", headTicket.isDone());
        assertTrue("owner phase-2 granted by the post-rescue loop", ownerPhase2.isDone());
        assertEquals(0, budget.waiterCount());
        assertEquals(130, budget.used());

        AtomicReference<NodeByteBudget.Hold> headHold = new AtomicReference<>();
        headTicket.addListener(ActionListener.wrap(headHold::set, e -> fail(e.toString())));
        assertNotNull(headHold.get());
        assertFalse("rescued hold is plain, not the overshoot owner", headHold.get().isOvershoot());
        assertSame(owner, budget.overshootOwner());

        headHold.get().close();
        closeResult(ownerPhase2);
        residual.close();
        budget.clearOwner(owner);
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
    }

    public void testRescueOneHeadPerCall() {
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        NodeByteBudget.Hold residual = budget.tryAdmit(80);
        assertNotNull(residual);
        NodeByteBudget.Hold overshoot = occupyOvershoot(budget, 25);
        overshoot.close();
        assertEquals(80, budget.used());
        assertSame(overshoot.lease(), budget.overshootOwner());

        SubscribableListener<NodeByteBudget.Hold> first = budget.admitAsync(50, new RowGroupIo(), () -> false, Runnable::run);
        SubscribableListener<NodeByteBudget.Hold> second = budget.admitAsync(50, new RowGroupIo(), () -> false, Runnable::run);
        assertEquals(2, budget.waiterCount());

        assertTrue(budget.rescueHeadOverCap());
        assertTrue(first.isDone());
        assertFalse("second waiter stays queued until the next stall window", second.isDone());
        assertEquals(1, budget.waiterCount());
        assertEquals(130, budget.used());
        AtomicReference<NodeByteBudget.Hold> firstHold = new AtomicReference<>();
        first.addListener(ActionListener.wrap(firstHold::set, e -> fail(e.toString())));
        assertFalse(firstHold.get().isOvershoot());
        assertSame(overshoot.lease(), budget.overshootOwner());

        assertTrue(budget.rescueHeadOverCap());
        assertTrue(second.isDone());
        assertEquals(0, budget.waiterCount());
        assertEquals(180, budget.used());
        AtomicReference<NodeByteBudget.Hold> secondHold = new AtomicReference<>();
        second.addListener(ActionListener.wrap(secondHold::set, e -> fail(e.toString())));
        assertFalse(secondHold.get().isOvershoot());
        assertSame(overshoot.lease(), budget.overshootOwner());

        firstHold.get().close();
        secondHold.get().close();
        residual.close();
        budget.clearOwner(overshoot.lease());
        assertEquals(0, budget.used());
    }

    public void testRescueSkipsCancelledHead() {
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        NodeByteBudget.Hold residual = budget.tryAdmit(80);
        assertNotNull(residual);
        NodeByteBudget.Hold overshoot = occupyOvershoot(budget, 25);
        overshoot.close();

        AtomicBoolean cancelFirst = new AtomicBoolean();
        SubscribableListener<NodeByteBudget.Hold> cancelled = budget.admitAsync(50, new RowGroupIo(), cancelFirst::get, Runnable::run);
        SubscribableListener<NodeByteBudget.Hold> live = budget.admitAsync(50, new RowGroupIo(), () -> false, Runnable::run);
        assertEquals(2, budget.waiterCount());
        cancelFirst.set(true);

        assertTrue(budget.rescueHeadOverCap());
        assertTrue(cancelled.isDone());
        cancelled.addListener(ActionListener.wrap(h -> fail("cancelled head must not be rescued"), e -> {
            assertThat(e, instanceOf(EsRejectedExecutionException.class));
        }));
        assertTrue(live.isDone());
        assertEquals(0, budget.waiterCount());
        assertEquals(130, budget.used());
        AtomicReference<NodeByteBudget.Hold> liveHold = new AtomicReference<>();
        live.addListener(ActionListener.wrap(liveHold::set, e -> fail(e.toString())));
        assertFalse(liveHold.get().isOvershoot());
        assertSame(overshoot.lease(), budget.overshootOwner());

        liveHold.get().close();
        residual.close();
        budget.clearOwner(overshoot.lease());
        assertEquals(0, budget.used());
    }

    public void testRescueNoopWhenQueueEmpty() {
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        NodeByteBudget.Hold hold = budget.tryAdmit(40);
        assertNotNull(hold);
        assertFalse(budget.rescueHeadOverCap());
        assertEquals(40, budget.used());
        hold.close();
        assertEquals(0, budget.used());
    }

    private static NodeByteBudget.Hold occupyOvershoot(NodeByteBudgetService budget, long bytes) {
        SubscribableListener<NodeByteBudget.Hold> ticket = budget.admitAsync(bytes, new RowGroupIo(), () -> false, Runnable::run);
        assertTrue(ticket.isDone());
        AtomicReference<NodeByteBudget.Hold> hold = new AtomicReference<>();
        ticket.addListener(ActionListener.wrap(hold::set, e -> fail(e.toString())));
        assertNotNull(hold.get());
        assertTrue(hold.get().isOvershoot());
        return hold.get();
    }

    private static void closeResult(SubscribableListener<NodeByteBudget.Hold> listener) {
        listener.addListener(ActionListener.wrap(NodeByteBudget.Hold::close, e -> fail(e.toString())));
    }
}
