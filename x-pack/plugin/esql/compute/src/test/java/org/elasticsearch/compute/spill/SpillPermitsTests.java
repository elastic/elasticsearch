/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class SpillPermitsTests extends ESTestCase {

    public void testWaitersAreServedInOrderOnTheReleasingThread() {
        int max = between(1, 4);
        SpillPermits permits = new SpillPermits(max);
        List<Releasable> held = new ArrayList<>();
        List<Integer> order = new ArrayList<>();
        List<Thread> callbackThreads = new ArrayList<>();
        int requests = max + between(1, 5);
        for (int i = 0; i < requests; i++) {
            final int id = i;
            permits.acquire(ActionListener.wrap(permit -> {
                order.add(id);
                held.add(permit);
                callbackThreads.add(Thread.currentThread());
            }, e -> fail(e.toString())));
        }
        assertThat(permits.inUse(), equalTo(max));
        assertThat(permits.waiting(), equalTo(requests - max));
        assertThat(order.size(), equalTo(max));
        while (held.isEmpty() == false) {
            Releasable first = held.remove(0);
            first.close();
            first.close(); // a second close is harmless
        }
        assertThat(order.size(), equalTo(requests));
        for (int i = 0; i < requests; i++) {
            assertThat(order.get(i), equalTo(i));
            assertThat("a waiter is called back on the releasing thread", callbackThreads.get(i), sameInstance(Thread.currentThread()));
        }
        assertThat(permits.inUse(), equalTo(0));
        assertThat(permits.waiting(), equalTo(0));
    }

    public void testAWaiterThatClosesInlineHandsThePermitOn() {
        SpillPermits permits = new SpillPermits(1);
        List<Integer> served = new ArrayList<>();
        List<Releasable> first = new ArrayList<>();
        permits.acquire(ActionListener.wrap(permit -> {
            served.add(0);
            first.add(permit);
        }, e -> fail(e.toString())));
        // the first holder keeps its permit; the next two waiters release theirs as soon as they get them
        for (int i = 1; i <= 2; i++) {
            final int id = i;
            permits.acquire(ActionListener.wrap(permit -> {
                served.add(id);
                permit.close();
            }, e -> fail(e.toString())));
        }
        assertThat(served, equalTo(List.of(0)));
        assertThat(permits.waiting(), equalTo(2));
        // releasing the first permit serves both waiters in turn, each handing on from inside the previous release
        first.get(0).close();
        assertThat(served, equalTo(List.of(0, 1, 2)));
        assertThat(permits.inUse(), equalTo(0));
        assertThat(permits.waiting(), equalTo(0));
    }

    public void testAThrowingWaiterGetsItsFailureAndThePermitMovesOn() {
        SpillPermits permits = new SpillPermits(1);
        List<Releasable> first = new ArrayList<>();
        permits.acquire(ActionListener.wrap(first::add, e -> fail(e.toString())));
        List<Exception> failures = new ArrayList<>();
        // a listener that throws from onResponse itself; ActionListener.wrap would route the throw to its own failure path
        permits.acquire(new ActionListener<>() {
            @Override
            public void onResponse(Releasable permit) {
                throw new IllegalStateException("broken waiter");
            }

            @Override
            public void onFailure(Exception e) {
                failures.add(e);
            }
        });
        List<Integer> served = new ArrayList<>();
        permits.acquire(ActionListener.wrap(permit -> {
            served.add(2);
            permit.close();
        }, e -> fail(e.toString())));
        // the permit moves on to the next waiter and the broken one gets its failure; with assertions enabled, as in
        // tests, the contract violation then surfaces as an error to the releasing caller
        AssertionError error = expectThrows(AssertionError.class, () -> first.get(0).close());
        assertThat(error.getMessage(), equalTo("java.lang.IllegalStateException: broken waiter"));
        assertThat(failures.size(), equalTo(1));
        assertThat(failures.get(0).getMessage(), equalTo("broken waiter"));
        assertThat(served, equalTo(List.of(2)));
        assertThat(permits.inUse(), equalTo(0));
        assertThat(permits.waiting(), equalTo(0));
    }

    public void testRejectsANonPositiveBound() {
        expectThrows(IllegalArgumentException.class, () -> new SpillPermits(0));
    }
}
