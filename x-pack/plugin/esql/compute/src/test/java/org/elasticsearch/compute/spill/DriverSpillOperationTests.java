/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The driver's scheduling adapter: an operation is an async action from issue to completion, its blocked state completes
 * on success and on failure alike, the outcome is in place when the blocked state completes, and the completion piece
 * works on its own for a holder that has no driver.
 */
public class DriverSpillOperationTests extends ComputeTestCase {

    private DriverContext driverContext() {
        BlockFactory factory = blockFactory();
        return new DriverContext(factory.bigArrays(), factory, null);
    }

    public void testHeldOperationBlocksAndCompletesWithItsResult() {
        DriverContext context = driverContext();
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        DriverSpillOperation<String> operation = DriverSpillOperation.issue(context, "writing", held::set);
        assertTrue(context.hasPendingAsyncActions());
        IsBlockedResult blocked = operation.blocked();
        assertFalse(blocked.listener().isDone());
        assertThat(blocked.reason(), equalTo("writing"));
        assertFalse(operation.isDone());
        AtomicReference<Boolean> outcomeInPlace = new AtomicReference<>();
        blocked.listener().addListener(ActionListener.running(() -> outcomeInPlace.set(operation.isDone())));
        held.get().onResponse("done");
        assertTrue(blocked.listener().isDone());
        assertTrue(outcomeInPlace.get());
        assertThat(operation.result(), equalTo("done"));
        assertThat(operation.failure(), nullValue());
        assertFalse(context.hasPendingAsyncActions());
        PlainActionFuture<Void> drained = new PlainActionFuture<>();
        context.waitForAsyncActions(drained);
        context.finish();
        assertTrue(drained.isDone());
    }

    public void testFailureAlsoUnblocks() {
        DriverContext context = driverContext();
        AtomicReference<ActionListener<String>> held = new AtomicReference<>();
        DriverSpillOperation<String> operation = DriverSpillOperation.issue(context, "reading", held::set);
        IsBlockedResult blocked = operation.blocked();
        held.get().onFailure(new IOException("boom"));
        assertTrue(blocked.listener().isDone());
        assertTrue(blocked.listener().isSuccess());
        assertTrue(operation.isDone());
        assertThat(operation.result(), nullValue());
        assertThat(operation.failure(), instanceOf(IOException.class));
        assertFalse(context.hasPendingAsyncActions());
        context.finish();
    }

    public void testInlineCompletionAndAThrowingIssue() {
        DriverContext context = driverContext();
        DriverSpillOperation<Integer> inline = DriverSpillOperation.issue(context, "inline", l -> l.onResponse(42));
        assertTrue(inline.isDone());
        assertThat(inline.result(), equalTo(42));
        assertTrue(inline.blocked().listener().isDone());
        assertFalse(context.hasPendingAsyncActions());
        DriverSpillOperation<Integer> throwing = DriverSpillOperation.issue(
            context,
            "throwing",
            l -> { throw new IllegalStateException("no"); }
        );
        assertTrue(throwing.isDone());
        assertThat(throwing.failure(), instanceOf(IllegalStateException.class));
        assertFalse(context.hasPendingAsyncActions());
        context.finish();
    }

    public void testCompletionStandsAlone() {
        SpillCompletion<String> completion = new SpillCompletion<>();
        AtomicReference<String> seen = new AtomicReference<>();
        completion.addListener(ActionListener.wrap(seen::set, e -> fail(e.toString())));
        IsBlockedResult blocked = completion.blocked("alone");
        assertThat(blocked.listener().isDone(), equalTo(false));
        completion.listener().onResponse("x");
        assertThat(seen.get(), equalTo("x"));
        assertThat(completion.result(), equalTo("x"));
        assertTrue(blocked.listener().isDone());
        assertThat(completion.blocked("again").listener(), sameInstance(blocked.listener()));
    }
}
