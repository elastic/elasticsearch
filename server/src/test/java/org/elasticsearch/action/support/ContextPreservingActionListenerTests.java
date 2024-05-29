/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class ContextPreservingActionListenerTests extends ESTestCase {

    public void testOriginalContextIsPreservedAfterOnResponse() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final boolean nonEmptyContext = randomBoolean();
        if (nonEmptyContext) {
            threadContext.putHeader("not empty", "value");
        }
        final ContextPreservingActionListener<Void> actionListener;
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            final ActionListener<Void> delegate = new ActionListener<Void>() {
                @Override
                public void onResponse(Void aVoid) {
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertNull(threadContext.getHeader("not empty"));
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException("onFailure shouldn't be called", e);
                }
            };
            if (randomBoolean()) {
                actionListener = new ContextPreservingActionListener<>(threadContext.newRestorableContext(true), delegate);
            } else {
                actionListener = ContextPreservingActionListener.wrapPreservingContext(delegate, threadContext);
            }
        }

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));

        actionListener.onResponse(null);

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));
    }

    public void testOriginalContextIsPreservedAfterOnFailure() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final boolean nonEmptyContext = randomBoolean();
        if (nonEmptyContext) {
            threadContext.putHeader("not empty", "value");
        }
        final ContextPreservingActionListener<Void> actionListener;
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            final ActionListener<Void> delegate = new ActionListener<Void>() {
                @Override
                public void onResponse(Void aVoid) {
                    throw new RuntimeException("onResponse shouldn't be called");
                }

                @Override
                public void onFailure(Exception e) {
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertNull(threadContext.getHeader("not empty"));
                }
            };

            if (randomBoolean()) {
                actionListener = new ContextPreservingActionListener<>(threadContext.newRestorableContext(true), delegate);
            } else {
                actionListener = ContextPreservingActionListener.wrapPreservingContext(delegate, threadContext);
            }

        }

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));

        actionListener.onFailure(new IOException("test"));

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));
    }

    public void testOriginalContextIsWhenListenerThrows() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final boolean nonEmptyContext = randomBoolean();
        if (nonEmptyContext) {
            threadContext.putHeader("not empty", "value");
        }
        final AtomicReference<String> methodCalled = new AtomicReference<>();
        final ContextPreservingActionListener<Void> actionListener;
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            final ActionListener<Void> delegate = new ActionListener<Void>() {
                @Override
                public void onResponse(Void aVoid) {
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertNull(threadContext.getHeader("not empty"));
                    assertTrue(methodCalled.compareAndSet(null, "onResponse"));
                }

                @Override
                public void onFailure(Exception e) {
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertNull(threadContext.getHeader("not empty"));
                    assertTrue(methodCalled.compareAndSet(null, "onFailure"));
                }
            };

            if (randomBoolean()) {
                actionListener = new ContextPreservingActionListener<>(threadContext.newRestorableContext(true), delegate);
            } else {
                actionListener = ContextPreservingActionListener.wrapPreservingContext(delegate, threadContext);
            }
        }

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));

        actionListener.onResponse(null);
        assertEquals("onResponse", methodCalled.getAndSet(null));

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));

        actionListener.onFailure(new Exception("simulated"));
        assertEquals("onFailure", methodCalled.getAndSet(null));

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));
    }

    public void testToStringIncludesDelegate() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final ContextPreservingActionListener<Void> actionListener;
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final ActionListener<Void> delegate = new ActionListener<>() {
                @Override
                public void onResponse(Void aVoid) {}

                @Override
                public void onFailure(Exception e) {}

                @Override
                public String toString() {
                    return "test delegate";
                }
            };

            actionListener = ContextPreservingActionListener.wrapPreservingContext(delegate, threadContext);
        }

        assertThat(actionListener.toString(), allOf(containsString("test delegate"), containsString("ContextPreservingActionListener")));
    }

}
