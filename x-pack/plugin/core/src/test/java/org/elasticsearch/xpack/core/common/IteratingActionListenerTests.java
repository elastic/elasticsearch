/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class IteratingActionListenerTests extends ESTestCase {

    public void testIteration() {
        final int numberOfItems = scaledRandomIntBetween(1, 32);
        final int numberOfIterations = scaledRandomIntBetween(1, numberOfItems);
        List<Object> items = new ArrayList<>(numberOfItems);
        for (int i = 0; i < numberOfItems; i++) {
            items.add(new Object());
        }

        final AtomicInteger iterations = new AtomicInteger(0);
        final BiConsumer<Object, ActionListener<Object>> consumer = (listValue, listener) -> {
            final int current = iterations.incrementAndGet();
            if (current == numberOfIterations) {
                listener.onResponse(items.get(current - 1));
            } else {
                listener.onResponse(null);
            }
        };

        IteratingActionListener<Object, Object> iteratingListener = new IteratingActionListener<>(
            ActionTestUtils.assertNoFailureListener(object -> {
                assertNotNull(object);
                assertThat(object, sameInstance(items.get(numberOfIterations - 1)));
            }),
            consumer,
            items,
            new ThreadContext(Settings.EMPTY)
        );
        iteratingListener.run();

        // we never really went async, its all chained together so verify this for sanity
        assertEquals(numberOfIterations, iterations.get());
    }

    public void testIterationDoesntAllowThreadContextLeak() {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final int numberOfItems = scaledRandomIntBetween(1, 32);
        final int numberOfIterations = scaledRandomIntBetween(1, numberOfItems);
        List<Object> items = new ArrayList<>(numberOfItems);
        for (int i = 0; i < numberOfItems; i++) {
            items.add(new Object());
        }

        threadContext.putHeader("outside", "listener");
        final AtomicInteger iterations = new AtomicInteger(0);
        final BiConsumer<Object, ActionListener<Object>> consumer = (listValue, listener) -> {
            final int current = iterations.incrementAndGet();
            assertEquals("listener", threadContext.getHeader("outside"));
            if (current == numberOfIterations) {
                threadContext.putHeader("foo", "bar");
                listener.onResponse(items.get(current - 1));
            } else {
                listener.onResponse(null);
            }
        };

        IteratingActionListener<Object, Object> iteratingListener = new IteratingActionListener<>(
            ActionTestUtils.assertNoFailureListener(object -> {
                assertNotNull(object);
                assertThat(object, sameInstance(items.get(numberOfIterations - 1)));
                assertEquals("bar", threadContext.getHeader("foo"));
                assertEquals("listener", threadContext.getHeader("outside"));
            }),
            consumer,
            items,
            threadContext
        );
        iteratingListener.run();

        // we never really went async, its all chained together so verify this for sanity
        assertEquals(numberOfIterations, iterations.get());
        assertNull(threadContext.getHeader("foo"));
        assertEquals("listener", threadContext.getHeader("outside"));
    }

    public void testIterationEmptyList() {
        IteratingActionListener<Object, Object> listener = new IteratingActionListener<>(
            ActionTestUtils.assertNoFailureListener(Assert::assertNull),
            (listValue, iteratingListener) -> fail("consumer should not have been called!!!"),
            Collections.emptyList(),
            new ThreadContext(Settings.EMPTY)
        );
        listener.run();
    }

    public void testFailure() {
        final int numberOfItems = scaledRandomIntBetween(1, 32);
        final int numberOfIterations = scaledRandomIntBetween(1, numberOfItems);
        List<Object> items = new ArrayList<>(numberOfItems);
        for (int i = 0; i < numberOfItems; i++) {
            items.add(new Object());
        }

        final AtomicInteger iterations = new AtomicInteger(0);
        final BiConsumer<Object, ActionListener<Object>> consumer = (listValue, listener) -> {
            final int current = iterations.incrementAndGet();
            if (current == numberOfIterations) {
                listener.onFailure(new ElasticsearchException("expected exception"));
            } else {
                listener.onResponse(null);
            }
        };

        final AtomicBoolean onFailureCalled = new AtomicBoolean(false);
        IteratingActionListener<Object, Object> iteratingListener = new IteratingActionListener<>(ActionListener.wrap((object) -> {
            fail("onResponse should not have been called, but was called with: " + object);
        }, (e) -> {
            assertEquals("expected exception", e.getMessage());
            assertTrue(onFailureCalled.compareAndSet(false, true));
        }), consumer, items, new ThreadContext(Settings.EMPTY));
        iteratingListener.run();

        // we never really went async, its all chained together so verify this for sanity
        assertEquals(numberOfIterations, iterations.get());
        assertTrue(onFailureCalled.get());
    }

    public void testFunctionApplied() {
        final int numberOfItems = scaledRandomIntBetween(2, 32);
        final int numberOfIterations = scaledRandomIntBetween(1, numberOfItems);
        List<Object> items = new ArrayList<>(numberOfItems);
        for (int i = 0; i < numberOfItems; i++) {
            items.add(new Object());
        }

        final AtomicInteger iterations = new AtomicInteger(0);
        final Predicate<Object> iterationPredicate = object -> {
            final int current = iterations.incrementAndGet();
            return current != numberOfIterations;
        };
        final BiConsumer<Object, ActionListener<Object>> consumer = (listValue, listener) -> {
            listener.onResponse(items.get(iterations.get()));
        };

        final AtomicReference<Object> originalObject = new AtomicReference<>();
        final AtomicReference<Object> result = new AtomicReference<>();
        final Function<Object, Object> responseFunction = object -> {
            originalObject.set(object);
            Object randomResult;
            do {
                randomResult = randomFrom(items);
            } while (randomResult == object);
            result.set(randomResult);
            return randomResult;
        };

        IteratingActionListener<Object, Object> iteratingListener = new IteratingActionListener<>(
            ActionTestUtils.assertNoFailureListener(object -> {
                assertNotNull(object);
                assertNotNull(originalObject.get());
                assertThat(object, sameInstance(result.get()));
                assertThat(object, not(sameInstance(originalObject.get())));
                assertThat(originalObject.get(), sameInstance(items.get(iterations.get() - 1)));
            }),
            consumer,
            items,
            new ThreadContext(Settings.EMPTY),
            responseFunction,
            iterationPredicate
        );
        iteratingListener.run();

        // we never really went async, its all chained together so verify this for sanity
        assertEquals(numberOfIterations, iterations.get());
    }
}
