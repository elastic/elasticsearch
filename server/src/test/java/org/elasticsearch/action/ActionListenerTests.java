/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ActionListenerTests extends ESTestCase {

    public void testWrap() {
        AtomicReference<Boolean> reference = new AtomicReference<>();
        AtomicReference<Exception> exReference = new AtomicReference<>();

        CheckedConsumer<Boolean, ? extends Exception> handler = (o) -> {
            if (Boolean.FALSE.equals(o)) {
                throw new IllegalArgumentException("must not be false");
            }
            reference.set(o);
        };
        ActionListener<Boolean> wrap = ActionListener.wrap(handler, exReference::set);
        wrap.onResponse(Boolean.FALSE);
        assertNull(reference.get());
        assertNotNull(exReference.get());
        assertEquals("must not be false", exReference.get().getMessage());
        exReference.set(null);

        wrap.onResponse(Boolean.TRUE);
        assertTrue(reference.get());
        assertNull(exReference.get());
    }

    public void testOnResponse() {
        final int numListeners = randomIntBetween(1, 20);
        List<AtomicReference<Boolean>> refList = new ArrayList<>();
        List<AtomicReference<Exception>> excList = new ArrayList<>();
        List<ActionListener<Boolean>> listeners = new ArrayList<>();
        List<Boolean> failOnTrue = new ArrayList<>();
        AtomicInteger exceptionCounter = new AtomicInteger(0);
        for (int i = 0; i < numListeners; i++) {
            boolean doFailOnTrue = rarely();
            failOnTrue.add(doFailOnTrue);
            AtomicReference<Boolean> reference = new AtomicReference<>();
            AtomicReference<Exception> exReference = new AtomicReference<>();
            refList.add(reference);
            excList.add(exReference);
            CheckedConsumer<Boolean, ? extends Exception> handler = (o) -> {
                if (Boolean.FALSE.equals(o)) {
                    throw new IllegalArgumentException("must not be false " + exceptionCounter.getAndIncrement());
                }
                if (doFailOnTrue) {
                    throw new IllegalStateException("must not be true");
                }
                reference.set(o);
            };
            listeners.add(ActionListener.wrap(handler, exReference::set));
        }

        ActionListener.onResponse(listeners, Boolean.TRUE);
        for (int i = 0; i < numListeners; i++) {
            if (failOnTrue.get(i) == false) {
                assertTrue("listener index " + i, refList.get(i).get());
                refList.get(i).set(null);
            } else {
                assertNull("listener index " + i, refList.get(i).get());
            }

        }

        for (int i = 0; i < numListeners; i++) {
            if (failOnTrue.get(i) == false) {
                assertNull("listener index " + i, excList.get(i).get());
            } else {
                assertEquals("listener index " + i, "must not be true", excList.get(i).get().getMessage());
            }
        }

        ActionListener.onResponse(listeners, Boolean.FALSE);
        for (int i = 0; i < numListeners; i++) {
            assertNull("listener index " + i, refList.get(i).get());
        }

        assertEquals(numListeners, exceptionCounter.get());
        for (int i = 0; i < numListeners; i++) {
            assertNotNull(excList.get(i).get());
            assertEquals("listener index " + i, "must not be false " + i, excList.get(i).get().getMessage());
        }
    }

    public void testOnFailure() {
        final int numListeners = randomIntBetween(1, 20);
        List<AtomicReference<Boolean>> refList = new ArrayList<>();
        List<AtomicReference<Exception>> excList = new ArrayList<>();
        List<ActionListener<Boolean>> listeners = new ArrayList<>();

        final int listenerToFail = randomBoolean() ? -1 : randomIntBetween(0, numListeners - 1);
        for (int i = 0; i < numListeners; i++) {
            AtomicReference<Boolean> reference = new AtomicReference<>();
            AtomicReference<Exception> exReference = new AtomicReference<>();
            refList.add(reference);
            excList.add(exReference);
            boolean fail = i == listenerToFail;
            CheckedConsumer<Boolean, ? extends Exception> handler = (o) -> { reference.set(o); };
            listeners.add(ActionListener.wrap(handler, (e) -> {
                exReference.set(e);
                if (fail) {
                    throw new RuntimeException("double boom");
                }
            }));
        }

        try {
            ActionListener.onFailure(listeners, new Exception("booom"));
            assertTrue("unexpected succces listener to fail: " + listenerToFail, listenerToFail == -1);
        } catch (RuntimeException ex) {
            assertTrue("listener to fail: " + listenerToFail, listenerToFail >= 0);
            assertNotNull(ex.getCause());
            assertEquals("double boom", ex.getCause().getMessage());
        }

        for (int i = 0; i < numListeners; i++) {
            assertNull("listener index " + i, refList.get(i).get());
        }

        for (int i = 0; i < numListeners; i++) {
            assertEquals("listener index " + i, "booom", excList.get(i).get().getMessage());
        }
    }

    public void testRunAfter() {
        {
            AtomicBoolean afterSuccess = new AtomicBoolean();
            ActionListener<Object> listener = ActionListener.runAfter(ActionListener.wrap(r -> {}, e -> {}), () -> afterSuccess.set(true));
            listener.onResponse(null);
            assertThat(afterSuccess.get(), equalTo(true));
        }
        {
            AtomicBoolean afterFailure = new AtomicBoolean();
            ActionListener<Object> listener = ActionListener.runAfter(ActionListener.wrap(r -> {}, e -> {}), () -> afterFailure.set(true));
            listener.onFailure(null);
            assertThat(afterFailure.get(), equalTo(true));
        }
    }

    public void testRunBefore() {
        {
            AtomicBoolean afterSuccess = new AtomicBoolean();
            ActionListener<Object> listener = ActionListener.runBefore(ActionListener.wrap(r -> {}, e -> {}), () -> afterSuccess.set(true));
            listener.onResponse(null);
            assertThat(afterSuccess.get(), equalTo(true));
        }
        {
            AtomicBoolean afterFailure = new AtomicBoolean();
            ActionListener<Object> listener = ActionListener.runBefore(ActionListener.wrap(r -> {}, e -> {}), () -> afterFailure.set(true));
            listener.onFailure(null);
            assertThat(afterFailure.get(), equalTo(true));
        }
    }

    public void testNotifyOnce() {
        AtomicInteger onResponseTimes = new AtomicInteger();
        AtomicInteger onFailureTimes = new AtomicInteger();
        ActionListener<Object> listener = ActionListener.notifyOnce(new ActionListener<Object>() {
            @Override
            public void onResponse(Object o) {
                onResponseTimes.getAndIncrement();
            }

            @Override
            public void onFailure(Exception e) {
                onFailureTimes.getAndIncrement();
            }
        });
        boolean success = randomBoolean();
        if (success) {
            listener.onResponse(null);
        } else {
            listener.onFailure(new RuntimeException("test"));
        }
        for (int iters = between(0, 10), i = 0; i < iters; i++) {
            if (randomBoolean()) {
                listener.onResponse(null);
            } else {
                listener.onFailure(new RuntimeException("test"));
            }
        }
        if (success) {
            assertThat(onResponseTimes.get(), equalTo(1));
            assertThat(onFailureTimes.get(), equalTo(0));
        } else {
            assertThat(onResponseTimes.get(), equalTo(0));
            assertThat(onFailureTimes.get(), equalTo(1));
        }
    }

    public void testCompleteWith() {
        PlainActionFuture<Integer> onResponseListener = new PlainActionFuture<>();
        ActionListener.completeWith(onResponseListener, () -> 100);
        assertThat(onResponseListener.isDone(), equalTo(true));
        assertThat(onResponseListener.actionGet(), equalTo(100));

        PlainActionFuture<Integer> onFailureListener = new PlainActionFuture<>();
        ActionListener.completeWith(onFailureListener, () -> { throw new IOException("not found"); });
        assertThat(onFailureListener.isDone(), equalTo(true));
        assertThat(expectThrows(ExecutionException.class, onFailureListener::get).getCause(), instanceOf(IOException.class));

        AtomicReference<Exception> exReference = new AtomicReference<>();
        ActionListener<String> listener = new ActionListener<String>() {
            @Override
            public void onResponse(String s) {
                if (s == null) {
                    throw new IllegalArgumentException("simulate onResponse exception");
                }
            }

            @Override
            public void onFailure(Exception e) {
                exReference.set(e);
                if (e instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) e;
                }
            }
        };

        AssertionError assertionError = expectThrows(AssertionError.class, () -> ActionListener.completeWith(listener, () -> null));
        assertThat(assertionError.getCause(), instanceOf(IllegalArgumentException.class));
        assertNull(exReference.get());

        assertionError = expectThrows(AssertionError.class, () -> ActionListener.completeWith(listener, () -> {
            throw new IllegalArgumentException();
        }));
        assertThat(assertionError.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exReference.get(), instanceOf(IllegalArgumentException.class));
    }

    /**
     * Test that map passes the output of the function to its delegate listener and that exceptions in the function are propagated to the
     * onFailure handler. Also verify that exceptions from ActionListener.onResponse does not invoke onFailure, since it is the
     * responsibility of the ActionListener implementation (the client of the API) to handle exceptions in onResponse and onFailure.
     */
    public void testMap() {
        AtomicReference<Exception> exReference = new AtomicReference<>();

        ActionListener<String> listener = new ActionListener<String>() {
            @Override
            public void onResponse(String s) {
                if (s == null) {
                    throw new IllegalArgumentException("simulate onResponse exception");
                }
            }

            @Override
            public void onFailure(Exception e) {
                exReference.set(e);
                if (e instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) e;
                }
            }
        };
        ActionListener<Boolean> mapped = listener.map(b -> {
            if (b == null) {
                return null;
            } else if (b) {
                throw new IllegalStateException("simulate map function exception");
            } else {
                return b.toString();
            }
        });

        AssertionError assertionError = expectThrows(AssertionError.class, () -> mapped.onResponse(null));
        assertThat(assertionError.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertNull(exReference.get());
        mapped.onResponse(false);
        assertNull(exReference.get());
        mapped.onResponse(true);
        assertThat(exReference.get(), instanceOf(IllegalStateException.class));

        assertionError = expectThrows(AssertionError.class, () -> mapped.onFailure(new IllegalArgumentException()));
        assertThat(assertionError.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exReference.get(), instanceOf(IllegalArgumentException.class));
        mapped.onFailure(new IllegalStateException());
        assertThat(exReference.get(), instanceOf(IllegalStateException.class));
    }
}
