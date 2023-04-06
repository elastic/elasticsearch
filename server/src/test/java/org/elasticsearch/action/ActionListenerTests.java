/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ReachabilityChecker;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ActionListenerTests extends ESTestCase {

    public void testWrapConsumers() {
        AtomicReference<Boolean> reference = new AtomicReference<>();
        AtomicReference<Exception> exReference = new AtomicReference<>();

        ActionListener<Boolean> wrap = ActionListener.wrap(new CheckedConsumer<>() {
            @Override
            public void accept(Boolean o) {
                if (Boolean.FALSE.equals(o)) {
                    throw new IllegalArgumentException("must not be false");
                }
                reference.set(o);
            }

            @Override
            public String toString() {
                return "test handler";
            }
        }, new Consumer<>() {
            @Override
            public void accept(Exception newValue) {
                exReference.set(newValue);
            }

            @Override
            public String toString() {
                return "test exception handler";
            }
        });

        assertEquals("WrappedActionListener{test handler}{test exception handler}", wrap.toString());

        wrap.onResponse(Boolean.FALSE);
        assertNull(reference.getAndSet(null));
        assertEquals("must not be false", exReference.getAndSet(null).getMessage());

        wrap.onResponse(Boolean.TRUE);
        assertTrue(reference.getAndSet(null));
        assertNull(exReference.getAndSet(null));

        wrap.onFailure(new RuntimeException("test exception"));
        assertNull(reference.getAndSet(null));
        assertEquals("test exception", exReference.getAndSet(null).getMessage());
    }

    public void testWrapRunnable() {
        var executed = new AtomicBoolean();
        var listener = ActionListener.running(new Runnable() {
            @Override
            public void run() {
                assertTrue(executed.compareAndSet(false, true));
            }

            @Override
            public String toString() {
                return "test runnable";
            }
        });

        assertEquals("RunnableWrappingActionListener{test runnable}", listener.toString());

        listener.onResponse(new Object());
        assertTrue(executed.getAndSet(false));

        listener.onFailure(new Exception("simulated"));
        assertTrue(executed.getAndSet(false));

        expectThrows(
            AssertionError.class,
            () -> ActionListener.running(() -> { throw new UnsupportedOperationException(); }).onResponse(null)
        );
    }

    public void testWrapListener() {
        var succeeded = new AtomicBoolean();
        var failed = new AtomicBoolean();

        var listener = ActionListener.wrap(new ActionListener<>() {
            @Override
            public void onResponse(Object o) {
                assertTrue(succeeded.compareAndSet(false, true));
                if (o instanceof RuntimeException e) {
                    throw e;
                }
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(failed.compareAndSet(false, true));
                assertEquals("test exception", e.getMessage());
                if (e instanceof UnsupportedOperationException uoe) {
                    throw uoe;
                }
            }

            @Override
            public String toString() {
                return "test listener";
            }
        });

        assertEquals("wrapped{test listener}", listener.toString());

        listener.onResponse(new Object());
        assertTrue(succeeded.getAndSet(false));
        assertFalse(failed.getAndSet(false));

        listener.onFailure(new RuntimeException("test exception"));
        assertFalse(succeeded.getAndSet(false));
        assertTrue(failed.getAndSet(false));

        listener.onResponse(new RuntimeException("test exception"));
        assertTrue(succeeded.getAndSet(false));
        assertTrue(failed.getAndSet(false));
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
            listeners.add(new ActionListener<>() {
                @Override
                public void onResponse(Boolean result) {
                    reference.set(result);
                }

                @Override
                public void onFailure(Exception e) {
                    exReference.set(e);
                    if (fail) {
                        throw new RuntimeException("double boom");
                    }
                }
            });
        }

        try {
            ActionListener.onFailure(listeners, new Exception("booom"));
            assertEquals("unexpected succces listener to fail: " + listenerToFail, -1, listenerToFail);
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
            ActionListener<Object> listener = ActionListener.runAfter(ActionListener.noop(), () -> afterSuccess.set(true));
            listener.onResponse(null);
            assertThat(afterSuccess.get(), equalTo(true));
        }
        {
            AtomicBoolean afterFailure = new AtomicBoolean();
            ActionListener<Object> listener = ActionListener.runAfter(ActionListener.noop(), () -> afterFailure.set(true));
            listener.onFailure(new RuntimeException("test"));
            assertThat(afterFailure.get(), equalTo(true));
        }
    }

    public void testRunBefore() {
        {
            AtomicBoolean afterSuccess = new AtomicBoolean();
            ActionListener<Object> listener = ActionListener.runBefore(ActionListener.noop(), () -> afterSuccess.set(true));
            listener.onResponse(null);
            assertThat(afterSuccess.get(), equalTo(true));
        }
        {
            AtomicBoolean afterFailure = new AtomicBoolean();
            ActionListener<Object> listener = ActionListener.runBefore(ActionListener.noop(), () -> afterFailure.set(true));
            listener.onFailure(new RuntimeException("test"));
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

    public void testNotifyOnceReleasesDelegate() {
        final var reachabilityChecker = new ReachabilityChecker();
        final var listener = ActionListener.notifyOnce(reachabilityChecker.register(ActionListener.running(() -> {})));
        reachabilityChecker.checkReachable();
        listener.onResponse(null);
        reachabilityChecker.ensureUnreachable();
        assertEquals("notifyOnce[null]", listener.toString());
    }

    public void testConcurrentNotifyOnce() throws InterruptedException {
        final var completed = new AtomicBoolean();
        final var listener = ActionListener.notifyOnce(new ActionListener<Void>() {
            @Override
            public void onResponse(Void o) {
                assertTrue(completed.compareAndSet(false, true));
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(completed.compareAndSet(false, true));
            }

            @Override
            public String toString() {
                return "inner-listener";
            }
        });
        assertThat(listener.toString(), equalTo("notifyOnce[inner-listener]"));

        final var threads = new Thread[between(1, 10)];
        final var startBarrier = new CyclicBarrier(threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    startBarrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                if (randomBoolean()) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(new RuntimeException("test"));
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        assertTrue(completed.get());
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
        ActionListener<String> listener = new ActionListener<>() {
            @Override
            public void onResponse(String s) {
                if (s == null) {
                    throw new IllegalArgumentException("simulate onResponse exception");
                }
            }

            @Override
            public void onFailure(Exception e) {
                exReference.set(e);
                if (e instanceof IllegalArgumentException iae) {
                    throw iae;
                }
            }
        };

        AssertionError assertionError = expectThrows(AssertionError.class, () -> ActionListener.completeWith(listener, () -> null));
        assertThat(assertionError.getCause(), instanceOf(IllegalArgumentException.class));
        assertNull(exReference.get());

        assertionError = expectThrows(AssertionError.class, () -> ActionListener.completeWith(listener, () -> {
            throw new IllegalArgumentException();
        }));
        assertThat(assertionError.getCause(), instanceOf(AssertionError.class));
        assertThat(assertionError.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exReference.get(), instanceOf(IllegalArgumentException.class));
    }

    /**
     * Test that map passes the output of the function to its delegate listener and that exceptions in the function are propagated to the
     * onFailure handler. Also verify that exceptions from ActionListener.onResponse does not invoke onFailure, since it is the
     * responsibility of the ActionListener implementation (the client of the API) to handle exceptions in onResponse and onFailure.
     */
    public void testMap() {
        AtomicReference<Exception> exReference = new AtomicReference<>();

        ActionListener<String> listener = new ActionListener<>() {
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

    public void testRunBeforeThrowsAssertionErrorIfExecutedMoreThanOnce() {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        final String description = randomAlphaOfLength(10);
        final ActionListener<Void> runBefore = ActionListener.runBefore(ActionListener.noop(), makeCheckedRunnable(description));

        completeListener(randomBoolean(), runBefore);

        var error = expectThrows(AssertionError.class, () -> completeListener(true, runBefore));
        assertThat(error.getMessage(), containsString(description));
    }

    public void testRunAfterThrowsAssertionErrorIfExecutedMoreThanOnce() {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        final String description = randomAlphaOfLength(10);
        final ActionListener<Void> runAfter = randomBoolean()
            ? ActionListener.runAfter(ActionListener.noop(), makeRunnable(description))
            : ActionListener.releaseAfter(ActionListener.noop(), makeReleasable(description, new AtomicBoolean()));

        completeListener(randomBoolean(), runAfter);

        var error = expectThrows(AssertionError.class, () -> completeListener(true, runAfter));
        assertThat(error.getMessage(), containsString(description));
    }

    public void testWrappedRunBeforeOrAfterThrowsAssertionErrorIfExecutedMoreThanOnce() {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        final ActionListener<Void> throwingListener = new ActionListener<>() {
            @Override
            public void onResponse(Void o) {
                throw new AlreadyClosedException("throwing on purpose");
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("should not be called");
            }
        };

        final String description = randomAlphaOfLength(10);
        final ActionListener<Void> runBeforeOrAfterListener = randomBoolean()
            ? ActionListener.runBefore(throwingListener, makeCheckedRunnable(description))
            : ActionListener.runAfter(throwingListener, makeRunnable(description));

        final ActionListener<Void> wrappedListener = ActionListener.running(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                runBeforeOrAfterListener.onFailure(e);
            }

            @Override
            protected void doRun() {
                runBeforeOrAfterListener.onResponse(null);
            }
        });

        var error = expectThrows(AssertionError.class, () -> completeListener(true, wrappedListener));
        assertThat(error.getMessage(), containsString(description));
    }

    public void testReleasing() {
        runReleasingTest(true);
        runReleasingTest(false);
    }

    private static void runReleasingTest(boolean successResponse) {
        final AtomicBoolean releasedFlag = new AtomicBoolean();
        final String description = randomAlphaOfLength(10);
        final ActionListener<Void> l = ActionListener.releasing(makeReleasable(description, releasedFlag));
        assertThat(l.toString(), containsString("release[" + description + "]}"));
        completeListener(successResponse, l);
        assertTrue(releasedFlag.get());
    }

    private static void completeListener(boolean successResponse, ActionListener<Void> listener) {
        if (successResponse) {
            try {
                listener.onResponse(null);
            } catch (Exception e) {
                // ok
            }
        } else {
            listener.onFailure(new RuntimeException("simulated"));
        }
    }

    public void testReleaseAfter() {
        runReleaseAfterTest(true, false);
        runReleaseAfterTest(true, true);
        runReleaseAfterTest(false, false);
    }

    private static void runReleaseAfterTest(boolean successResponse, final boolean throwFromOnResponse) {
        final AtomicBoolean released = new AtomicBoolean();
        final String description = randomAlphaOfLength(10);
        final ActionListener<Void> l = ActionListener.releaseAfter(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                if (throwFromOnResponse) {
                    throw new RuntimeException("onResponse");
                }
            }

            @Override
            public void onFailure(Exception e) {
                // ok
            }

            @Override
            public String toString() {
                return "test listener";
            }
        }, makeReleasable(description, released));
        assertThat(l.toString(), containsString("test listener/release[" + description + "]"));

        if (successResponse) {
            try {
                l.onResponse(null);
            } catch (Exception e) {
                // ok
            }
        } else {
            l.onFailure(new RuntimeException("supplied"));
        }

        assertTrue(released.get());
    }

    private static Releasable makeReleasable(String description, AtomicBoolean releasedFlag) {
        return new Releasable() {
            @Override
            public void close() {
                assertTrue(releasedFlag.compareAndSet(false, true));
            }

            @Override
            public String toString() {
                return description;
            }
        };
    }

    private static Runnable makeRunnable(String description) {
        return new Runnable() {
            @Override
            public void run() {}

            @Override
            public String toString() {
                return description;
            }
        };
    }

    private static CheckedRunnable<?> makeCheckedRunnable(String description) {
        return new CheckedRunnable<>() {
            @Override
            public void run() {}

            @Override
            public String toString() {
                return description;
            }
        };
    }

    public static <T> Matcher<T> isMappedActionListener() {
        return instanceOf(ActionListenerImplementations.MappedActionListener.class);
    }
}
