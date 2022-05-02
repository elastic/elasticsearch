/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AdapterActionFutureTests extends ESTestCase {

    public void testInterruption() throws Exception {
        final AdapterActionFuture<String, Integer> adapter = new AdapterActionFuture<String, Integer>() {
            @Override
            protected String convert(final Integer listenerResponse) {
                return Objects.toString(listenerResponse);
            }
        };

        // test all possible methods that can be interrupted
        final Runnable runnable = () -> {
            final int method = randomIntBetween(0, 4);
            switch (method) {
                case 0 -> adapter.actionGet();
                case 1 -> adapter.actionGet("30s");
                case 2 -> adapter.actionGet(30000);
                case 3 -> adapter.actionGet(TimeValue.timeValueSeconds(30));
                case 4 -> adapter.actionGet(30, TimeUnit.SECONDS);
                default -> throw new AssertionError(method);
            }
        };

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Thread main = Thread.currentThread();
        final Thread thread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            main.interrupt();
        });
        thread.start();

        final AtomicBoolean interrupted = new AtomicBoolean();

        barrier.await();

        try {
            runnable.run();
        } catch (final IllegalStateException e) {
            interrupted.set(Thread.interrupted());
        }
        // we check this here instead of in the catch block to ensure that the catch block executed
        assertTrue(interrupted.get());

        thread.join();
    }

    public void testUnwrapException() {
        checkUnwrap(new RemoteTransportException("test", new RuntimeException()), RuntimeException.class, RemoteTransportException.class);
        checkUnwrap(
            new RemoteTransportException("test", new Exception()),
            UncategorizedExecutionException.class,
            RemoteTransportException.class
        );
        checkUnwrap(new Exception(), UncategorizedExecutionException.class, Exception.class);
        checkUnwrap(new ElasticsearchException("test", new Exception()), ElasticsearchException.class, ElasticsearchException.class);
    }

    private void checkUnwrap(Exception exception, Class<? extends Exception> actionGetException, Class<? extends Exception> getException) {
        final AdapterActionFuture<Void, Void> adapter = new AdapterActionFuture<Void, Void>() {
            @Override
            protected Void convert(Void listenerResponse) {
                fail();
                return null;
            }
        };

        adapter.onFailure(exception);
        assertEquals(actionGetException, expectThrows(RuntimeException.class, adapter::actionGet).getClass());
        assertEquals(actionGetException, expectThrows(RuntimeException.class, () -> adapter.actionGet(10, TimeUnit.SECONDS)).getClass());
        assertEquals(getException, expectThrows(ExecutionException.class, () -> adapter.get()).getCause().getClass());
        assertEquals(getException, expectThrows(ExecutionException.class, () -> adapter.get(10, TimeUnit.SECONDS)).getCause().getClass());
    }
}
