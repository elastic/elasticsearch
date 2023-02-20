/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FutureUtilsTests extends ESTestCase {

    public void testCancellingNullFutureOkay() {
        FutureUtils.cancel(null);
    }

    public void testRunningFutureNotInterrupted() {
        final Future<?> future = mock(Future.class);
        FutureUtils.cancel(future);
        verify(future).cancel(false);
    }

    public void testInterruption() {
        final var future = new PlainActionFuture<Void>();
        final var currentThread = Thread.currentThread();
        currentThread.interrupt();

        expectThrows(ElasticsearchTimeoutException.class, () -> FutureUtils.get(future, 0, randomFrom(TimeUnit.values())));
        assertTrue(currentThread.isInterrupted());

        future.onResponse(null);
        assertNull(FutureUtils.get(future, 0, randomFrom(TimeUnit.values())));
        assertTrue(currentThread.isInterrupted());

        assertEquals(
            "Future got interrupted",
            expectThrows(IllegalStateException.class, () -> FutureUtils.get(future, 1, randomFrom(TimeUnit.values()))).getMessage()
        );
        assertTrue(currentThread.isInterrupted());
    }

}
