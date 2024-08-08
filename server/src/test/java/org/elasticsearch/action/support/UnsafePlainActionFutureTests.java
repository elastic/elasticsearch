/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class UnsafePlainActionFutureTests extends ESTestCase {

    public void testOneArg() {
        String unsafeExecutorName = "unsafe-executor";
        String otherExecutorName = "other-executor";
        UnsafePlainActionFuture<?> future = new UnsafePlainActionFuture<Void>(unsafeExecutorName);
        Thread other1 = getThread(otherExecutorName);
        Thread other2 = getThread(otherExecutorName);
        assertFalse(future.allowedExecutors(other1, other2));
        Thread unsafe1 = getThread(unsafeExecutorName);
        Thread unsafe2 = getThread(unsafeExecutorName);
        assertTrue(future.allowedExecutors(unsafe1, unsafe2));

        assertTrue(future.allowedExecutors(unsafe1, other1));
    }

    public void testTwoArg() {
        String unsafeExecutorName1 = "unsafe-executor-1";
        String unsafeExecutorName2 = "unsafe-executor-2";
        String otherExecutorName = "other-executor";
        UnsafePlainActionFuture<?> future = new UnsafePlainActionFuture<Void>(unsafeExecutorName1, unsafeExecutorName2);
        Thread other1 = getThread(otherExecutorName);
        Thread other2 = getThread(otherExecutorName);
        assertFalse(future.allowedExecutors(other1, other2));
        Thread unsafe1Thread1 = getThread(unsafeExecutorName1);
        Thread unsafe2Thread1 = getThread(unsafeExecutorName2);
        Thread unsafe1Thread2 = getThread(unsafeExecutorName1);
        Thread unsafe2Thread2 = getThread(unsafeExecutorName2);
        assertTrue(future.allowedExecutors(unsafe1Thread1, unsafe1Thread2));
        assertTrue(future.allowedExecutors(unsafe2Thread1, unsafe2Thread2));

        assertTrue(future.allowedExecutors(unsafe1Thread1, unsafe2Thread2));
        assertTrue(future.allowedExecutors(unsafe2Thread1, other1));
        assertTrue(future.allowedExecutors(other1, unsafe2Thread2));
    }

    private static Thread getThread(String executorName) {
        Thread t = new Thread("[" + executorName + "][]");
        assertThat(EsExecutors.executorName(t), equalTo(executorName));
        return t;
    }
}
