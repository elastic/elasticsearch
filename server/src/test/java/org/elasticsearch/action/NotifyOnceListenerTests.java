/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicReference;

public class NotifyOnceListenerTests extends ESTestCase {

    public void testWhenSuccessCannotNotifyMultipleTimes() {
        AtomicReference<String> response = new AtomicReference<>();
        AtomicReference<Exception> exception = new AtomicReference<>();

        NotifyOnceListener<String> listener = new NotifyOnceListener<String>() {
            @Override
            public void innerOnResponse(String s) {
                response.set(s);
            }

            @Override
            public void innerOnFailure(Exception e) {
                exception.set(e);
            }
        };

        listener.onResponse("response");
        listener.onResponse("wrong-response");
        listener.onFailure(new RuntimeException());

        assertNull(exception.get());
        assertEquals("response", response.get());
    }

    public void testWhenErrorCannotNotifyMultipleTimes() {
        AtomicReference<String> response = new AtomicReference<>();
        AtomicReference<Exception> exception = new AtomicReference<>();

        NotifyOnceListener<String> listener = new NotifyOnceListener<String>() {
            @Override
            public void innerOnResponse(String s) {
                response.set(s);
            }

            @Override
            public void innerOnFailure(Exception e) {
                exception.set(e);
            }
        };

        RuntimeException expected = new RuntimeException();
        listener.onFailure(expected);
        listener.onFailure(new IllegalArgumentException());
        listener.onResponse("response");

        assertNull(response.get());
        assertSame(expected, exception.get());
    }
}
