/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

/**
 * A {@link PlainActionFuture} which bypasses the deadlock-detection checks since we're only using this in tests.
 */
public class TestPlainActionFuture<T> extends PlainActionFuture<T> {
    @Override
    boolean allowedExecutors(Thread thread1, Thread thread2) {
        return true;
    }
}
